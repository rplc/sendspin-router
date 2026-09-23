/*
 * Sendspin Router -> ioBroker
 *
 * MQTT adapter instance: mqtt.0 (must subscribe to sendspin/router/#)
 * Router base topic:     sendspin/router
 *
 * Mirrors the router's retained MQTT state into 0_userdata.0.SendspinRouter
 * and turns writes on the controls into MQTT commands.
 *
 * Every control is ONE state, following the usual ioBroker convention:
 *   ack=true  -> actual value, confirmed by the router
 *   ack=false -> command from a GUI/script; forwarded to the router, which
 *                confirms by publishing new state (written back with ack=true)
 *
 *   Groups.<group>.Stream    (input_select in Lovelace)  -> set_stream
 *   Groups.<group>.Volume    (input_number in Lovelace)  -> set_volume
 *   Groups.<group>.Mute      (input_boolean in Lovelace) -> set_mute
 *   Groups.<group>.Members   (JSON array of client ids)  -> set_members
 *   Clients.<client>.Group   (group id, '' = none)       -> set_group
 *   Clients.<client>.Volume / .Mute                      -> set_volume / set_mute
 *
 * Group assignment for Lovelace (two input_selects):
 *   Assign.Client  pick a client; Assign.Group then shows its current group
 *   Assign.Group   pick a group -> the selected client moves immediately
 *
 * The router only publishes when something changed, and this script only
 * writes states whose value changed. The only periodic task is a daily
 * cleanup of clients the router no longer reports.
 */

const MQTT = 'mqtt.0';
const BASE = 'sendspin/router';
const ROOT = '0_userdata.0.SendspinRouter';
const LOVELACE = 'lovelace.0';

// Stream value meaning "no source" (the router's null).
const STREAM_OFF = 'off';
const STREAM_OFF_LABEL = 'Aus';

// Placeholder keys for the Assign.* selects. They must not collide with a
// client or group id.
const ASSIGN_NONE = '_'; // nothing selected
const GROUP_NONE = 'none'; // client belongs to no group
const LABEL_SELECT_CLIENT = 'Client wählen';
const LABEL_NO_GROUP = 'Keine Gruppe';

// Remove objects created by older versions of this script (Command.* states,
// Router.ActiveSource, ...). They would otherwise keep conflicting Lovelace
// entity names.
const CLEANUP_LEGACY_OBJECTS = true;

// Clients the router has not reported for at least this long are deleted
// (objects included) by a daily job. Checked only while the router is online.
const CLIENT_CLEANUP_AFTER_HOURS = 24;
const CLIENT_CLEANUP_CRON = '17 4 * * *'; // daily at 04:17

// Optional room policy for clients the router left unassigned (no static
// client entry and no clients.default_group in the router config). Applied
// once per client and script start. First match wins.
const GROUP_RULES = [
    { group: 'wohnzimmer', regex: [/^wohnzimmer/i, /living/i] },
    { group: 'bad', regex: [/^bad/i, /badezimmer/i, /bath/i] },
    { group: 'schlafzimmer', regex: [/^schlafzimmer/i, /bedroom/i] },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const MQTT_PREFIX = `${MQTT}.${BASE.replaceAll('/', '.')}`;
const MQTT_IDS = {
    sources: `${MQTT_PREFIX}.state.sources`,
    groups: `${MQTT_PREFIX}.state.groups`,
    clients: `${MQTT_PREFIX}.state.clients`,
    router: `${MQTT_PREFIX}.state.router`,
    availability: `${MQTT_PREFIX}.availability`,
};
const MQTT_COMMAND_ERROR = `${MQTT_PREFIX}.event.command_error`;

// Clients GROUP_RULES must no longer touch (see mirrorClients).
const ruleApplied = new Set();

// Last known router state, keyed like the MQTT topics.
const data = { sources: {}, groups: {}, clients: {}, router: {}, availability: '' };

function safe(s) {
    return String(s).replace(/[^A-Za-z0-9_-]/g, '_');
}

function parse(value, fallback) {
    if (value === null || value === undefined || value === '') return fallback;
    if (typeof value !== 'string') return value;
    try {
        return JSON.parse(value);
    } catch (e) {
        return value;
    }
}

function publish(command, payload) {
    const topic = `${BASE}/command/${command}`;
    log(`Sendspin -> ${topic} ${JSON.stringify(payload)}`, 'debug');
    sendTo(MQTT, 'sendMessage2Client', {
        topic,
        message: JSON.stringify(payload),
        retain: false,
    });
}

function clampVolume(value) {
    const n = Math.round(Number(value));
    if (!Number.isFinite(n)) throw new Error(`invalid volume ${value}`);
    return Math.max(0, Math.min(100, n));
}

function toBool(value) {
    return value === true || value === 1 || value === 'true' || value === 'on' || value === '1';
}

function setObjectP(id, obj) {
    return new Promise((resolve, reject) => setObject(id, obj, err => (err ? reject(err) : resolve())));
}

function deleteObjectP(id, recursive = false) {
    return new Promise((resolve, reject) => deleteObject(id, recursive, err => (err ? reject(err) : resolve())));
}

function getObjectViewP(design, search, params) {
    return new Promise((resolve, reject) =>
        getObjectView(design, search, params, (err, res) => (err ? reject(err) : resolve(res))));
}

// All work runs strictly one task after another, so overlapping MQTT updates
// can never interleave half-finished mirror runs.
let queue = Promise.resolve();
function enqueue(task) {
    queue = queue.then(task).catch(e => log(`Sendspin: ${e && e.stack ? e.stack : e}`, 'error'));
    return queue;
}

// ---------------------------------------------------------------------------
// Objects and states: create/update only when needed, write only on change
// ---------------------------------------------------------------------------

const knownChannels = new Set();
const knownCommon = new Map(); // id -> JSON of the desired common last applied
const lastWritten = new Map(); // id -> value last written with ack=true

async function ensureChannel(id, name) {
    if (knownChannels.has(id)) return;
    if (!existsObject(id)) {
        await setObjectP(id, { type: 'channel', common: { name }, native: {} });
    }
    knownChannels.add(id);
}

function mergeCommon(current, desired) {
    const merged = { ...current };
    for (const [key, value] of Object.entries(desired)) {
        if (key === 'custom') {
            merged.custom = { ...(current.custom || {}) };
            for (const [instance, cfg] of Object.entries(value)) {
                merged.custom[instance] = { ...(merged.custom[instance] || {}), ...cfg };
            }
        } else {
            merged[key] = value;
        }
    }
    return merged;
}

async function ensureState(id, desired) {
    const signature = JSON.stringify(desired);
    if (knownCommon.get(id) === signature) return;

    if (!existsObject(id)) {
        // Pass everything (incl. custom) on creation: a freshly created object
        // is not necessarily in getObject()'s cache yet.
        await createStateAsync(id, {
            read: true,
            write: false,
            def: desired.type === 'boolean' ? false : desired.type === 'number' ? 0 : '',
            ...desired,
        });
    } else {
        // Existing object (possibly from an older script version): update
        // only the keys this script owns, keep everything else.
        const obj = getObject(id);
        const merged = mergeCommon(obj.common || {}, desired);
        if (JSON.stringify(merged) !== JSON.stringify(obj.common || {})) {
            await setObjectP(id, { ...obj, common: merged });
        }
    }
    knownCommon.set(id, signature);
}

async function put(id, value) {
    if (lastWritten.has(id) && lastWritten.get(id) === value) return;
    lastWritten.set(id, value);
    await setStateAsync(id, value, true);
}

function lovelace(entity, name, extra = {}) {
    return { [LOVELACE]: { enabled: true, entity, name, ...extra } };
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

const watchedCommands = new Set();
let resyncTimer = null;

function scheduleResync() {
    // If the router rejects a command or the value did not change, it
    // publishes nothing. Re-mirroring afterwards writes the real value back
    // with ack=true, so no control is left hanging with ack=false.
    if (resyncTimer) clearTimeout(resyncTimer);
    resyncTimer = setTimeout(() => {
        resyncTimer = null;
        enqueue(mirrorAll);
    }, 3000);
}

function onCommand(id, handler) {
    if (watchedCommands.has(id)) return;
    watchedCommands.add(id);
    on({ id, change: 'any', ack: false }, obj => {
        // The router's confirmation must be written even if it equals the
        // value this script wrote last time.
        lastWritten.delete(id);
        try {
            handler(obj.state.val);
        } catch (e) {
            log(`Sendspin command via ${id} rejected: ${e}`, 'warn');
        }
        scheduleResync();
    });
}

// ---------------------------------------------------------------------------
// Mirroring
// ---------------------------------------------------------------------------

function streamChoices() {
    const choices = { [STREAM_OFF]: STREAM_OFF_LABEL };
    for (const [id, source] of Object.entries(data.sources)) {
        choices[id] = source.name || id;
    }
    return choices;
}

function groupChoices() {
    const choices = { '': '-' };
    for (const [id, group] of Object.entries(data.groups)) {
        choices[id] = group.name || id;
    }
    return choices;
}

async function mirrorRouter() {
    await ensureState(`${ROOT}.Router.Online`, { name: 'Router online', type: 'boolean', role: 'indicator.reachable' });
    await ensureState(`${ROOT}.Router.Version`, { name: 'Router version', type: 'string', role: 'text' });
    await put(`${ROOT}.Router.Online`, data.availability === 'online');
    await put(`${ROOT}.Router.Version`, String(data.router.version || ''));
}

async function mirrorSources() {
    for (const [id, s] of Object.entries(data.sources)) {
        const base = `${ROOT}.Sources.${safe(id)}`;
        await ensureChannel(base, s.name || id);
        await ensureState(`${base}.Id`, { name: 'Source ID', type: 'string', role: 'text' });
        await ensureState(`${base}.Name`, { name: 'Name', type: 'string', role: 'text' });
        await ensureState(`${base}.Available`, { name: 'Audio flowing', type: 'boolean', role: 'indicator' });
        await ensureState(`${base}.Uri`, { name: 'URI', type: 'string', role: 'text' });
        await ensureState(`${base}.SampleRate`, { name: 'Sample rate', type: 'number', role: 'value', unit: 'Hz' });
        await ensureState(`${base}.Channels`, { name: 'Channels', type: 'number', role: 'value' });
        await ensureState(`${base}.BitDepth`, { name: 'Bit depth', type: 'number', role: 'value' });

        await put(`${base}.Id`, String(s.id || id));
        await put(`${base}.Name`, String(s.name || id));
        await put(`${base}.Available`, !!s.available);
        await put(`${base}.Uri`, String(s.uri || ''));
        await put(`${base}.SampleRate`, Number(s.sample_rate || 0));
        await put(`${base}.Channels`, Number(s.channels || 0));
        await put(`${base}.BitDepth`, Number(s.bit_depth || 0));
    }
}

async function mirrorGroups() {
    const choices = streamChoices();

    for (const [id, g] of Object.entries(data.groups)) {
        const base = `${ROOT}.Groups.${safe(id)}`;
        const name = g.name || id;

        await ensureChannel(base, name);
        await ensureState(`${base}.Id`, { name: 'Group ID', type: 'string', role: 'text' });
        await ensureState(`${base}.Name`, { name: 'Name', type: 'string', role: 'text' });
        await ensureState(`${base}.PlaybackState`, { name: 'Playback state', type: 'string', role: 'media.state' });
        await ensureState(`${base}.Members`, {
            name: 'Members (JSON array of client IDs)', type: 'string', role: 'json', write: true,
        });
        await ensureState(`${base}.Volume`, {
            name: 'Volume', type: 'number', role: 'level.volume', write: true,
            min: 0, max: 100, step: 1, unit: '%',
            custom: lovelace('input_number', `sendspin_${name}_Volume`, { mode: 'slider' }),
        });
        await ensureState(`${base}.Mute`, {
            name: 'Mute', type: 'boolean', role: 'media.mute', write: true,
            custom: lovelace('input_boolean', `sendspin_${name}_Mute`),
        });
        await ensureState(`${base}.Stream`, {
            name: 'Stream', type: 'string', role: 'media.input', write: true, states: choices,
            custom: lovelace('input_select', `sendspin_${name}_Stream`),
        });

        await put(`${base}.Id`, String(g.id || id));
        await put(`${base}.Name`, String(name));
        await put(`${base}.PlaybackState`, String(g.playback_state || 'stopped'));
        await put(`${base}.Members`, JSON.stringify(g.members || []));
        await put(`${base}.Volume`, Number(g.volume ?? 0));
        await put(`${base}.Mute`, !!g.mute);
        await put(`${base}.Stream`, g.stream || STREAM_OFF);

        onCommand(`${base}.Volume`, v => publish(`group/${id}/set_volume`, { volume: clampVolume(v) }));
        onCommand(`${base}.Mute`, v => publish(`group/${id}/set_mute`, { mute: toBool(v) }));
        onCommand(`${base}.Stream`, v => {
            const source = !v || v === STREAM_OFF ? null : String(v);
            if (source !== null && !data.sources[source]) throw new Error(`unknown source '${source}'`);
            publish(`group/${id}/set_stream`, { source });
        });
        onCommand(`${base}.Members`, v => {
            const members = parse(v, []);
            if (!Array.isArray(members)) throw new Error('Members must be a JSON array');
            publish(`group/${id}/set_members`, { members: members.map(String) });
        });
    }
}

function suggestedGroup(c, id) {
    const text = `${c.name || ''} ${id}`;
    const rule = GROUP_RULES.find(r => r.regex.some(re => re.test(text)));
    return rule && data.groups[rule.group] ? rule.group : null;
}

const knownClients = new Set();

async function mirrorClients() {
    const choices = groupChoices();

    for (const [id, c] of Object.entries(data.clients)) {
        const base = `${ROOT}.Clients.${safe(id)}`;
        knownClients.add(id);
        missingSince.delete(safe(id));

        await ensureChannel(base, c.name || id);
        await ensureState(`${base}.Id`, { name: 'Client ID', type: 'string', role: 'text' });
        await ensureState(`${base}.Name`, { name: 'Name', type: 'string', role: 'text' });
        await ensureState(`${base}.Available`, { name: 'Connected', type: 'boolean', role: 'indicator.connected' });
        await ensureState(`${base}.Roles`, { name: 'Roles', type: 'string', role: 'json' });
        await ensureState(`${base}.Group`, {
            name: 'Group', type: 'string', role: 'text', write: true, states: choices,
        });
        await ensureState(`${base}.Volume`, {
            name: 'Volume', type: 'number', role: 'level.volume', write: true,
            min: 0, max: 100, step: 1, unit: '%',
        });
        await ensureState(`${base}.Mute`, { name: 'Mute', type: 'boolean', role: 'media.mute', write: true });

        await put(`${base}.Id`, String(c.id || id));
        await put(`${base}.Name`, String(c.name || id));
        await put(`${base}.Available`, !!c.available);
        await put(`${base}.Roles`, JSON.stringify(c.roles || []));
        await put(`${base}.Group`, c.group_id || '');
        if (c.volume !== null && c.volume !== undefined) await put(`${base}.Volume`, Number(c.volume));
        if (c.mute !== null && c.mute !== undefined) await put(`${base}.Mute`, !!c.mute);

        onCommand(`${base}.Group`, v => {
            const group = v ? String(v) : null;
            if (group !== null && !data.groups[group]) throw new Error(`unknown group '${group}'`);
            ruleApplied.add(id);
            publish(`client/${id}/set_group`, { group });
        });
        onCommand(`${base}.Volume`, v => publish(`client/${id}/set_volume`, { volume: clampVolume(v) }));
        onCommand(`${base}.Mute`, v => publish(`client/${id}/set_mute`, { mute: toBool(v) }));

        // Rules are only for clients never seen in a group; a deliberate
        // "no group" must not be undone.
        if (c.group_id) ruleApplied.add(id);
        if (!c.group_id && !ruleApplied.has(id)) {
            ruleApplied.add(id);
            const group = suggestedGroup(c, id);
            if (group) {
                log(`Sendspin: assigning client ${id} to group ${group} (GROUP_RULES)`);
                publish(`client/${id}/set_group`, { group });
            }
        }
    }

    // Clients the router no longer knows about stay as objects but go offline.
    for (const id of knownClients) {
        if (!data.clients[id]) await put(`${ROOT}.Clients.${safe(id)}.Available`, false);
    }
}

// ---------------------------------------------------------------------------
// Group assignment selects (Lovelace)
// ---------------------------------------------------------------------------

// Client currently picked in Assign.Client (a client id or ASSIGN_NONE).
let assignClient = ASSIGN_NONE;

function groupLabel(groupId) {
    if (!groupId) return LABEL_NO_GROUP;
    const group = data.groups[groupId];
    return group ? group.name || groupId : groupId;
}

async function mirrorAssign() {
    if (assignClient !== ASSIGN_NONE && !data.clients[assignClient]) assignClient = ASSIGN_NONE;

    // "Name (Gruppe)" labels, sorted by name; refreshed whenever a client
    // moves, so the dropdown always shows the current assignment.
    const clientChoices = { [ASSIGN_NONE]: LABEL_SELECT_CLIENT };
    const clients = Object.entries(data.clients)
        .map(([id, c]) => ({ id, name: String(c.name || id), c }))
        .sort((a, b) => a.name.localeCompare(b.name, 'de'));
    for (const { id, name, c } of clients) {
        const details = [groupLabel(c.group_id)];
        if (!c.available) details.push('offline');
        clientChoices[id] = `${name} (${details.join(', ')})`;
    }

    const groupChoices = { [ASSIGN_NONE]: '-', [GROUP_NONE]: LABEL_NO_GROUP };
    for (const [id, g] of Object.entries(data.groups)) groupChoices[id] = g.name || id;

    const base = `${ROOT}.Assign`;
    await ensureChannel(base, 'Group assignment');
    await ensureState(`${base}.Client`, {
        name: 'Client to assign', type: 'string', role: 'text', write: true, states: clientChoices,
        custom: lovelace('input_select', 'sendspin_assign_client'),
    });
    await ensureState(`${base}.Group`, {
        name: 'Group of the selected client', type: 'string', role: 'text', write: true, states: groupChoices,
        custom: lovelace('input_select', 'sendspin_assign_group'),
    });

    const selected = data.clients[assignClient];
    await put(`${base}.Client`, assignClient);
    await put(`${base}.Group`, selected ? selected.group_id || GROUP_NONE : ASSIGN_NONE);

    onCommand(`${base}.Client`, v => {
        const id = v ? String(v) : ASSIGN_NONE;
        if (id !== ASSIGN_NONE && !data.clients[id]) throw new Error(`unknown client '${id}'`);
        assignClient = id;
        enqueue(mirrorAssign); // show the new client's current group right away
    });
    onCommand(`${base}.Group`, v => {
        if (assignClient === ASSIGN_NONE) throw new Error('select a client first');
        if (!v || v === ASSIGN_NONE) return; // placeholder picked, nothing to do
        const group = v === GROUP_NONE ? null : String(v);
        if (group !== null && !data.groups[group]) throw new Error(`unknown group '${group}'`);
        ruleApplied.add(assignClient);
        publish(`client/${assignClient}/set_group`, { group });
    });
}

async function mirrorAll() {
    await mirrorRouter();
    await mirrorSources();
    await mirrorGroups();
    await mirrorClients();
    await mirrorAssign();
}

// Which mirror steps depend on which topic (stream/group choices included).
const MIRROR_FOR = {
    sources: [mirrorSources, mirrorGroups],
    groups: [mirrorGroups, mirrorClients, mirrorAssign],
    clients: [mirrorClients, mirrorAssign],
    router: [mirrorRouter],
    availability: [mirrorRouter],
};

function ingest(key, value) {
    if (key === 'availability') {
        data.availability = String(value || '');
    } else {
        const parsed = parse(value, {});
        data[key] = parsed && typeof parsed === 'object' ? parsed : {};
    }
}

// ---------------------------------------------------------------------------
// Vanished clients
// ---------------------------------------------------------------------------

const missingSince = new Map(); // object key (safe client id) -> first seen missing (ms)

function forgetObjects(id) {
    const inside = key => key === id || key.startsWith(`${id}.`);
    for (const key of [...knownChannels]) if (inside(key)) knownChannels.delete(key);
    for (const cache of [knownCommon, lastWritten]) {
        for (const key of [...cache.keys()]) if (inside(key)) cache.delete(key);
    }
}

async function cleanupVanishedClients() {
    // While the router is offline its client list is stale; never delete then.
    if (data.availability !== 'online') return;

    const prefix = `${ROOT}.Clients.`;
    const view = await getObjectViewP('system', 'channel', { startkey: prefix, endkey: `${prefix}\u9999` });
    const present = new Set(Object.keys(data.clients).map(safe));
    const now = Date.now();

    for (const row of (view && view.rows) || []) {
        const key = row.id.slice(prefix.length);
        if (!key || key.includes('.')) continue; // only Clients.<client> channels
        if (present.has(key)) {
            missingSince.delete(key);
            continue;
        }
        if (!missingSince.has(key)) {
            missingSince.set(key, now); // the grace period starts now
            continue;
        }
        if (now - missingSince.get(key) < CLIENT_CLEANUP_AFTER_HOURS * 3600 * 1000) continue;

        await deleteObjectP(row.id, true);
        forgetObjects(row.id);
        missingSince.delete(key);
        for (const id of [...knownClients]) if (safe(id) === key) knownClients.delete(id);
        log(`Sendspin: removed client ${row.id}, not reported by the router for ` +
            `${CLIENT_CLEANUP_AFTER_HOURS} h`);
    }
}

// ---------------------------------------------------------------------------
// Legacy cleanup
// ---------------------------------------------------------------------------

async function cleanupLegacyObjects() {
    const ids = [`${ROOT}.Router.ActiveSource`];
    for (const id of Object.keys(data.groups)) {
        const base = `${ROOT}.Groups.${safe(id)}`;
        for (const suffix of ['Stream', 'Volume', 'Mute', 'Members']) ids.push(`${base}.Command.${suffix}`);
        ids.push(`${base}.Command`);
    }
    for (const id of Object.keys(data.clients)) {
        const base = `${ROOT}.Clients.${safe(id)}`;
        ids.push(`${base}.OffsetUs`, `${base}.Capabilities`);
    }
    for (const id of ids) {
        if (existsObject(id)) {
            await deleteObjectP(id);
            log(`Sendspin: removed legacy object ${id}`);
        }
    }
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

const KEY_BY_MQTT_ID = Object.fromEntries(Object.entries(MQTT_IDS).map(([key, id]) => [id, key]));

on({ id: Object.values(MQTT_IDS), change: 'ne' }, obj => {
    const key = KEY_BY_MQTT_ID[obj.id];
    if (!key) return;
    enqueue(async () => {
        ingest(key, obj.state.val);
        for (const step of MIRROR_FOR[key]) await step();
    });
});

on({ id: MQTT_COMMAND_ERROR, change: 'any' }, obj => {
    const err = parse(obj.state.val, {});
    log(`Sendspin router rejected ${err.command || 'command'}: ${err.error || obj.state.val}`, 'warn');
    scheduleResync();
});

enqueue(async () => {
    await ensureChannel(ROOT, 'Sendspin Router');
    for (const name of ['Clients', 'Groups', 'Sources', 'Router', 'Assign']) {
        await ensureChannel(`${ROOT}.${name}`, name);
    }
    for (const [key, id] of Object.entries(MQTT_IDS)) {
        const current = await getStateAsync(id);
        if (current) ingest(key, current.val);
    }
    // Keep the client picked before a script restart.
    const picked = await getStateAsync(`${ROOT}.Assign.Client`);
    if (picked && data.clients[picked.val]) assignClient = String(picked.val);
    if (CLEANUP_LEGACY_OBJECTS) await cleanupLegacyObjects();
    await mirrorAll();
    await cleanupVanishedClients(); // starts the grace period for missing clients
    log(`Sendspin: mirrored ${Object.keys(data.groups).length} group(s), ` +
        `${Object.keys(data.clients).length} client(s), ${Object.keys(data.sources).length} source(s)`);
});

schedule(CLIENT_CLEANUP_CRON, () => enqueue(cleanupVanishedClients));

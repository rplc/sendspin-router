/*
 * Sendspin Router -> ioBroker
 *
 * MQTT adapter instance: mqtt.0
 * Router base topic: sendspin/router
 *
 * The router owns discovery. ioBroker owns room policy: clients are assigned
 * to groups here based on their advertised name/ID. The router receives the
 * resulting set_members command over MQTT.
 */
const MQTT = 'mqtt.0';
const BASE = 'sendspin/router';
const ROOT = '0_userdata.0.SendspinRouter';

// Adjust these rules to your actual Sendspin client names. First match wins.
const GROUP_RULES = [
    { group: 'wohnzimmer', regex: [/^wohnzimmer/i, /living/i] },
    { group: 'bad', regex: [/^bad/i, /badezimmer/i, /bath/i] },
    { group: 'schlafzimmer', regex: [/^schlafzimmer/i, /bedroom/i] },
];

function safe(s) { return String(s).replace(/[^A-Za-z0-9_-]/g, '_'); }
function mqttState(suffix) { return `${MQTT}.${BASE.replaceAll('/', '.')}.${suffix.replaceAll('/', '.')}`; }
function publish(topic, payload) {
    sendTo(MQTT, 'sendMessage2Client', { topic, message: JSON.stringify(payload), retain: false });
}
async function channel(id, name) {
    await setObjectAsync(id, { type: 'channel', common: { name }, native: {} });
}
async function state(id, name, type, role, write = false) {
    await setObjectAsync(id, {
        type: 'state',
        common: { name, type, role, read: true, write, def: type === 'boolean' ? false : type === 'number' ? 0 : '' },
        native: {}
    });
}
async function initBase() {
    await channel(ROOT, 'Sendspin Router');
    for (const [id, name] of [['Clients','Clients'],['Groups','Groups'],['Sources','Sources'],['Router','Router']]) await channel(`${ROOT}.${id}`, name);
    await state(`${ROOT}.Router.ActiveSource`, 'Active source', 'string', 'media.source', true);
}
function suggestedGroup(c) {
    const text = `${c.name || ''} ${c.id || ''}`;
    for (const rule of GROUP_RULES) if (rule.regex.some(r => r.test(text))) return rule.group;
    return null;
}
async function mirrorClients(clients) {
    if (!clients || typeof clients !== 'object') return;
    for (const [id, c] of Object.entries(clients)) {
        const base = `${ROOT}.Clients.${safe(id)}`;
        await channel(base, c.name || id);
        await state(`${base}.Id`, 'Client ID', 'string', 'text');
        await state(`${base}.Name`, 'Name', 'string', 'text');
        await state(`${base}.Available`, 'Available', 'boolean', 'indicator.connected');
        await state(`${base}.Group`, 'Group', 'string', 'text', true);
        await state(`${base}.Volume`, 'Volume', 'number', 'level.volume', true);
        await state(`${base}.Mute`, 'Mute', 'boolean', 'switch', true);
        await state(`${base}.OffsetUs`, 'Offset (µs)', 'number', 'value');
        await state(`${base}.Roles`, 'Roles', 'string', 'text');
        await state(`${base}.Capabilities`, 'Capabilities', 'string', 'json');
        await setStateAsync(`${base}.Id`, c.id || id, true);
        await setStateAsync(`${base}.Name`, c.name || id, true);
        await setStateAsync(`${base}.Available`, !!c.available, true);
        await setStateAsync(`${base}.OffsetUs`, Number(c.offset_us || 0), true);
        await setStateAsync(`${base}.Roles`, JSON.stringify(c.roles || []), true);
        await setStateAsync(`${base}.Capabilities`, JSON.stringify(c.capabilities || {}), true);
        if (!getState(`${base}.Group`)?.val) {
            const g = suggestedGroup({ ...c, id });
            if (g) await setStateAsync(`${base}.Group`, g, false);
        }
    }
}
async function mirrorGroups(groups) {
    if (!groups || typeof groups !== 'object') return;
    for (const [id, g] of Object.entries(groups)) {
        const base = `${ROOT}.Groups.${safe(id)}`;
        await channel(base, g.name || id);
        await state(`${base}.Id`, 'Group ID', 'string', 'text');
        await state(`${base}.Name`, 'Name', 'string', 'text');
        await state(`${base}.Members`, 'Members', 'string', 'json');
        await state(`${base}.Volume`, 'Volume', 'number', 'level.volume');
        await state(`${base}.Mute`, 'Mute', 'boolean', 'switch');
        await state(`${base}.Stream`, 'Stream', 'string', 'media.source');
        await state(`${base}.PlaybackState`, 'Playback state', 'string', 'text');
        await state(`${base}.Command.Volume`, 'Set volume', 'number', 'level.volume', true);
        await state(`${base}.Command.Mute`, 'Set mute', 'boolean', 'switch', true);
        await state(`${base}.Command.Stream`, 'Set stream', 'string', 'media.source', true);
        await state(`${base}.Command.Members`, 'Set members (JSON)', 'string', 'json', true);
        await setStateAsync(`${base}.Id`, g.id || id, true);
        await setStateAsync(`${base}.Name`, g.name || id, true);
        await setStateAsync(`${base}.Members`, JSON.stringify(g.members || []), true);
        await setStateAsync(`${base}.Volume`, Number(g.volume ?? 100), true);
        await setStateAsync(`${base}.Mute`, !!g.mute, true);
        await setStateAsync(`${base}.Stream`, g.stream || '', true);
        await setStateAsync(`${base}.PlaybackState`, g.playback_state || 'stopped', true);
    }
}
async function mirrorSources(sources) {
    if (!sources || typeof sources !== 'object') return;
    for (const [id, s] of Object.entries(sources)) {
        const base = `${ROOT}.Sources.${safe(id)}`;
        await channel(base, s.name || id);
        for (const [suffix, name, type, role] of [
            ['Id','Source ID','string','text'],['Name','Name','string','text'],['Available','Available','boolean','indicator.connected'],
            ['Uri','URI','string','text'],['SampleRate','Sample rate','number','value'],['Channels','Channels','number','value'],['BitDepth','Bit depth','number','value']
        ]) await state(`${base}.${suffix}`, name, type, role);
        await setStateAsync(`${base}.Id`, s.id || id, true); await setStateAsync(`${base}.Name`, s.name || id, true);
        await setStateAsync(`${base}.Available`, !!s.available, true); await setStateAsync(`${base}.Uri`, s.uri || '', true);
        await setStateAsync(`${base}.SampleRate`, Number(s.sample_rate || 0), true); await setStateAsync(`${base}.Channels`, Number(s.channels || 0), true);
        await setStateAsync(`${base}.BitDepth`, Number(s.bit_depth || 0), true);
    }
}
async function process(suffix, value) {
    let data; try { data = typeof value === 'string' ? JSON.parse(value) : value; } catch (_) { return; }
    if (suffix === 'state.clients') await mirrorClients(data);
    else if (suffix === 'state.groups') await mirrorGroups(data);
    else if (suffix === 'state.sources') await mirrorSources(data);
    else if (suffix === 'state.router') await setStateAsync(`${ROOT}.Router.ActiveSource`, data.active_source || '', true);
}

initBase().catch(e => log(`Sendspin init failed: ${e}`, 'error'));

on({ id: [mqttState('state.clients'), mqttState('state.groups'), mqttState('state.sources'), mqttState('state.router')], change: 'any' }, obj => process(obj.id.substring((MQTT + '.').length).replaceAll('.', '.'), obj.state.val));

// Commands are intentionally separate from mirrored state.
function commandWatcher(id, topic, makePayload) {
    on({ id, change: 'ne', ack: false }, obj => {
        try { publish(topic, makePayload(obj.state.val)); }
        catch (e) { log(`Sendspin command ${id} failed: ${e}`, 'error'); }
        setStateAsync(id, obj.state.val, true);
    });
}

(async () => {
    await wait(1500);
    on({ id: `${ROOT}.Router.ActiveSource`, change: 'ne', ack: false }, obj => {
        publish(`${BASE}/command/router/set_active_source`, { source: obj.state.val || null });
        setStateAsync(obj.id, obj.state.val, true);
    });

    // Dynamically attach command watchers whenever a new router group appears.
    const wired = new Set();
    setInterval(async () => {
        const result = await getObjectListAsync({ startkey: `${ROOT}.Groups.`, endkey: `${ROOT}.Groups.\u9999` });
        for (const o of result) {
            const m = o._id.match(new RegExp(`^${ROOT.replaceAll('.', '\\.')}\\.Groups\\.([^\\.]+)\\.Command\\.(Volume|Mute|Stream|Members)$`));
            if (!m || wired.has(o._id)) continue;
            wired.add(o._id);
            const group = m[1], cmd = m[2];
            const action = { Volume:'set_volume', Mute:'set_mute', Stream:'set_stream', Members:'set_members' }[cmd];
            commandWatcher(o._id, `${BASE}/command/group/${group}/${action}`, value => cmd === 'Members' ? { members: JSON.parse(value || '[]') } : cmd === 'Volume' ? { volume: Number(value) } : cmd === 'Mute' ? { mute: !!value } : { source: value || null });
        }
    }, 2000);
})();

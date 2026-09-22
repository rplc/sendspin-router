/*
 * Sendspin Router -> ioBroker
 *
 * MQTT adapter instance: mqtt.0
 * Router base topic: sendspin/router
 *
 * Responsibilities:
 *   - Mirror Sendspin MQTT state into 0_userdata.0.SendspinRouter
 *   - Assign discovered clients to groups by name/ID
 *   - Provide writable group controls for Lovelace:
 *       Groups.<group>.Command.Stream  -> input_select
 *       Groups.<group>.Command.Volume  -> input_number
 *       Groups.<group>.Command.Mute    -> input_boolean
 *   - Forward ioBroker commands back to the Sendspin router via MQTT
 *
 * IMPORTANT:
 *   MQTT state objects already exist below mqtt.0. They are mirrored into
 *   0_userdata.0.SendspinRouter. Existing MQTT values are also imported once
 *   when this script starts.
 */

const MQTT = 'mqtt.0';
const BASE = 'sendspin/router';
const ROOT = '0_userdata.0.SendspinRouter';
const LOVELACE = 'lovelace.0';

// Adjust these rules to your actual Sendspin client names. First match wins.
const GROUP_RULES = [
    { group: 'wohnzimmer', regex: [/^wohnzimmer/i, /living/i] },
    { group: 'bad', regex: [/^bad/i, /badezimmer/i, /bath/i] },
    { group: 'schlafzimmer', regex: [/^schlafzimmer/i, /bedroom/i] },
];

function safe(s) {
    return String(s).replace(/[^A-Za-z0-9_-]/g, '_');
}

function mqttState(suffix) {
    return `${MQTT}.${BASE.replaceAll('/', '.')}.${suffix.replaceAll('/', '.')}`;
}

function mqttSuffix(id) {
    const prefix = `${MQTT}.${BASE.replaceAll('/', '.')}.`;
    return String(id).startsWith(prefix) ? String(id).slice(prefix.length) : null;
}

function publish(topic, payload) {
    sendTo(MQTT, 'sendMessage2Client', {
        topic,
        message: JSON.stringify(payload),
        retain: false,
    });
}

function setObjectAsyncCompat(id, obj) {
    return new Promise((resolve, reject) => {
        setObject(id, obj, err => err ? reject(err) : resolve());
    });
}

async function channel(id, name) {
    if (existsObject(id)) return;
    await setObjectAsyncCompat(id, {
        type: 'channel',
        common: { name },
        native: {},
    });
}

async function state(id, name, type, role, write = false, extraCommon = {}) {
    await createStateAsync(id, {
        name,
        type,
        role,
        read: true,
        write,
        def: type === 'boolean' ? false : type === 'number' ? 0 : '',
        ...extraCommon,
    });
}

async function updateStateObject(id, mutateCommon) {
    const obj = getObject(id);
    if (!obj) return;
    const common = { ...(obj.common || {}) };
    mutateCommon(common);
    await setObjectAsyncCompat(id, {
        ...obj,
        common,
    });
}

async function initBase() {
    await channel(ROOT, 'Sendspin Router');
    for (const [id, name] of [
        ['Clients', 'Clients'],
        ['Groups', 'Groups'],
        ['Sources', 'Sources'],
        ['Router', 'Router'],
    ]) {
        await channel(`${ROOT}.${id}`, name);
    }

    await state(`${ROOT}.Router.ActiveSource`, 'Active source', 'string', 'media.source', true);
}

function suggestedGroup(c) {
    const text = `${c.name || ''} ${c.id || ''}`;
    for (const rule of GROUP_RULES) {
        if (rule.regex.some(r => r.test(text))) return rule.group;
    }
    return null;
}

const wiredClientGroups = new Set();

function watchClientGroup(stateId, clientId) {
    if (wiredClientGroups.has(stateId)) return;
    wiredClientGroups.add(stateId);

    on({ id: stateId, change: 'ne', ack: false }, obj => {
        const newGroup = obj.state.val ? String(obj.state.val) : '';
        const oldGroup = obj.oldState && obj.oldState.val ? String(obj.oldState.val) : '';

        try {
            if (oldGroup && oldGroup !== newGroup) {
                const oldMembersState = getState(`${ROOT}.Groups.${safe(oldGroup)}.Members`);
                const oldMembers = oldMembersState && oldMembersState.val
                    ? JSON.parse(oldMembersState.val)
                    : [];

                publish(`${BASE}/command/group/${safe(oldGroup)}/set_members`, {
                    members: oldMembers.filter(id => id !== clientId),
                });
            }

            if (newGroup && newGroup !== oldGroup) {
                const newMembersState = getState(`${ROOT}.Groups.${safe(newGroup)}.Members`);
                const newMembers = newMembersState && newMembersState.val
                    ? JSON.parse(newMembersState.val)
                    : [];

                if (!newMembers.includes(clientId)) newMembers.push(clientId);

                publish(`${BASE}/command/group/${safe(newGroup)}/set_members`, {
                    members: newMembers,
                });
            }

            setStateAsync(obj.id, newGroup, true);
        } catch (e) {
            log(`Sendspin client group command ${clientId} failed: ${e}`, 'error');
        }
    });
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

        watchClientGroup(`${base}.Group`, c.id || id);
    }
}

async function configureGroupGui(base, groupId, groupName, streamChoices) {
    const streamId = `${base}.Command.Stream`;
    const volumeId = `${base}.Command.Volume`;
    const muteId = `${base}.Command.Mute`;

    // input_select: values come from common.states.
    await updateStateObject(streamId, common => {
        common.states = streamChoices;
        common.custom = {
            ...(common.custom || {}),
            [LOVELACE]: {
                ...((common.custom || {})[LOVELACE] || {}),
                enabled: true,
                entity: 'input_select',
                name: `sendspin_${groupName}_Stream`,
            },
        };
    });

    // input_number: slider from 0..100.
    await updateStateObject(volumeId, common => {
        common.min = 0;
        common.max = 100;
        common.step = 1;
        common.custom = {
            ...(common.custom || {}),
            [LOVELACE]: {
                ...((common.custom || {})[LOVELACE] || {}),
                enabled: true,
                entity: 'input_number',
                name: `sendspin_${groupName}_Volume`,
                mode: 'slider',
            },
        };
    });

    await updateStateObject(muteId, common => {
        common.custom = {
            ...(common.custom || {}),
            [LOVELACE]: {
                ...((common.custom || {})[LOVELACE] || {}),
                enabled: true,
                entity: 'input_boolean',
                name: `sendspin_${groupName}_Mute`
            },
        };
    });
}

function buildStreamChoices(sources) {
    const choices = {};
    if (!sources || typeof sources !== 'object') return choices;

    for (const [id, source] of Object.entries(sources)) {
        const value = source.id || id;
        const label = source.name || value;
        choices[value] = label;
    }

    return choices;
}

async function updateAllGroupStreamChoices(sources) {
    const choices = buildStreamChoices(sources);
    const groups = getObject(`${ROOT}.Groups`);
    if (!groups) return;

    // We cannot enumerate child objects with getObjectListAsync (not part of the
    // documented javascript API), so use the group IDs known from the mirrored
    // MQTT state below instead. This function is called from mirrorGroups too.
    return choices;
}

async function mirrorGroups(groups, sourcesForGui = null) {
    if (!groups || typeof groups !== 'object') return;

    const streamChoices = buildStreamChoices(sourcesForGui || {});

    for (const [id, g] of Object.entries(groups)) {
        const base = `${ROOT}.Groups.${safe(id)}`;
        const groupName = g.name || id;

        await channel(base, groupName);
        await state(`${base}.Id`, 'Group ID', 'string', 'text');
        await state(`${base}.Name`, 'Name', 'string', 'text');
        await state(`${base}.Members`, 'Members', 'string', 'json');
        await state(`${base}.Volume`, 'Volume', 'number', 'level.volume');
        await state(`${base}.Mute`, 'Mute', 'boolean', 'switch');
        await state(`${base}.Stream`, 'Stream', 'string', 'media.source');
        await state(`${base}.PlaybackState`, 'Playback state', 'string', 'text');

        await state(`${base}.Command.Volume`, 'Set volume', 'number', 'level.volume', true, {
            min: 0,
            max: 100,
            step: 1,
        });
        await state(`${base}.Command.Mute`, 'Set mute', 'boolean', 'switch', true);
        await state(`${base}.Command.Stream`, 'Set stream', 'string', 'media.source', true, {
            states: streamChoices,
        });
        await state(`${base}.Command.Members`, 'Set members (JSON)', 'string', 'json', true);

        await setStateAsync(`${base}.Id`, g.id || id, true);
        await setStateAsync(`${base}.Name`, groupName, true);
        await setStateAsync(`${base}.Members`, JSON.stringify(g.members || []), true);
        await setStateAsync(`${base}.Volume`, Number(g.volume ?? 100), true);
        await setStateAsync(`${base}.Mute`, !!g.mute, true);
        await setStateAsync(`${base}.Stream`, g.stream || '', true);
        await setStateAsync(`${base}.PlaybackState`, g.playback_state || 'stopped', true);

        // Keep the GUI controls synchronized with the actual router state.
        await setStateAsync(`${base}.Command.Volume`, Number(g.volume ?? 100), true);
        await setStateAsync(`${base}.Command.Mute`, !!g.mute, true);
        await setStateAsync(`${base}.Command.Stream`, g.stream || '', true);

        await configureGroupGui(base, id, groupName, streamChoices);

        commandWatcher(
            `${base}.Command.Volume`,
            `${BASE}/command/group/${safe(id)}/set_volume`,
            value => ({ volume: Number(value) })
        );
        commandWatcher(
            `${base}.Command.Mute`,
            `${BASE}/command/group/${safe(id)}/set_mute`,
            value => ({ mute: !!value })
        );
        commandWatcher(
            `${base}.Command.Stream`,
            `${BASE}/command/group/${safe(id)}/set_stream`,
            value => ({ source: value || null })
        );
        commandWatcher(
            `${base}.Command.Members`,
            `${BASE}/command/group/${safe(id)}/set_members`,
            value => ({ members: JSON.parse(value || '[]') })
        );
    }
}

async function mirrorSources(sources) {
    if (!sources || typeof sources !== 'object') return;

    for (const [id, s] of Object.entries(sources)) {
        const base = `${ROOT}.Sources.${safe(id)}`;

        await channel(base, s.name || id);
        for (const [suffix, name, type, role] of [
            ['Id', 'Source ID', 'string', 'text'],
            ['Name', 'Name', 'string', 'text'],
            ['Available', 'Available', 'boolean', 'indicator.connected'],
            ['Uri', 'URI', 'string', 'text'],
            ['SampleRate', 'Sample rate', 'number', 'value'],
            ['Channels', 'Channels', 'number', 'value'],
            ['BitDepth', 'Bit depth', 'number', 'value'],
        ]) {
            await state(`${base}.${suffix}`, name, type, role);
        }

        await setStateAsync(`${base}.Id`, s.id || id, true);
        await setStateAsync(`${base}.Name`, s.name || id, true);
        await setStateAsync(`${base}.Available`, !!s.available, true);
        await setStateAsync(`${base}.Uri`, s.uri || '', true);
        await setStateAsync(`${base}.SampleRate`, Number(s.sample_rate || 0), true);
        await setStateAsync(`${base}.Channels`, Number(s.channels || 0), true);
        await setStateAsync(`${base}.BitDepth`, Number(s.bit_depth || 0), true);
    }
}

async function mirrorAllGroupsWithCurrentSources() {
    const groupState = getState(mqttState('state.groups'));
    const sourceState = getState(mqttState('state.sources'));
    if (!groupState || !groupState.val) return;

    let groups;
    let sources;
    try {
        groups = typeof groupState.val === 'string' ? JSON.parse(groupState.val) : groupState.val;
        sources = sourceState && sourceState.val
            ? (typeof sourceState.val === 'string' ? JSON.parse(sourceState.val) : sourceState.val)
            : {};
    } catch (_) {
        return;
    }

    await mirrorGroups(groups, sources);
}

async function handleMqttState(suffix, value) {
    let data;
    try {
        data = typeof value === 'string' ? JSON.parse(value) : value;
    } catch (_) {
        return;
    }

    if (suffix === 'state.clients') {
        await mirrorClients(data);
    } else if (suffix === 'state.groups') {
        const sourceState = getState(mqttState('state.sources'));
        let sources = {};
        try {
            if (sourceState && sourceState.val) {
                sources = typeof sourceState.val === 'string' ? JSON.parse(sourceState.val) : sourceState.val;
            }
        } catch (_) {
            sources = {};
        }
        await mirrorGroups(data, sources);
    } else if (suffix === 'state.sources') {
        await mirrorSources(data);
        await mirrorAllGroupsWithCurrentSources();
    } else if (suffix === 'state.router') {
        await setStateAsync(`${ROOT}.Router.ActiveSource`, data.active_source || '', true);
    }
}

const MQTT_STATE_IDS = [
    mqttState('state.clients'),
    mqttState('state.groups'),
    mqttState('state.sources'),
    mqttState('state.router'),
];

async function mirrorExistingMqttState() {
    // Sources first so group Stream input_select gets its choices immediately.
    for (const suffix of ['state.sources', 'state.clients', 'state.groups', 'state.router']) {
        const id = mqttState(suffix);
        const current = getState(id);
        if (!current || current.val === undefined || current.val === null || current.val === '') continue;
        await handleMqttState(suffix, current.val);
    }
}

// Commands are intentionally separate from mirrored state.
const wiredCommands = new Set();

function commandWatcher(id, topic, makePayload) {
    if (wiredCommands.has(id)) return;
    wiredCommands.add(id);

    on({ id, change: 'ne', ack: false }, obj => {
        try {
            publish(topic, makePayload(obj.state.val));
        } catch (e) {
            log(`Sendspin command ${id} failed: ${e}`, 'error');
        }
        setStateAsync(id, obj.state.val, true);
    });
}

initBase()
    .then(() => mirrorExistingMqttState())
    .catch(e => log(`Sendspin init/mirror failed: ${e}`, 'error'));

// Mirror future MQTT state changes. The important part is mqttSuffix():
// obj.id is e.g. mqtt.0.sendspin.router.state.clients and must become
// state.clients, not sendspin.router.state.clients.
on({ id: MQTT_STATE_IDS, change: 'any' }, obj => {
    const suffix = mqttSuffix(obj.id);
    if (!suffix) return;
    handleMqttState(suffix, obj.state.val)
        .catch(e => log(`Sendspin state mirror failed: ${e}`, 'error'));
});

// Router-wide source command (kept for compatibility with the existing MVP).
(async () => {
    await wait(1500);

    on({ id: `${ROOT}.Router.ActiveSource`, change: 'ne', ack: false }, obj => {
        publish(`${BASE}/command/router/set_active_source`, {
            source: obj.state.val || null,
        });
        setStateAsync(obj.id, obj.state.val, true);
    });
})();

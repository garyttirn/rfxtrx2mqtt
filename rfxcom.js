// --------------------------------------------------------------------------------------
// RFXTRX 2 MQTT bridge v1.1
// --------------------------------------------------------------------------------------

const rfxcom = require('rfxcom');
const mqtt = require('mqtt');
const yaml = require('js-yaml');
const fs = require('fs');

let config = null;
let client = null;
let rfxtrx = null;

let txProtocols = [];

let publishOptions={ retain:true, qos:1 };

// --------------------------------------------------------------------------------------
// Logging
// --------------------------------------------------------------------------------------

const KNRM = '\x1B[0m';
const KRED = '\x1B[31;1m';
const KGRN = '\x1B[32;1m';
const KYEL = '\x1B[33;1m';
const KBLU = '\x1B[34;1m';
const KMAG = '\x1B[35;1m';
const KCYA = '\x1B[36;1m';
const KWHT = '\x1B[37;1m';

function log(level, component, message)
{
    let ls = '';
    switch( level ) {
        case 'error': ls = KRED + 'ERROR' + KNRM; break;
        case 'warn': ls = KYEL + 'WARN' + KNRM; break;
        case 'info': ls = KGRN + 'INFO' + KNRM; break;
    }

    let d = new Date();
    let s = d.toISOString() + " [" + ls + "] " + KWHT + component + KNRM + " : " + message;
    console.log(s);
}

function loginfo(component, message)
{
    log('info', component, message);
}

function logwarn(component, message)
{
    log('warn', component, message);
}

function logerror(component, message, exit = true)
{
    log('error', component, message);

    if( exit )
        process.exit(1);
}


// --------------------------------------------------------------------------------------
// Event handlers
// --------------------------------------------------------------------------------------

function parseValue(s)
{
    return (s * 1.0).toString();
}

function event_sensor(event, protocol)
{
    const baseTopic = config.mqtt.base_topic;
    const setTopic = config[protocol].topic;

    // Prepends the base_topic followed by a slash if it exists; otherwise uses just the set topic
    const topic = baseTopic ? `${baseTopic}/${setTopic}` : setTopic;

    for( let i in config[protocol].devices ) {
        if( i == event.id ) {
            const m = config[protocol].devices[i];
            if( event.temperature !== undefined )
                client.publish(`${topic}/${m}/temperature/value`, parseValue(event.temperature), publishOptions);
            if( event.humidity !== undefined )
                client.publish(`${topic}/${m}/humidity/value`, parseValue(event.humidity), publishOptions);
            if( event.barometer !== undefined )
                client.publish(`${topic}/${m}/barometer/value`, parseValue(event.barometer), publishOptions);
            if( event.rainfallIncrement !== undefined )
                client.publish(`${topic}/${m}/rainfallIncrement/value`, parseValue(event.rainfallIncrement), publishOptions);
            if( event.rainfall !== undefined )
                client.publish(`${topic}/${m}/rainfall/value`, parseValue(event.rainfall), publishOptions);
            if( event.rainfallRate !== undefined )
                client.publish(`${topic}/${m}/rainfallRate/value`, parseValue(event.rainfallRate), publishOptions);
            if( event.gustSpeed !== undefined )
                client.publish(`${topic}/${m}/gustSpeed/value`, parseValue(event.gustSpeed), publishOptions);
            if( event.averageSpeed !== undefined )
                client.publish(`${topic}/${m}/averageSpeed/value`, parseValue(event.averageSpeed), publishOptions);
            if( event.direction !== undefined )
                client.publish(`${topic}/${m}/direction/value`, parseValue(event.direction), publishOptions);
            if( event.chillfactor !== undefined )
                client.publish(`${topic}/${m}/chillfactor/value`, parseValue(event.chillfactor), publishOptions);
            if( event.uv !== undefined )
                client.publish(`${topic}/${m}/uv/value`, parseValue(event.uv), publishOptions);
            if( event.insolation !== undefined )
                client.publish(`${topic}/${m}/insolation/value`, parseValue(event.insolation), publishOptions);
            if( event.weight !== undefined )
                client.publish(`${topic}/${m}/weight/value`, parseValue(event.weight), publishOptions);
            if( event.batteryLevel !== undefined )
                client.publish(`${topic}/${m}/battery/value`, parseValue(event.batteryLevel*11.1), publishOptions);
            if( event.rssi !== undefined )
                client.publish(`${topic}/${m}/rssi/value`, parseValue(event.rssi*8 - 120), publishOptions);
            break;
        }
    }
}

function event_energy(event, protocol)
{
    const baseTopic = config.mqtt.base_topic;
    const setTopic = config[protocol].topic;

    // Prepends the base_topic followed by a slash if it exists; otherwise uses just the set topic
    const topic = baseTopic ? `${baseTopic}/${setTopic}` : setTopic;

    for( let i in config[protocol].devices ) {
        if( i == event.id ) {
            const m = config[protocol].devices[i];
            if( event.voltage !== undefined )
                client.publish(`${topic}/${m}/voltage`, parseValue(event.voltage), publishOptions);
            if( event.current !== undefined )
                client.publish(`${topic}/${m}/current`, parseValue(event.current), publishOptions);
            if( event.count !== undefined )
                client.publish(`${topic}/${m}/count`, parseValue(event.count), publishOptions);
            if( event.power !== undefined )
                client.publish(`${topic}/${m}/power`, parseValue(event.power), publishOptions);
            if( event.energy !== undefined )
                client.publish(`${topic}/${m}/energy`, parseValue(event.energy), publishOptions);
            if( event.powerFactor !== undefined )
                client.publish(`${topic}/${m}/powerFactor`, parseValue(event.powerFactor), publishOptions);
            if( event.frequency !== undefined )
                client.publish(`${topic}/${m}/frequency`, parseValue(event.frequency), publishOptions);
            break;
        }
    }
}

function event_lighting(event, protocol)
{
    const baseTopic = config.mqtt.base_topic;
    const setTopic = config[protocol].topic;

    // Prepends the base_topic followed by a slash if it exists; otherwise uses just the set topic
    const topic = baseTopic ? `${baseTopic}/${setTopic}` : setTopic;

    for( let i in config[protocol].devices ) {
        if( i == `${event.id}/${event.unitCode}` ) {
            const m = config[protocol].devices[i];
            if( event.command !== 'Set Level' ) {
                client.publish(`${topic}/${m}/state`, event.command);
            } else {
                client.publish(`${topic}/${m}/level`, parseValue(event.level), publishOptions);
            }
            break;
        }
    }
}

// --------------------------------------------------------------------------------------
// Protocol initializers
// --------------------------------------------------------------------------------------

function addProtocol(rfxtrx, t)
{
    switch( t ) {

        case 'temperature1':
        case 'humidity1':
        case 'temperaturehumidity1':
        case 'temphumbaro1':
        case 'temperaturerain1':
        case 'rain1':
        case 'wind1':
        case 'weather':
        case 'uv1':
        case 'solar':
        case 'weight1':
            config[t].topic ??= t;
            rfxtrx.on(t, function(event) { event_sensor(event, t); });
            loginfo('RFXC', `Added protocol ${t}`);
            break;

        case 'elec1':
        case 'elec2':
        case 'elec3':
        case 'elec4':
        case 'elec5':
            config[t].topic ??= t;
            rfxtrx.on(t, function(event) { event_energy(event, t); });
            loginfo('RFXC', `Added protocol ${t}`);
            break;

        case 'lighting2':
            config.lighting2.topic ??= t;
            config.lighting2.subtype ??= 'AC';
            rfxtrx.on('lighting2', function(event) { event_lighting(event, t); });
            config.lighting2.tx = new rfxcom.Lighting2(rfxtrx, rfxcom.lighting2[config.lighting2.subtype]);
            txProtocols.push(config.lighting2);
            loginfo('RFXC', `Added protocol ${t}`);
            break;

        case 'lighting5':
            config.lighting5.topic ??= t;
            config.lighting5.subtype ??= 'CONRAD';
            rfxtrx.on('lighting5', function(event) { event_lighting(event, t); });
            config.lighting5.tx = new rfxcom.Lighting5(rfxtrx, rfxcom.lighting5[config.lighting5.subtype]);
            txProtocols.push(config.lighting5);
            loginfo('RFXC', `Added protocol ${t}`);
            break;

    }
}


// --------------------------------------------------------------------------------------
// MQTT handling
// --------------------------------------------------------------------------------------

function subscribeTopics()
{
    let topics = [];
    for( let p of txProtocols ) {
        const baseTopic = config.mqtt.base_topic;
        const setTopic = `${p.topic}/+/set`;

        // Prepends the base_topic followed by a slash if it exists; otherwise uses just the set topic
        const topic = baseTopic ? `${baseTopic}/${setTopic}` : setTopic;

        if( !topics.includes(topic) ) {
            client.subscribe(topic, (err) => { if( err ) console.log(err); });
            topics.push(topic);
            loginfo('MQTT', `Listening to ${topic}`);
        }
    }
}

function receiveMessage(topic, message)
{
    if( !rfxtrx.ready ) {
        logwarn('MQTT', `MQTT command received, but RFXCOM is not ready`);
        return;
    }

    let t = topic.split("/");

    // Search in all used protocols
    for( let p of txProtocols ) {

        let pt = p.topic.split("/");

        // Skip if the message topic doesn't match the protocol
        if( pt[pt.length-1] != t[t.length-3] ) continue;

        // Find the device the message was intended for
        for( let i in p.devices ) {
            const m = p.devices[i];
            if( m === t[t.length-2] ) {
                const s = message.toString();
                loginfo('RFXC', `${m} set to ${s}`);
                try {
                    if( ['on', 'On', 'true'].includes(s) ) {
                        p.tx.switchOn(i);
                    } else if( ['off', 'Off', 'false'].includes(s) ) {
                        p.tx.switchOff(i);
                    } else {
                        p.tx.setLevel(i, s * 1);
                    }
                }
                catch( error ) {
                    logwarn('RFXC', `${error.message}`);
                }
                break;
            }
        }

    }
}


// --------------------------------------------------------------------------------------
// Device scanning
// --------------------------------------------------------------------------------------

function scanForDevices(mode, newOnly)
{
    let packetType = 0;

    rfxtrx.on('receive', function(data) { packetType = data[1]; } );

    const protocols = [ 'temperature1', 'humidity1', 'temperaturehumidity1', 'temphumbaro1', 'temperaturerain1', 'rain1', 'wind1', 'uv1', 'weather', 'solar', 'weight1', 'elec1', 'elec2', 'elec3', 'elec4', 'elec5',  
                        'lighting2', 'lighting5' ];

    let seen = new Map();

    if( newOnly ) {
        for( let t in config ) {
            if( protocols.includes(t) && config[t].devices ) {
                for( let d in config[t].devices ) seen.set(t+d, true);
            }
        }
    }

    for( let i of protocols ) {
        rfxtrx.on(i, function(event) {
            const device_id = `${event.id}${(event.unitCode >= 0) ? '/' + event.unitCode : ''}`;
            if( !seen.has(i + device_id) ) {
                if( mode == 1 ) seen.set(i + device_id, true);
                let s = '';
                for( let j of rfxcom.deviceNames[packetType][event.subtype] ) s += `${j}, `;
                s = s.slice(0, -2);
                console.log(`# Device type: ${packetType}:${event.subtype} (${s})`);
                console.log(`  * Device ID:  ${device_id}`);
                console.log(`  * Protocol:   ${i}`);
                if( rfxcom[i] )
                    console.log(`  * TX subtype: ${rfxcom[i][event.subtype]}`);
                if( mode == 0 ) {
                    if( event.command )
                        console.log(`  * Command:    '${event.command}'`);
                    if( event.rssi )
                        console.log(`  * RSSI:       ${event.rssi}`);
                }
                console.log('');
            }
        });
    }
}


// --------------------------------------------------------------------------------------
// Command line args
// --------------------------------------------------------------------------------------

function readArgumentS()
{
    let args = {};
    args.scan = process.argv.indexOf('-scan') > 0 || process.argv.indexOf('--scan') > 0;
    args.stream = process.argv.indexOf('-stream') > 0 || process.argv.indexOf('--stream') > 0;
    args.newOnly = process.argv.indexOf('-new') > 0 || process.argv.indexOf('--new') > 0;

    return args;
}


// --------------------------------------------------------------------------------------
// Config file
// --------------------------------------------------------------------------------------

function readConfig()
{
    try {
        config = yaml.load(fs.readFileSync('/etc/rfxtrx2mqtt/configuration.yaml', 'utf8'));
    } catch( e ) {
        if (e.name === 'YAMLException') {
           console.error(`YAML Syntax Error on line ${e.mark.line + 1}, column ${e.mark.column + 1}:`);
           console.error(e.message);
        } else {
        logerror('MAIN', `Error loading config file !`);
        }
    }

    if( !config.serial?.port ) {
        logerror('RFXC', `RFXCOM port is undefined !`);
    }

    if( !config.mqtt?.server ) {
        logerror('MQTT', `MQTT server is undefined !`);
    }

    return config;
}


// --------------------------------------------------------------------------------------
// Main entry point
// --------------------------------------------------------------------------------------

function main()
{
    console.log(KBLU + '### RFXTRX 2 MQTT bridge V1.1 ###' + KNRM);

    // Command line args
    let args = readArgumentS();

    // Read config
    config = readConfig();

    // Open connection to the RFXTRX
    rfxtrx = new rfxcom.RfxCom(config.serial.port, { debug: false });

    rfxtrx.on('connectfailed', () => {
        logerror('RFXC', `Cannot open serial port !`);
    });

    loginfo('RFXC', `Opening RFXCOM on ${config.serial.port}`);

    if( !args.scan && !args.stream ) {

        // Register all used protocols
        for( let t in config ) {
            addProtocol(rfxtrx, t, config);
        }

        // Init connection to MQTT server

        loginfo('MQTT', `Connecting MQTT server at ${config.mqtt.server}`);

        // retain-flag from configuration
        publishOptions.retain = config.mqtt.retain ?? publishOptions.retain;;

        settings = {};
        if( config.mqtt.username ) settings.username = config.mqtt.username;
        if( config.mqtt.password ) settings.password = config.mqtt.password;

        client = mqtt.connect(config.mqtt.server, settings);

        client.on('connect', () => {
            loginfo('MQTT', `Connected`);
            subscribeTopics();
        });

        client.on('error', (err) => {
            logerror('MQTT', `MQTT connection error`, false);
            console.log(err);
            process.exit(1);
        });

        client.on('message', receiveMessage);

    } else {

        // In scan mode, start device scanning
        scanForDevices(args.scan ? 1 : 0, args.newOnly);

    }

    rfxtrx.on('status', (event) => {
        loginfo('RFXC', `${event.receiverType}, HW version ${event.hardwareVersion}`);
        loginfo('RFXC', `Firmware version ${event.firmwareVersion} (${event.firmwareType})`);
        loginfo('RFXC', `TX power +${event.transmitterPower} dBm`);
    });

    // Start the RFXTRX
    rfxtrx.initialise(function() {
        rfxtrx.ready = true;
        loginfo('RFXC', `RFXCOM connection established`);
        if( args.scan || args.stream )
            loginfo('RFXC', `Started scanning for devices...\n`);
    });
}

main();


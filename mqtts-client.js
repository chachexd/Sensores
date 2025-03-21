/***************************************************************
 * mqtt-client-debug-ip.js
 * Código alternativo para conectarse a The Things Stack (Industries)
 * vía MQTT usando una dirección IP, y enviar datos a InfluxDB con
 * nombres de campos y tags descriptivos.
 * NOTA: Se desactiva la validación TLS (rejectUnauthorized: false)
 * para descartar problemas de DNS/certificados. No usar en producción.
 ***************************************************************/

// Forzar IPv4 primero (para evitar problemas intermitentes de DNS)
require('dns').setDefaultResultOrder('ipv4first');

const mqtt = require('mqtt');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

// ---------------------------------------------------------
// 1. CONFIGURACIÓN DE THE THINGS STACK (Industries)
// ---------------------------------------------------------
// Usamos una dirección IP obtenida de nslookup: por ejemplo, "52.17.144.232"
const TTN_BROKER_IP   = '52.17.144.232';
const TTN_USERNAME    = 'prueba1'; // Este es el Application ID
const TTN_PASSWORD    = 'NNSXS.M5VJ6N22PHAY32ELIJ26K5NULLEA4QBYEX62IDA.HUW3IU5V7WNWKKZQNRQUGXYVPY42CS5KWUDW2ENOC2LJNNYGBR2A';
// El topic sigue siendo el mismo, pero se conecta vía IP:
const TTN_TOPIC       = 'v3/prueba1@smartfenixiot/devices/+/up';

console.log('=== Configuración de The Things Stack (usando IP) ===');
console.log(`Broker IP: ${TTN_BROKER_IP}`);
console.log(`Application ID (Usuario): ${TTN_USERNAME}`);
console.log(`API Key (Password): ${TTN_PASSWORD}`);
console.log(`Topic de suscripción: ${TTN_TOPIC}`);

// ---------------------------------------------------------
// 2. CONFIGURACIÓN DE INFLUXDB
// ---------------------------------------------------------
const INFLUX_URL    = 'http://localhost:8086';
const INFLUX_TOKEN  = '1znZf2FZ4syZ8HtEgDQKtm6p9T0_decSkMIX3HicbKTgy0GlU2TW0l3lcUDNoQ9fgDJYasyal2DEQ1yG3YFydg==';
const INFLUX_ORG    = 'Smartfenix';
const INFLUX_BUCKET = 'smartfenix';

console.log('\n=== Configuración de InfluxDB ===');
console.log(`URL: ${INFLUX_URL}`);
console.log(`Token: ${INFLUX_TOKEN}`);
console.log(`Organización: ${INFLUX_ORG}`);
console.log(`Bucket: ${INFLUX_BUCKET}`);

console.log('\nIniciando conexión a InfluxDB...');
const influxDB = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
const writeApi = influxDB.getWriteApi(INFLUX_ORG, INFLUX_BUCKET);
console.log('Conexión a InfluxDB establecida.');

// ---------------------------------------------------------
// 3. Conexión a MQTT (The Things Stack Industries) vía TLS usando IP
// ---------------------------------------------------------
console.log('\nConectando a The Things Stack vía MQTT (TLS) usando IP...');
const client = mqtt.connect(`mqtts://${TTN_BROKER_IP}:8883`, {
  username: TTN_USERNAME,
  password: TTN_PASSWORD,
  rejectUnauthorized: false // Ignora la validación de certificados (solo para pruebas)
});

client.on('connect', () => {
  console.log('Conexión MQTT establecida.');
  console.log(`Intentando suscribir al topic: ${TTN_TOPIC}`);
  client.subscribe(TTN_TOPIC, (err) => {
    if (err) {
      console.error('Error al suscribirse al topic:', err);
    } else {
      console.log(`Suscripción exitosa al topic: ${TTN_TOPIC}`);
    }
  });
});

client.on('error', (err) => {
  console.error('Error en conexión MQTT:', err);
});

client.on('reconnect', () => {
  console.log('Reintentando conexión MQTT...');
});

// ---------------------------------------------------------
// 4. Manejo de mensajes MQTT
// ---------------------------------------------------------
client.on('message', (topic, message) => {
  console.log('\n======================================');
  console.log('Mensaje recibido en topic:', topic);
  console.log('Mensaje raw:', message.toString());
  
  try {
    const payload = JSON.parse(message.toString());
    console.log('Payload parseado:', JSON.stringify(payload, null, 2));

    // Extraer identificadores básicos
    const deviceName = payload.end_device_ids?.device_id || 'desconocido';
    const applicationName = payload.end_device_ids?.application_ids?.application_id || 'unknown';
    const receivedAt = payload.received_at || new Date().toISOString();

    // Extraer frame count y puerto (si se desean almacenar)
    const frameCount = payload.uplink_message?.f_cnt || 0;
    const portNumber = payload.uplink_message?.f_port || 0;

    // Acceder a la parte decodificada con las mediciones
    const decoded = payload.uplink_message?.decoded_payload || {};
    const measurements = Array.isArray(decoded.messages) ? decoded.messages : [];

    console.log(`Datos extraídos: device_name=${deviceName}, application_name=${applicationName}, received_at=${receivedAt}`);
    console.log('Decoded Payload:', JSON.stringify(decoded, null, 2));

    if (measurements.length === 0) {
      console.log(`No se encontraron mediciones en decoded_payload.messages para el dispositivo: ${deviceName}`);
      return;
    }

    // Iterar sobre cada medición y crear un punto para InfluxDB
    measurements.forEach((measurement, index) => {
      const sensorId = measurement.measurementId?.toString() || '0';
      const sensorValue = measurement.measurementValue || 0;
      const sensorType = measurement.type || 'desconocido';

      console.log(`Medición [${index}]: sensor_id=${sensorId}, sensor_type=${sensorType}, sensor_value=${sensorValue}`);

      const point = new Point('sensor_measurement')
        .tag('application_name', applicationName)
        .tag('device_name', deviceName)
        .tag('sensor_type', sensorType)
        .stringField('sensor_id', sensorId)
        .floatField('sensor_value', sensorValue)
        .intField('frame_count', frameCount)
        .intField('port_number', portNumber)
        .timestamp(new Date(receivedAt));

      console.log('Enviando punto a InfluxDB (line protocol):', point.toLineProtocol());
      writeApi.writePoint(point);
    });

    console.log(`Se han enviado ${measurements.length} medición(es) a InfluxDB para el dispositivo: ${deviceName}`);
  } catch (error) {
    console.error('Error al procesar el mensaje MQTT:', error);
  }
});

// ---------------------------------------------------------
// 5. Cierre controlado del script (CTRL + C)
// ---------------------------------------------------------
process.on('SIGINT', async () => {
  console.log('\nSIGINT recibido. Cerrando conexiones...');
  try {
    await writeApi.close();
    console.log('Conexión a InfluxDB cerrada.');
  } catch (error) {
    console.error('Error al cerrar la conexión a InfluxDB:', error);
  }
  client.end(() => {
    console.log('Conexión MQTT cerrada.');
    process.exit(0);
  });
});

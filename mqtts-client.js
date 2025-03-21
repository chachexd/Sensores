/***************************************************************
 * mqtt-client-debug.js
 * Script para conectarse a The Things Stack vía MQTT y enviar
 * datos a InfluxDB, con logs detallados para monitorear el proceso.
 ***************************************************************/

const mqtt = require('mqtt');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

// ---------------------------------------------------------
// 1. CONFIGURACIÓN THE THINGS STACK
// ---------------------------------------------------------
const TTN_BROKER   = 'eu1.cloud.thethings.network';
const TTN_USERNAME = 'admin';
const TTN_PASSWORD = 'NNSXS.M5VJ6N22PHAY32ELIJ26K5NULLEA4QBYEX62IDA.HUW3IU5V7WNWKKZQNRQUGXYVPY42CS5KWUDW2ENOC2LJNNYGBR2A';
const TTN_TOPIC    = 'v3/prueba1@TTN/devices/+/up';

console.log('=== Configuración de The Things Stack ===');
console.log(`Broker: ${TTN_BROKER}`);
console.log(`Application ID (Usuario): ${TTN_USERNAME}`);
console.log(`API Key (Password): ${TTN_PASSWORD}`);
console.log(`Topic de suscripción: ${TTN_TOPIC}`);

// ---------------------------------------------------------
// 2. CONFIGURACIÓN INFLUXDB
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
// 3. Conexión a MQTT (The Things Stack)
// ---------------------------------------------------------
console.log('\nConectando a The Things Stack vía MQTT...');
const client = mqtt.connect(`mqtt://${TTN_BROKER}:8883`, {
  username: TTN_USERNAME,
  password: TTN_PASSWORD
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

    // Extraer datos básicos (ajusta según la estructura real de tu payload)
    const deviceId = payload.end_device_ids?.device_id || 'desconocido';
    const receivedAt = payload.received_at || new Date().toISOString();
    const decodedPayload = payload.uplink_message?.decoded_payload || {};

    console.log(`Datos extraídos: deviceId=${deviceId}, receivedAt=${receivedAt}`);
    console.log('Decoded Payload:', JSON.stringify(decodedPayload, null, 2));

    // Ejemplo: se asume que en el payload existen los campos "temp" y "hum"
    const temperature = decodedPayload.temp || 0;
    const humidity = decodedPayload.hum || 0;
    console.log(`Mediciones: temperatura=${temperature}, humedad=${humidity}`);

    // Crear un punto para InfluxDB
    const point = new Point('sensor_data')
      .tag('device', deviceId)
      .floatField('temperature', temperature)
      .floatField('humidity', humidity)
      .timestamp(new Date(receivedAt));
    
    console.log('Enviando punto a InfluxDB (line protocol):', point.toLineProtocol());
    writeApi.writePoint(point);
    console.log('Punto enviado a InfluxDB correctamente.');
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

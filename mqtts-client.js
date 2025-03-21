/***************************************************************
 * mqtt-client.js
 * Script para conectarse a The Things Stack vía MQTT y enviar
 * datos a InfluxDB.
 ***************************************************************/

//
// 1. Importar librerías
//
const mqtt = require('mqtt');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

//
// 2. CONFIGURACIÓN THE THINGS STACK
//    (REEMPLAZA los valores con tus datos)
//
const TTN_BROKER   = 'eu1.cloud.thethings.network'; // Cambia si usas otra región
const TTN_USERNAME = 'admin';           // Ej: "mi-app"
const TTN_PASSWORD = 'NNSXS.M5VJ6N22PHAY32ELIJ26K5NULLEA4QBYEX62IDA.HUW3IU5V7WNWKKZQNRQUGXYVPY42CS5KWUDW2ENOC2LJNNYGBR2A';        // Tu API Key de TTN
const TTN_TOPIC    = 'v3/prueba1@TTN/devices/+/up';
/*
  Notas:
  - TTN_BROKER: suele ser "eu1.cloud.thethings.network" o "nam1.cloud.thethings.network"
  - TTN_USERNAME: generalmente es tu Application ID
  - TTN_PASSWORD: tu API Key (empieza con "NNSXS.")
  - TTN_TOPIC: la ruta MQTT donde te suscribes. 
    Suele tener el formato: "v3/<appID>@<tenantID>/devices/+/up"
*/

//
// 3. CONFIGURACIÓN INFLUXDB
//    (REEMPLAZA con tus datos de InfluxDB)
//
const INFLUX_URL    = 'http://localhost:8086'; // Ej: "https://us-west-2-1.aws.cloud2.influxdata.com"
const INFLUX_TOKEN  = 'TU_INFLUXDB_TOKEN';
const INFLUX_ORG    = 'TU_ORG';
const INFLUX_BUCKET = 'TU_BUCKET';
/*
  Notas:
  - INFLUX_URL: dirección de tu instancia InfluxDB.
  - INFLUX_TOKEN: token de autenticación.
  - INFLUX_ORG: nombre de la organización.
  - INFLUX_BUCKET: nombre del bucket donde guardar los datos.
*/

//
// 4. Crear cliente de InfluxDB
//
const influxDB = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
const writeApi = influxDB.getWriteApi(INFLUX_ORG, INFLUX_BUCKET);

//
// 5. Crear cliente MQTT y conectar
//    Nota: se usa "mqtts" en lugar de "mqtt" si queremos conexión segura (TLS, puerto 8883).
//
const client = mqtt.connect(`mqtt://${TTN_BROKER}:8883`, {
  username: TTN_USERNAME,
  password: TTN_PASSWORD
});

client.on('connect', () => {
  console.log('Conectado a The Things Stack MQTT');
  client.subscribe(TTN_TOPIC, (err) => {
    if (!err) {
      console.log(`Suscrito al topic: ${TTN_TOPIC}`);
    } else {
      console.error('Error al suscribirse al topic', err);
    }
  });
});

//
// 6. Manejo de mensajes MQTT
//
client.on('message', (topic, message) => {
  try {
    // Convertir el mensaje a objeto JSON
    const payload = JSON.parse(message.toString());

    // Extraer datos básicos (ajusta según tu estructura de payload)
    const deviceId       = payload.end_device_ids.device_id;
    const receivedAt     = payload.received_at;
    const decodedPayload = payload.uplink_message.decoded_payload || {};

    // Supongamos que en el payload hay campos "temp" y "hum"
    // Ajusta a los campos reales que te interesen
    const temperature = decodedPayload.temp || 0;
    const humidity    = decodedPayload.hum  || 0;

    // Crear un punto para InfluxDB
    const point = new Point('sensor_data') // "measurement" en Influx
      .tag('device', deviceId)
      .floatField('temperature', temperature)
      .floatField('humidity', humidity)
      .timestamp(new Date(receivedAt));

    // Escribir el punto en InfluxDB
    writeApi.writePoint(point);

    // Mostrar por consola
    console.log('Datos escritos en InfluxDB:', {
      deviceId,
      temperature,
      humidity,
      receivedAt
    });
  } catch (error) {
    console.error('Error al procesar el mensaje MQTT:', error);
  }
});

//
// 7. Cierre controlado del script (CTRL + C)
//
process.on('SIGINT', async () => {
  console.log('Cerrando conexiones...');
  await writeApi.close();
  client.end();
  process.exit(0);
});

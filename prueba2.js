/***************************************************************
 * simulate.js
 * Procesa un archivo JSON (con un array de eventos de The Things Stack)
 * y extrae las mediciones de los mensajes uplink, insertándolas en InfluxDB
 * con nombres de tags y fields más representativos.
 ***************************************************************/

const fs = require('fs');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

// ---------------------------------------------------------
// 1. CONFIGURACIÓN DE INFLUXDB
// ---------------------------------------------------------
const INFLUX_URL    = 'http://localhost:8086'; 
const INFLUX_TOKEN  = '1znZf2FZ4syZ8HtEgDQKtm6p9T0_decSkMIX3HicbKTgy0GlU2TW0l3lcUDNoQ9fgDJYasyal2DEQ1yG3YFydg==';
const INFLUX_ORG    = 'smartfenix';
const INFLUX_BUCKET = 'smartfenix';

// Crear el cliente y el Write API
const influxDB = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
const writeApi = influxDB.getWriteApi(INFLUX_ORG, INFLUX_BUCKET);

// ---------------------------------------------------------
// 2. Función para procesar un mensaje individual
// ---------------------------------------------------------
function processMessage(message) {
  // Solo procesamos los eventos de uplink válidos
  if (message.name !== 'as.up.data.forward') return;

  const data = message.data;
  if (!data || !data.uplink_message || !data.uplink_message.decoded_payload) {
    console.log('Evento sin payload decodificado, se omite.');
    return;
  }

  // Extraer identificadores
  const deviceName      = data.end_device_ids?.device_id || 'desconocido';
  const applicationName = data.end_device_ids?.application_ids?.application_id || 'unknown';

  // Extraer información adicional (por ejemplo, contador de frames y puerto)
  const frameCount  = data.uplink_message.f_cnt || 0;
  const portNumber  = data.uplink_message.f_port || 0;

  // Determinar el timestamp (priorizando data.received_at)
  const eventTime = data.received_at || message.time;

  // Acceder a las mediciones decodificadas
  const decoded = data.uplink_message.decoded_payload;
  if (!decoded.messages || !Array.isArray(decoded.messages)) {
    console.log(`No se encontraron mediciones en el payload para el dispositivo ${deviceName}`);
    return;
  }

  // Iterar sobre cada medición y escribir un punto en InfluxDB
  decoded.messages.forEach((measurement) => {
    // Extraer datos de la medición
    const sensorId    = measurement.measurementId?.toString() || '0';
    const sensorValue = measurement.measurementValue || 0;
    const sensorType  = measurement.type || 'desconocido';

    // Crear un punto con nombres descriptivos:
    // - Measurement: "sensor_measurement"
    // - Tags: application_name, device_name, sensor_type
    // - Fields: sensor_id, sensor_value, frame_count y port_number
    const point = new Point('sensor_measurement')
      .tag('application_name', applicationName)
      .tag('device_name', deviceName)
      .tag('sensor_type', sensorType)
      .stringField('sensor_id', sensorId)
      .floatField('sensor_value', sensorValue)
      .intField('frame_count', frameCount)
      .intField('port_number', portNumber)
      .timestamp(new Date(eventTime));

    writeApi.writePoint(point);
    console.log(`Escrito en InfluxDB -> device_name: ${deviceName}, sensor_type: ${sensorType}, sensor_value: ${sensorValue}, frame_count: ${frameCount}, port_number: ${portNumber}, time: ${eventTime}`);
  });
}

// ---------------------------------------------------------
// 3. Lectura y procesamiento del archivo JSON
// ---------------------------------------------------------
if (process.argv.length < 3) {
  console.error('Uso: node simulate.js <ruta_al_archivo_json>');
  process.exit(1);
}

const jsonFilePath = process.argv[2];

fs.readFile(jsonFilePath, 'utf8', (err, fileData) => {
  if (err) {
    console.error('Error al leer el archivo JSON:', err);
    process.exit(1);
  }
  try {
    const messages = JSON.parse(fileData);
    if (!Array.isArray(messages)) {
      console.error('El archivo JSON debe contener un array de eventos.');
      process.exit(1);
    }
    // Procesar cada evento
    messages.forEach(msg => processMessage(msg));

    // Cerrar la conexión con InfluxDB
    writeApi.close()
      .then(() => console.log('Conexión a InfluxDB cerrada correctamente.'))
      .catch(e => console.error('Error al cerrar la conexión a InfluxDB:', e));
  } catch (e) {
    console.error('Error al parsear el JSON:', e);
    process.exit(1);
  }
});

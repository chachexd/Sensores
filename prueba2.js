/***************************************************************
 * simulate.js
 * Adaptado para el JSON de la "estacion-meteorologica". Procesa
 * todos los eventos, busca los de "as.up.data.forward" y extrae
 * las mediciones del payload, almacenándolas en InfluxDB.
 ***************************************************************/

const fs = require('fs');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

// ---------------------------------------------------------
// 1. CONFIGURACIÓN INFLUXDB
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
  // Solo nos interesan los eventos con "name" == "as.up.data.forward"
  if (message.name !== 'as.up.data.forward') {
    // Puedes descomentar para ver qué otros tipos de eventos aparecen:
    // console.log(`Evento ignorado (name=${message.name})`);
    return;
  }

  const data = message.data;
  if (!data || !data.uplink_message || !data.uplink_message.decoded_payload) {
    console.log('Mensaje sin payload decodificado, se omite.');
    return;
  }

  // Extraer el device_id y el application_id
  const deviceId      = data.end_device_ids?.device_id || 'desconocido';
  const applicationId = data.end_device_ids?.application_ids?.application_id || 'unknown';

  // f_cnt y f_port (por si quieres guardarlos también en Influx)
  const fCnt  = data.uplink_message.f_cnt  || 0;
  const fPort = data.uplink_message.f_port || 0;

  // Timestamp: priorizamos data.received_at; si no, usamos message.time
  const receivedAt = data.received_at || message.time;

  // Acceder a la parte decodificada
  const decoded = data.uplink_message.decoded_payload;
  if (!decoded.messages || !Array.isArray(decoded.messages)) {
    console.log(`No se encontraron mediciones en el payload para ${deviceId}`);
    return;
  }

  // Iterar sobre las mediciones que vienen en "decoded_payload.messages"
  decoded.messages.forEach((measurement) => {
    // measurementId, measurementValue y type
    const measId    = measurement.measurementId?.toString() || '0';
    const measValue = measurement.measurementValue || 0;
    const measType  = measurement.type || 'desconocido';

    // Crear un punto para cada medición
    const point = new Point('sensor_data')
      .tag('application', applicationId)
      .tag('device', deviceId)
      .tag('type', measType)
      .stringField('measurementId', measId)
      .floatField('measurementValue', measValue)
      // Guardar también fCnt y fPort (si lo deseas):
      .intField('fCnt', fCnt)
      .intField('fPort', fPort)
      .timestamp(new Date(receivedAt));

    writeApi.writePoint(point);
    console.log(`Escrito en InfluxDB -> device=${deviceId}, type=${measType}, value=${measValue}, fCnt=${fCnt}, fPort=${fPort}, time=${receivedAt}`);
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
    // El archivo es un array de objetos (eventos)
    const messages = JSON.parse(fileData);
    if (!Array.isArray(messages)) {
      console.error('El archivo JSON debe contener un array de eventos.');
      process.exit(1);
    }
    // Procesar cada evento del array
    messages.forEach(msg => processMessage(msg));

    // Cerrar la conexión con InfluxDB
    writeApi.close()
      .then(() => console.log('Conexión a InfluxDB cerrada.'))
      .catch(e => console.error('Error al cerrar la conexión a InfluxDB:', e));
  } catch (e) {
    console.error('Error al parsear el JSON:', e);
    process.exit(1);
  }
});

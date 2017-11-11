/**
 * Background Cloud Function to be triggered by Pub/Sub.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
exports.weatherBigquery = function (event, callback) {
  const BigQuery = require('@google-cloud/bigquery');
  const projectId = "brandonfreitag-sandbox";
  const datasetId = "raspberryweather_dataset";
  const tableId = "raspberryweather_table";
  const pubsubMessage = event.data;
  const incomingData = pubsubMessage.data ? Buffer.from(pubsubMessage.data, 'base64').toString() : "{'sensorID':'na','timecollected':'1/1/1970 00:00:00','zipcode':'00000','latitude':'0.0','longitude':'0.0','temperature':'-273','humidity':'-1','dewpoint':'-273','pressure':'0'}";
//  const rows = [{"pressure": "24.46", "temperature": "68.73", "dewpoint": "42.43", "timecollected": "10/31/2017 14:11:54", "latitude": "40.026176", "sensorID": "s-2525-226F", "zipcode": "80301", "longitude": "-105.259047", "humidity": "26.94"}];
//  const rows = [`${incomingData}`];
  const jsonData = JSON.parse(incomingData);
  var rows = [jsonData];

//  console.log(`Received: ${incomingData}`);
  console.log(`Rows: ${rows}`);

  // Instantiates a client
  const bigquery = BigQuery({
    projectId: projectId
  });

  // Inserts data into a table
  bigquery
    .dataset(datasetId)
    .table(tableId)
    .insert(rows)
    .then((insertErrors) => {
      console.log('Inserted:');
      rows.forEach((row) => console.log(row));

      if (insertErrors && insertErrors.length > 0) {
        console.log('Insert errors:');
        insertErrors.forEach((err) => console.error(err));
      }
    })
    .catch((err) => {
      console.error('ERROR:', err);
    });
  // [END bigquery_insert_stream]

  callback();
};

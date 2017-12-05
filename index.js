/**
 * Background Cloud Function to be triggered by Pub/Sub.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
exports.weatherBigquery = function (event, callback) {
  const BigQuery = require('@google-cloud/bigquery');
  const projectId = "myProject"; //Enter your project ID here
  const datasetId = "myDataset"; //Enter your BigQuery dataset name here
  const tableId = "myTable"; //Enter your BigQuery table name here -- make sure it is setup correctly
  const pubsubMessage = event.data;
  // Incoming data is in JSON format
  const incomingData = pubsubMessage.data ? Buffer.from(pubsubMessage.data, 'base64').toString() : "{'sensorID':'na','timecollected':'1/1/1970 00:00:00','zipcode':'00000','latitude':'0.0','longitude':'0.0','temperature':'-273','humidity':'-1','dewpoint':'-273','pressure':'0'}";
  const jsonData = JSON.parse(incomingData);
  var rows = [jsonData];

  console.log(`Incoming data: ${rows}`);

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

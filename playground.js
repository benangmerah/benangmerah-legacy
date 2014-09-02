// BenangMerah
// To run: node server.js

// modules
var config = require('config');
var conn = require('starmutt');
var api = require('./lib/api');
var querystring = require('querystring');
var _ = require('lodash');
var shared = require('./lib/shared');

// STARDOG INIT ---
if (config.stardog) {
  conn.setEndpoint(config.stardog.endpoint);
  conn.setCredentials(config.stardog.username, config.stardog.password);
  conn.setDefaultDatabase(config.stardog.database);
  conn.setConcurrency(config.stardog.concurrency || 4);
}

var comparisonEnabled = false;
var allObservations = [];
var allAreas = {};
var observationsByArea = {};
var areaFullNameIndex = {};

var params = {};
var req = {
  resourceURI: 'http://data.ukp.go.id/dataset/data-bantuan-langsung-masyarakat-dan-kemiskinan-bali#jumlah_penduduk_miskin'
}
api.rankings({
  '@id': req.resourceURI,
  // where: { 'bm:refPeriod': {'@value': '2009', '@type': 'xsd:gYear'}}
  where: {}
})
.then(function() {
  var allObservationsPromise = api.rankings({
    '@id': req.resourceURI,
    where: {}
  }).then(function(data) {
    allObservations = data;

    _.forEach(allObservations, function(observation) {
      var area = observation['bm:refArea'];
      var parent = area['bm:hasParent'];
      var areaId = area['@id'];
      if (!allAreas[areaId]) {
        allAreas[areaId] = area;
      }

      var areaFullName = shared.getPreferredLabel(area);
      if (parent) {
        areaFullName += ', ' + shared.getPreferredLabel(parent);
      }
      if (!areaFullNameIndex[areaFullName]) {
        areaFullNameIndex = areaId;
      }

      if (!observationsByArea[areaId]) {
        observationsByArea[areaId] = [];
      }

      observationsByArea[areaId].push(observation);
    });
  });

  return allObservationsPromise;
})
.then(function() {
  console.dir(observationsByArea);
});

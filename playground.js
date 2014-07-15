// BenangMerah
// To run: node server.js

// modules
var util = require('util');
var config = require('config');
var conn = require('starmutt');
var shared = require('./shared');
var async = require('async');
var _ = require('lodash');
var helpers = require('./helpers');
var logger = require('winston');
var jsonld = require('jsonld');

var n3 = require('n3');
var n3util = n3.Util;

// STARDOG INIT ---
if (config.stardog) {
  conn.setEndpoint(config.stardog.endpoint);
  conn.setCredentials(config.stardog.username, config.stardog.password);
  conn.setDefaultDatabase(config.stardog.database);
}
var res = 'http://benangmerah.net/place/idn/jawa-barat/kota-bandung'
var condition = util.format(
  'graph ?g { ?observation qb:dataSet <http://data.ukp.go.id/dataset/ipm-dan-komponennya-per-kabupaten#>. } ');
shared.getDatacube(condition, [], function(err, data) {
  console.log(err, data);
})

var q = 'construct { ?observation ?p ?o } where { graph ?g { ?observation a qb:Observation.  ?observation ?p ?o. } }';
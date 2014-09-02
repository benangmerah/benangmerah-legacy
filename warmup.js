// BenangMerah
// To run: node server.js

// modules
var config = require('config');
var conn = require('starmutt');
var api = require('./lib/api');
var _ = require('lodash');
var shared = require('./lib/shared');
var request = require('request');
var async = require('async');

// STARDOG INIT ---
if (config.stardog) {
  conn.setEndpoint(config.stardog.endpoint);
  conn.setCredentials(config.stardog.username, config.stardog.password);
  conn.setDefaultDatabase(config.stardog.database);
  conn.setConcurrency(config.stardog.concurrency || 4);
}

var base = 'http://localhost:3000';
var logStream = process.stderr;

var queue = async.queue(function(job, callback) {
  logStream.write('Fetching ' + job + '...\n');
  request(base + job, function(err) {
    if (err) {
      logStream.write('Error: ' + err + '\n');
      callback(err);
    }
    else {
      logStream.write('Finished fetching ' + job + '.\n');
      callback();
    }
  });
}, 2);

var query = 'SELECT DISTINCT ?x WHERE { ?x a [] }';
conn.getColValues(query).then(function(data) {
  _.each(data, function(subject) {
    var url = shared.getDescriptionPath(subject);
    queue.push(url);
  });
});
var crypto = require('crypto');
var stream = require('stream');
var util = require('util');

var async = require('async');
var config = require('config');
var conn = require('starmutt');
var express = require('express');
var logger = require('winston');
var n3 = require('n3');
var yaml = require('js-yaml');
var _ = require('lodash');

var shared = require('../shared');

var dataManager = {};
module.exports = dataManager;

var metaGraphIri = dataManager.metaGraphIri ||
  (config.dataManager && config.dataManager.metaGraphIri) ||
  shared.META_NS;

dataManager.availableDrivers = [];
dataManager.driverDetails = {};
dataManager.driverInstances = [];
dataManager.instanceLogs = {};
dataManager.instanceObjects = {};

dataManager.DriverSparqlStream = require('./DriverSparqlStream');
dataManager.DriverInstance = require('./DriverInstance');

for (var key in require('../../package').dependencies) {
  if (/^benangmerah-driver-/.test(key)) {
    dataManager.availableDrivers.push(key);
    dataManager.driverDetails[key] = require(key + '/package');
  }
}

dataManager.fetchDriverInstances = function(callback) {
  conn.getGraph({
    query: 'CONSTRUCT { ?x ?p ?o. } ' +
           'WHERE { GRAPH <' + metaGraphIri + '> { ' +
           ' ?x a bm:DriverInstance. ?x ?p ?o.' +
           ' FILTER (?p != bm:specifies) } }',
    form: 'compact',
    context: shared.context,
    cache: false
  }, function(err, data) {
    if (err) {
      return callback(err);
    }

    delete data['@context'];

    var graph;
    if (data['@graph']) {
      graph = data['@graph'];
    }
    else if (!_.isEmpty(data)) {
      graph = [data];
    }

    if (_.isEmpty(graph)) {
      return callback();
    }

    dataManager.driverInstances = _.map(graph, function(rawDriverInstance) {
      return new dataManager.DriverInstance(rawDriverInstance);
    });

    // Clear references to stale graphs
    var instanceIds = _.pluck(dataManager.driverInstances, '@id');
    for (var id in dataManager.instanceObjects) {
      if (instanceIds.indexOf(id) === -1) {
        delete dataManager.instanceObjects[id];
        delete dataManager.instanceLogs[id];
      }
    }

    return callback();
  });
};

dataManager.getInstance = function(instanceId) {
  for (var i = 0; i < dataManager.driverInstances.length; ++i) {
    var instance = dataManager.driverInstances[i];
    if (instance['@id'] === instanceId) {
      return instance;
    }
  }

  return null;
};

dataManager.createInstance = function(instanceData, callback) {
  conn.insertGraph({
    '@context': shared.context,
    '@id': instanceData['@id'],
    '@type': 'bm:DriverInstance',
    'rdfs:label': instanceData['rdfs:label'],
    'rdfs:comment': instanceData['rdfs:comment'],
    'bm:driverName': instanceData['bm:driverName'],
    'bm:optionsYAML': instanceData['bm:optionsYAML'],
    'bm:enabled': instanceData['bm:enabled'] ? true : false
  }, metaGraphIri, function(err, data) {
    if (err) {
      return callback(err);
    }

    dataManager.fetchDriverInstances(callback);
  });
};
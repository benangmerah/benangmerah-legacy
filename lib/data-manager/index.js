var config = require('config');
var conn = require('starmutt');
var Promise = require('bluebird');
var yaml = require('js-yaml');
var _ = require('lodash');

var shared = require('../shared');

var dataManager = {};
module.exports = dataManager;

var metaGraphIri = dataManager.metaGraphIri = 
  (config.dataManager && config.dataManager.metaGraphIri) ||
  shared.META_NS;

dataManager.availableDrivers = [];
dataManager.driverDetails = {};
dataManager.driverInstances = [];
dataManager.instanceLogs = {};
dataManager.instanceObjects = {};
dataManager.sparqlStreams = {};

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

dataManager.fetchAllInstancesOfDriver = function(driverName) {
  var promise = Promise.resolve();
  _.forEach(dataManager.driverInstances, function(instance) {
    if (instance['bm:driverName'] !== driverName) {
      return;
    }

    instance.log('info', 'Queued for fetching...');
    promise = promise.then(function() {
      return instance.fetch();
    });
  });

  promise = promise.catch(function(err) {
    throw err;
  });

  return promise;
};
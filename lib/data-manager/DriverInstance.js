var crypto = require('crypto');
var stream = require('stream');
var util = require('util');

var _ = require('lodash');
var async = require('async');
var config = require('config');
var conn = require('starmutt');
var logger = require('winston');
var yaml = require('js-yaml');

var shared = require('../shared');
var dataManager = require('./index');
var DriverSparqlStream = require('./DriverSparqlStream');

var availableDrivers = dataManager.availableDrivers;
var instanceLogs = dataManager.instanceLogs;
var instanceObjects = dataManager.instanceObjects;
var toNT = shared.toNT;

var metaGraphIri = dataManager.metaGraphIri ||
  (config.dataManager && config.dataManager.metaGraphIri) ||
  shared.META_NS;

module.exports = DriverInstance;

function DriverInstance(rawDriverInstance) {
  var self = this;

  _.assign(self, rawDriverInstance);
  var instanceId = self['@id'];

  // Logs
  if (!instanceLogs[instanceId]) {
    instanceLogs[instanceId] = [];
  }
  self.logs = instanceLogs[instanceId]; // Pointer

  self.isMeta = _.contains(this['bm:driverName'], '-meta-');

  try {
    self.parseOptions();
    self.initDriver();
  }
  catch (e) {
    self['bm:enabled'] = false;
    self.log('error', e);
    self.log('error', 'Disabled.');
  }
}

DriverInstance.prototype.parseOptions = function() {
  var self = this;
  var optionsObject = yaml.safeLoad(self['bm:optionsYAML']);
  self.options = optionsObject;
};

DriverInstance.prototype.initDriver = function() {
  var self = this;
  var driverObject;

  // Identify the appropriate driver
  var driverName = self['bm:driverName'];
  if (!_.contains(availableDrivers, driverName)) {
    throw new Error('Driver ' + driverName + ' does not exist.');
  }

  // Construct the driver driverObject object
  var constructor = require(driverName);

  if (self.driverObject &&
      self.driverObject.constructor === constructor) {
    return;
  }

  self.log('info', 'Initialising...');

  driverObject = new constructor();
  driverObject.setOptions(self.options);

  self.driverObject = driverObject;
  self.attachEvents();

  self.log('finish', 'Initialised.');
};

DriverInstance.prototype.attachEvents = function() {
  var self = this;
  var driverObject = self.driverObject;

  self.initStreams();
  driverObject.on('addTriple', function(s, p, o) {
    if (!self.sparqlStream) {
      self.initStreams();
    }

    self.sparqlStream.write({
      subject: toNT(s),
      predicate: toNT(p),
      object: toNT(o)
    });
  });

  driverObject.on('log', function(level, message) {
    self.log(level, message);
  });

  driverObject.on('finish', function() {
    self.sparqlStream.end();
    self.log('info', 'Finished fetching.');
  });
};

Object.defineProperty(DriverInstance.prototype, 'driverObject', {
  get: function() {
    var instanceId = this['@id'];
    return instanceObjects[instanceId];
  },
  set: function(obj) {
    var instanceId = this['@id'];
    instanceObjects[instanceId] = obj;
  }
});

DriverInstance.prototype.log = function(level, message) {
  this.logs.push({
    level: level,
    message: message,
    timestamp: _.now()
  });

  logger.log(
    level === 'finish' ? 'info' : level,
    this['@id'] + ': ' + message
  );
};

Object.defineProperty(DriverInstance.prototype, 'lastLog', {
  get: function() {
    if (this.logs.length === 0) {
      return undefined;
    }

    return this.logs[this.logs.length - 1];
  }
});

DriverInstance.prototype.initStreams = function() {
  var self = this;

  self.sparqlStream = new DriverSparqlStream({
    instance: self,
    graphUri: self['@id']
  });

  self.sparqlStream.on('end', function() {
    self.flushStreams();
  });
};

DriverInstance.prototype.flushStreams = function() {
  var self = this;

  delete self.sparqlStream;
  delete self.isFetching;

  if (!_.contains(self['bm:driverName'], '-meta-')) {
    return;
  }

  self.log('info', 'Meta driver: refreshing driver driverObject cache...');
  dataManager.fetchDriverInstances(function() {
    self.log('finish', 'Idle.');
  });
};

DriverInstance.prototype.fetch = function() {
  var self = this;

  if (!self['bm:enabled'] || self.isFetching) {
    return;
  }

  self.isFetching = true;
  self.log('info', 'Fetching...');
  process.nextTick(function() {
    self.driverObject.fetch();
  });
};

DriverInstance.prototype.clear = function() {
  var self = this;

  var instanceId = self['@id'];
  var tripleResults = [];

  var driverObject = self.driverObject;

  self.log('info', 'Initiating clearing routine...');

  function getTriples(callback) {
    var getTriplesQuery =
      'select ?tripleId ?subject ?predicate ' + 
        '?object (count(distinct ?d) as ?count) ' +
      'where { graph <' + metaGraphIri + '> { ' +
        '<' + instanceId + '> bm:specifies ?tripleId. ' +
        '?tripleId rdf:subject ?subject; ' + 
          'rdf:predicate ?predicate; ' +
          'rdf:object ?object. ' +
        'optional { ?d bm:specifies ?tripleId } ' +
      '} } ' +
      'group by ?tripleId ?subject ?predicate ?object';

    self.log('info', 'Fetching corresponding triples...');
    conn.getResults({ query: getTriplesQuery, cache: false },
      function(err, results) {
        if (err) {
          return callback(err);
        }

        tripleResults = results;

        callback();
      });
  }

  function clearTriples(callback) {
    self.log('info', tripleResults.length + ' triples found.');

    var mainFragment = '';
    tripleResults.forEach(function(result) {
      if (result.count.value > 1) {
        return;
      }

      mainFragment +=
        toNT(result.subject) + ' ' +
        toNT(result.predicate) + ' ' +
        toNT(result.object) + '.\n';
    });

    var query;
    if (self.isMeta) {
      query =
        'DELETE DATA { GRAPH <' + metaGraphIri + '> {' + mainFragment + '} }';
    }
    else {
      query = 'DELETE DATA {' + mainFragment + '}';
    }

    self.log('info', 'Deleting triples...');
    DriverSparqlStream.queue.push({
      query: query,
      instance: self
    }, function(err) {
      if (err) {
        return callback(err);
      }

      self.log('finish', 'Finished deleting triples.');
      return callback();
    });
  }

  function clearMeta(callback) {
    var query =
      'DELETE { GRAPH <' + metaGraphIri + '> {' +
        '<' + self['@id'] + '> bm:specifies ?s.\n' +
        '?s ?p ?o } }' +
      'WHERE { GRAPH <' + metaGraphIri + '> {' +
        '<' + self['@id'] + '> bm:specifies ?s.\n' +
        '?s ?p ?o } }';

    self.log('info', 'Deleting meta triples...');
    DriverSparqlStream.queue.push({
      query: query,
      instance: self
    }, function(err) {
      if (err) {
        return callback(err);
      }

      self.log('finish', 'Finished deleting meta triples.');
      return callback();
    });
  }

  async.series([getTriples, clearTriples, clearMeta], function(err) {
    if (err) {
      return self.log('error', err);
    }

    self.log('finish', 'Finished clearing.');
  });
};

DriverInstance.prototype.deleteMeta = function(callback) {
  var id = this['@id'];
  var query =
    'DELETE { GRAPH <' + metaGraphIri + '> { <' + id + '> ?y ?z } } ' +
    'WHERE { GRAPH <' + metaGraphIri + '> { <' + id + '> ?y ?z } }';

  DriverSparqlStream.queue.push({
    query: query,
    instance: this
  }, callback);
};

DriverInstance.prototype.delete = function(callback) {
  var self = this;
  this.clear(function(err) {
    if (err) {
      return callback(err);
    }

    self.deleteMeta(callback);
  });
};
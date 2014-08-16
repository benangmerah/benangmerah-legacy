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
  self.logs = instanceLogs[instanceId]; // Reference

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
    driverObject: self,
    graphUri: self['@id'],
    isMeta: _.contains(this['bm:driverName'], '-meta-')
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
  if (!this['bm:enabled'] || this.isFetching) {
    return;
  }

  this.isFetching = true;
  this.log('info', 'Fetching...');
  this.driverObject.fetch();
};

DriverInstance.prototype.clear = function(callback) {
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

        tripleResults = _.filter(results, function(result) {
          return result.count.value <= 1;
        });

        callback();
      });
  }

  function clearTriples(callback) {
    self.log('info', tripleResults.length + ' triples found.');

    var query = 'delete data {';
    tripleResults.forEach(function(result) {
      query +=
        toNT(result.subject) + ' ' +
        toNT(result.predicate) + ' ' +
        toNT(result.object) + '.\n';
    });

    query += 'graph <' + metaGraphIri + '> {';
    tripleResults.forEach(function(result) {
      query +=
        '<' + instanceId + '> bm:specifies <' +
        result.tripleId.value + '>.\n';
    });

    query += '} }';

    self.log('info', 'Deleting triples...');
    conn.execQuery(query, function(err) {
      if (err) {
        return callback(err);
      }

      driverObject.log('finish', 'Finished clearing.');
      callback();
    });
  }

  async.series([getTriples, clearTriples], callback);
};

DriverInstance.prototype.deleteMeta = function(callback) {
  var id = this['@id'];
  var query =
    'delete { graph <' + metaGraphIri + '> { <' + id + '> ?y ?z } } ' +
    'where { graph <' + metaGraphIri + '> { <' + id + '> ?y ?z } }';

  conn.execQuery(query, callback);
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
var crypto = require('crypto');
var stream = require('stream');
var util = require('util');

var _ = require('lodash');
var async = require('async');
var config = require('config');
var conn = require('starmutt');

var shared = require('../shared');
var dataManager = require('./index');

var metaGraphIri = dataManager.metaGraphIri;
var fragmentLength = 
  config.dataManager && config.dataManager.fragmentLength || 1048576;
var concurrency =
  config.dataManager && config.dataManager.concurrency || 1;

module.exports = DriverSparqlStream;

function DriverSparqlStream(options) {
  DriverSparqlStream.super_.call(this, {
    decodeStrings: false,
    objectMode: true
  });
  this.instance = options.instance;
  this.graphUri = options.graphUri;
  this.isMeta = this.instance.isMeta;

  this.mainBuffer = '';
  this.metaBuffer = '';
  this.charCount = 0;
  this.queryCount = 0;
  this.pendingQueryCount = 0;
}

util.inherits(DriverSparqlStream, stream.Transform);

DriverSparqlStream.queue = async.queue(function(task, callback) {
  var query = task.query;
  var instance = task.instance;

  instance.log('info',
    'Executing SPARQL query... (length=' + query.length + ')');
  var start = _.now();

  conn.execQuery(query, function(err) {
    if (err) {
      instance.log('error', 'Query failed: ' + err);
      return callback(err);
    }

    var delta = _.now() - start;
    instance.log('info', 'Query completed in ' + delta + 'ms.');
    return callback();
  });
}, concurrency);

DriverSparqlStream.prototype._transform = function(triple, encoding, callback) {
  var tripleString =
    triple.subject + ' ' +
    triple.predicate + ' ' +
    triple.object + '.\n';

  var hash = crypto.createHash('sha1');
  hash.update(tripleString);
  var tripleHash = hash.digest('hex');
  var tripleHashIri = shared.META_NS + 'triple/' + tripleHash;

  var metaTripleString =
    '<' + this.graphUri + '> bm:specifies <' + tripleHashIri + '>.\n' +
    '<' + tripleHashIri + '> a rdf:Statement.\n' +
    '<' + tripleHashIri + '> rdf:subject ' + triple.subject + '.\n' +
    '<' + tripleHashIri + '> rdf:predicate ' + triple.predicate + '.\n' +
    '<' + tripleHashIri + '> rdf:object ' + triple.object + '.\n';

  this.mainBuffer += tripleString;
  this.metaBuffer += metaTripleString;
  this.charCount += tripleString.length + metaTripleString.length;

  if (this.charCount >= fragmentLength) {
    this.charCount = 0;
    this.commit();
    this.mainBuffer = '';
    this.metaBuffer = '';
  }
  callback();
};

DriverSparqlStream.prototype._flush = function(callback) {
  this.finished = true;
  this.commit();
  callback();
};

DriverSparqlStream.prototype.commit = function(callback) {
  var self = this;
  var query;
  var mainFragment = self.mainBuffer;
  var metaFragment = self.metaBuffer;

  if (self.isMeta) {
    query =
      'INSERT DATA { GRAPH <' + metaGraphIri + '> {' +
      mainFragment + metaFragment + ' } }';
  }
  else {
    query =
      'INSERT DATA { ' + mainFragment +
      'GRAPH <' + metaGraphIri + '> {' +
      metaFragment + ' } }';
  }

  self.pushQuery(query);
};

DriverSparqlStream.prototype.pushQuery = function(query) {
  var self = this;

  ++self.queryCount;
  ++self.pendingQueryCount;
  DriverSparqlStream.queue.push(
    { query: query, instance: self.instance },
    function() {
      --self.pendingQueryCount;
      if (self.finished && self.pendingQueryCount === 0) {
        self.instance.log('info', self.queryCount + ' queries completed.');
        self.instance.log('finish', 'Idle.');
        self.emit('end');
      }
    });
};
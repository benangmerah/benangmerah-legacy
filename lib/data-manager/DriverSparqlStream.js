var crypto = require('crypto');
var events = require('events');
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
  this.instance = options.instance;
  this.graphUri = options.graphUri;
  this.isMeta = this.instance.isMeta;

  this.lastSubject = '';
  this.lastPredicate = '';
  this.mainBuffer = '';
  this.metaBuffer = '';
  this.charCount = 0;
  this.queryCount = 0;
  this.pendingQueryCount = 0;
}

util.inherits(DriverSparqlStream, events.EventEmitter);

// For debugging purposes
var queryDump = require('fs').createWriteStream('dmqueries.sparql');
var tripleDump = require('fs').createWriteStream('dmtriples.ttl');
var metaDump = require('fs').createWriteStream('dmmeta.ttl');
_.forEach(shared.context, function(uri, prefix) {
  var line = '@prefix ' + prefix + ': <' + uri + '>.\n';
  tripleDump.write(line);
  metaDump.write(line);
});
tripleDump.write('\n');
metaDump.write('\n');

DriverSparqlStream.queue = async.queue(function(task, callback) {
  var query = task.query;
  var instance = task.instance;

  // For debugging purposes
  queryDump.write(query);
  queryDump.write(';\n\n');

  instance.log('info',
    'Executing SPARQL query... (length=' + query.length + ')');
  var start = _.now();

  conn.execQuery(query).then(function() {
    var delta = _.now() - start;
    instance.log('info', 'Query completed in ' + delta + 'ms.');
    callback();
  }).catch(function(err) {
    instance.log('error', 'Query failed: ' + err);
    callback(err);
  });
}, concurrency);

DriverSparqlStream.prototype.write = function(triple) {
  var subject = triple.subject;
  var predicate = triple.predicate === 'rdf:type' ? 'a' : triple.predicate;
  var object = triple.object;

  var mainAppend = '';
  if (subject === this.lastSubject) {
    if (predicate === this.lastPredicate) {
      mainAppend = ',\n    ' + object;
    }
    else {
      mainAppend = ';\n  ' + predicate + ' ' + object;
    }
  }
  else {
    if (this.lastSubject) {
      mainAppend = '.\n';
    }
    mainAppend += 
      subject + ' ' +
      predicate + ' ' +
      object;
  }
  this.lastSubject = subject;
  this.lastPredicate = predicate;
  this.mainBuffer += mainAppend;

  var canonicalTripleString =
    subject + ' ' +
    predicate + ' ' +
    object + '.\n';

  var hash = crypto.createHash('sha1');
  hash.update(canonicalTripleString);
  var tripleHash = hash.digest('hex');
  var tripleHashIri = shared.META_NS + 'triple/' + tripleHash;

  var metaTripleString =
    '<' + this.graphUri + '> bm:specifies <' + tripleHashIri + '>.\n' +
    '<' + tripleHashIri + '> a rdf:Statement;\n' +
    '  rdf:subject ' + triple.subject + ';\n' +
    '  rdf:predicate ' + triple.predicate + ';\n' +
    '  rdf:object ' + triple.object + '.\n';
  this.metaBuffer += metaTripleString;

  this.charCount += mainAppend.length + metaTripleString.length;

  if (this.charCount >= fragmentLength) {
    this.mainBuffer += '.\n';
    this.commit();
    this.charCount = 0;
    this.lastSubject = '';
    this.lastPredicate = '';
    this.mainBuffer = '';
    this.metaBuffer = '';
  }
};

DriverSparqlStream.prototype.end = function() {
  this.finished = true;
  this.mainBuffer += '.\n';
  this.commit();
  this.charCount = 0;
  this.lastSubject = '';
  this.lastPredicate = '';
  this.mainBuffer = '';
  this.metaBuffer = '';
};

DriverSparqlStream.prototype.commit = function(callback) {
  var self = this;
  var mainFragment = self.mainBuffer;
  var metaFragment = self.metaBuffer;

  tripleDump.write(mainFragment);
  tripleDump.write('\n# ---\n');
  metaDump.write(metaFragment);

  if (self.isMeta) {
    self.pushQuery(
      'INSERT DATA { GRAPH <' + metaGraphIri + '> {\n' +
      mainFragment + metaFragment + ' } }'
    );
  }
  else {
    self.pushQuery('INSERT DATA {\n' + mainFragment + '}');
    self.pushQuery(
      'INSERT DATA { GRAPH <' + metaGraphIri + '> {\n' +
      metaFragment + ' } }'
    );
  }
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
        self.emit('end');
      }
    });
};
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

var router = express.Router();
module.exports = router;

// Data Manager config
var metaGraphIri = shared.META_NS;
var instancesGraphUri =
  config.dataManager && config.dataManager.instancesGraphUri ||
  'tag:benangmerah.net:driver-instances';
var concurrency =
  config.dataManager && config.dataManager.concurrency || 1;
var fragmentLength =
  config.dataManager && config.dataManager.fragmentLength || 1048576;

var initiated = false;
var availableDrivers = [];
var driverDetails = {};
for (var key in require('../package').dependencies) {
  if (/^benangmerah-driver-/.test(key)) {
    availableDrivers.push(key);
    driverDetails[key] = require(key + '/package');
  }
}

var driverInstances = [];
var instanceLogs = {};
var instanceObjects = {};

var sharedQueryQueue = async.queue(function(task, callback) {
  var query = task.query;
  var instance = task.instance;

  instance.log('info',
    'Executing SPARQL query... (length=' + query.length + ')');
  var start = _.now();

  conn.execQuery(query, function(err) {
    if (err) {
      instance.instance.error('Query failed: ' + err);
      return callback(err);
    }

    var delta = _.now() - start;
    instance.log('info', 'Query completed in ' + delta + 'ms.');
    return callback();
  });
}, concurrency);

function DriverSparqlStream(options) {
  DriverSparqlStream.super_.call(this, {
    decodeStrings: false,
    objectMode: true
  });
  this.instance = options.instance;
  this.graphUri = options.graphUri;

  this.mainBuffer = '';
  this.metaBuffer = '';
  this.charCount = 0;
  this.queryCount = 0;
  this.pendingQueryCount = 0;
}

util.inherits(DriverSparqlStream, stream.Transform);

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
  var baseQuery, query;
  var mainFragment = self.mainBuffer;
  var metaFragment = self.metaBuffer;

  if (self.isMeta) {
    baseQuery = 'INSERT DATA { GRAPH <%s> { %s %s } }\n';
    query = util.format(baseQuery, metaGraphIri, mainFragment, metaFragment);
  }
  else {
    baseQuery = 'INSERT DATA { %s GRAPH <%s> { %s } }\n';
    query = util.format(baseQuery, mainFragment, metaGraphIri, metaFragment);
  }

  ++self.queryCount;
  ++self.pendingQueryCount;
  sharedQueryQueue.push(
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

function prepareInstance(rawDriverInstance) {
  var preparedInstance = rawDriverInstance;
  var instanceId = preparedInstance['@id'];

  // Logs
  if (!instanceLogs[instanceId]) {
    instanceLogs[instanceId] = [];
  }
  preparedInstance.logs = instanceLogs[instanceId]; // Reference
  preparedInstance.log = function(level, message) {
    this.logs.push({
      level: level,
      message: message,
      timestamp: _.now()
    });

    logger.log(
      level === 'finish' ? 'info' : level,
      instanceId + ': ' + message
    );
  };
  Object.defineProperty(preparedInstance, 'lastLog', {
    get: function() {
      if (this.logs.length === 0) {
        return undefined;
      }

      return this.logs[this.logs.length - 1];
    }
  });

  // Parse optionsYAML
  try {
    var optionsObject = yaml.safeLoad(preparedInstance['bm:optionsYAML']);
    preparedInstance.options = optionsObject;
  }
  catch (e) {
    preparedInstance['bm:enabled'] = false;
    preparedInstance.log('error', e);
  }

  if (!preparedInstance['bm:enabled']) {
    preparedInstance.log('error', 'Disabled.');
    return preparedInstance;
  }

  // Identify the appropriate driver
  var driverName = preparedInstance['bm:driverName'];
  if (!_.contains(availableDrivers, driverName)) {
    preparedInstance['bm:enabled'] = false;
    preparedInstance.log('error', 'Driver does not exist.');
    return preparedInstance;
  }

  Object.defineProperty(preparedInstance, 'instance', {
    get: function() {
      return instanceObjects[instanceId];
    },
    set: function(obj) {
      instanceObjects[instanceId] = obj;
    }
  });

  // Construct the driver instance object
  try {
    var constructor = require(driverName);

    if (preparedInstance.instance &&
        preparedInstance.instance.constructor === constructor) {
      return preparedInstance;
    }

    preparedInstance.log('info', 'Initialising...');

    var instance = new constructor();
    instance.setOptions(preparedInstance.options);

    var sparqlStream, tripleWriter;

    var initStreams = function() {
      sparqlStream = new DriverSparqlStream({
        instance: preparedInstance,
        graphUri: preparedInstance['@id'],
        isMeta: _.contains(driverName, '-meta-')
      });
      tripleWriter = n3.Writer(sparqlStream);

      sparqlStream.on('end', onEnd);
    };
    var onEnd = function() {
      sparqlStream = undefined;
      tripleWriter = undefined;
      delete preparedInstance.isFetching;

      if (driverName.indexOf('meta-') === -1) {
        return;
      }

      preparedInstance.log('info',
        'Meta driver: refreshing driver instance cache...');
      fetchDriverInstances(function() {
        preparedInstance.log('finish', 'Idle.');
      });
    };

    initStreams();
    instance.on('addTriple', function(s, p, o) {
      if (!tripleWriter) {
        initStreams();
      }

      // tripleWriter.addTriple(s, p, o);
      sparqlStream.write({
        subject: toNT(s),
        predicate: toNT(p),
        object: toNT(o)
      });
    });
    instance.on('log', function(level, message) {
      preparedInstance.log(level, message);
    });
    instance.on('finish', function() {
      tripleWriter.end();
      preparedInstance.log('info', 'Finished fetching.');
    });

    preparedInstance.log('finish', 'Initialised.');
    preparedInstance.instance = instance;
  }
  catch (e) {
    preparedInstance['bm:enabled'] = false;
    preparedInstance.log('error', e);
  }

  return preparedInstance;
}

function fetchDriverInstances(callback) {
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

    driverInstances = _.map(graph, prepareInstance);

    // Clear references to stale graphs
    var instanceIds = _.pluck(driverInstances, '@id');
    for (var id in instanceObjects) {
      if (instanceIds.indexOf(id) === -1) {
        delete instanceObjects[id];
        delete instanceLogs[id];
      }
    }

    return callback();
  });
}

var literalEscape    = /["\\\t\n\r\b\f]/;
var literalEscapeAll = /["\\\t\n\r\b\f]/g;
var literalReplacements = 
      { '\\': '\\\\', '"': '\\"', '\t': '\\t',
        '\n': '\\n', '\r': '\\r', '\b': '\\b', '\f': '\\f' };
var literalMatcher = /^"((?:.|\n|\r)*)"(?:\^\^<(.+)>|@([\-a-z]+))?$/i;
var prefixUris = _.invert(shared.prefixes);
function toNT(node) {
  if (!node) {
    return '';
  }

  var value, buf;
  if (node.type) {
    // SPARQL binding
    if (node.type === 'literal') {
      buf = '';

      value = node.value;
      if (literalEscape.test(value)) {
        value = value.replace(literalEscapeAll, function (match) {
          return literalReplacements[match];
        });
      }
      buf += '"' + value + '"';

      if (node.datatype) {
        buf += '^^';
        buf += toNT(node.datatype);
      }

      if (node.language) {
        buf += '@' + node.language;
      }

      return buf;
    }

    if (node.type === 'bnode') {
      return '_:' + node.value;
    }

    return toNT(node.value);
  }

  if (node[0] === '"') {
    // literal
    var literalMatch = literalMatcher.exec(node);
    value = literalMatch[1];
    var type = literalMatch[2];
    var language = literalMatch[3];
    buf = '';
    if (literalEscape.test(value)) {
      value = value.replace(literalEscapeAll, function (match) {
        return literalReplacements[match];
      });
    }

    buf = '"' + value + '"';
    if (type) {
      buf += '^^';
      buf += toNT(type);
    }
    else if (language) {
      buf += '@' + language;
    }

    return buf;
  }

  if (node[0] === '_') {
    // bnode
    return node;
  }

  // named node
  var prefixMatch = node.match(/^(.*[#\/])([a-z][\-_a-z0-9]*)$/i);
  if (prefixMatch && prefixUris[prefixMatch[1]]) {
    return prefixUris[prefixMatch[1]] + ':' + prefixMatch[2];
  }

  return '<' + node + '>';
}

function clearInstanceData(instanceId, callback) {
  var tripleResults = [];

  var instance = instanceObjects[instanceId];

  instance.info('Initiating clearing routine...');

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

    instance.info('Fetching corresponding triples...');
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
    instance.info(tripleResults.length + ' triples found.');

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

    instance.info('Deleting triples...');
    conn.execQuery(query, function(err) {
      if (err) {
        return callback(err);
      }

      instance.log('finish', 'Finished clearing.');
      callback();
    });
  }

  async.series([getTriples, clearTriples], callback);
}

// ---

// TODO implement authentication
function requireAuthentication(req, res, next) {
  next();
}

function init(req, res, next) {
  res.locals.layout = 'layouts/data-manager';
  res.locals.availableDrivers = availableDrivers;
  res.locals.driverDetails = driverDetails;

  if (!initiated) {
    return fetchDriverInstances(function() {
      initiated = true;
      next();
    });
  }

  next();
}

function index(req, res, next) {
  function render(err) {
    if (err) {
      res.locals.error = err;
    }

    res.locals.driverInstances = driverInstances;
    res.locals.query = req.query;
    res.locals.queryQueueLength = sharedQueryQueue.length();

    res.render('data-manager/index');
  }

  fetchDriverInstances(render);
}

function viewInstance(req, res, next) {
  var id = req.params.id;
  var instanceData = {};

  function getInstance(callback) {
    if (!id) {
      return callback('not_found');
    }

    for (var i = 0; i < driverInstances.length; ++i) {
      var instance = driverInstances[i];
      if (instance['@id'] === id) {
        instanceData = instance;
        return callback();
      }
    }

    return callback('not_found');
  }

  function render(err) {
    if (err) {
      return next(err);
    }

    return res.render('data-manager/view-instance', instanceData);
  }

  async.series([getInstance], render);
}

function createInstance(req, res, next) {
  function render(err) {
    res.render('data-manager/create-instance', {
      mode: 'create',
      availableDrivers: availableDrivers
    });
  }

  async.series([], render);
}

function submitCreateInstance(req, res, next) {
  var formErrors = [];

  function validateForm(callback) {
    if (!req.body['@id']) {
      formErrors.push('No ID specified.');
    }
    if (!req.body['bm:driverName']) {
      formErrors.push('No driver specified.');
    }
    if (req.body['bm:optionsYAML'] &&
        !yaml.safeLoad(req.body['bm:optionsYAML'])) {
      formErrors.push('Invalid YAML in optionsYAML.');
    }

    if (formErrors.length > 0) {
      return callback('form_invalid');
    }

    return callback();
  }

  function doInsert(callback) {
    conn.insertGraph({
      '@context': shared.context,
      '@id': req.body['@id'],
      '@type': 'bm:DriverInstance',
      'rdfs:label': req.body['rdfs:label'],
      'rdfs:comment': req.body['rdfs:comment'],
      'bm:driverName': req.body['bm:driverName'],
      'bm:optionsYAML': req.body['bm:optionsYAML'],
      'bm:enabled': req.body['bm:enabled'] ? true : false
    }, metaGraphIri, function(err, data) {
      if (err) {
        return callback(err);
      }

      fetchDriverInstances(callback);
    });
  }

  function render(err) {
    if (err) {
      var locals = _.extend({
        error: err,
        availableDrivers: availableDrivers
      }, req.body);

      if (err === 'form_invalid') {
        locals.formErrors = formErrors;
      }

      return res.render('data-manager/create-instance', locals);
    }

    return res.redirect('/data-manager/?success=true&createdInstance=' +
                        encodeURIComponent(req.body['@id']));
  }

  async.series([validateForm, doInsert], render);
}

function editInstance(req, res, next) {
  var id = req.params.id;
  var instanceData = {};

  function getInstance(callback) {
    if (!id) {
      return callback('not_found');
    }

    for (var i = 0; i < driverInstances.length; ++i) {
      var instance = driverInstances[i];
      if (instance['@id'] === id) {
        instanceData = instance;
        return callback();
      }
    }

    return callback('not_found');
  }

  function render(err) {
    var locals = _.extend({
      error: err,
      availableDrivers: availableDrivers
    }, instanceData);

    return res.render('data-manager/edit-instance', locals);
  }

  async.series([getInstance], render);
}

function submitEditInstance(req, res, next) {
  var id = req.params.id;
  var formErrors = [];

  function checkId(callback) {
    if (!id) {
      return callback('not_found');
    }

    for (var i = 0; i < driverInstances.length; ++i) {
      var instance = driverInstances[i];
      if (instance['@id'] === id) {
        return callback();
      }
    }

    return callback('not_found');
  }

  function validateForm(callback) {
    if (!req.body['@id']) {
      formErrors.push('No ID specified.');
    }
    if (!req.body['bm:driverName']) {
      formErrors.push('No driver specified.');
    }
    if (req.body['bm:optionsYAML'] &&
        !yaml.safeLoad(req.body['bm:optionsYAML'])) {
      formErrors.push('Invalid YAML in optionsYAML.');
    }

    if (formErrors.length > 0) {
      return callback('form_invalid');
    }

    return callback();
  }

  function doDelete(callback) {
    var query =
      'delete { graph <' + metaGraphIri + '> { <' + id + '> ?y ?z } } ' +
      'where { graph <' + metaGraphIri + '> { <' + id + '> ?y ?z } }';

    conn.execQuery(query, callback);
  }

  function doInsert(callback) {
    conn.insertGraph({
      '@context': shared.context,
      '@id': req.body['@id'],
      '@type': 'bm:DriverInstance',
      'rdfs:label': req.body['rdfs:label'],
      'rdfs:comment': req.body['rdfs:comment'],
      'bm:driverName': req.body['bm:driverName'],
      'bm:optionsYAML': req.body['bm:optionsYAML'],
      'bm:enabled': req.body['bm:enabled'] ? true : false
    }, instancesGraphUri, function(err, data) {
      if (err) {
        return callback(err);
      }

      fetchDriverInstances(callback);
    });
  }

  // This doesn't actually work yet for some reason.
  function doMove(callback) {
    if (id === req.body['@id']) {
      return callback();
    }

    var baseQuery =
      'delete { graph <%s> { ?x ?y ? z} } ' + 
      'insert { graph <%s> { ?x ?y ? z } } ' +
      'using <%s> where { ?x ?y ? z }';

    var moveQuery = util.format(baseQuery, id, req.body['@id'], id);

    conn.execQuery(moveQuery, callback);
  }

  function render(err) {
    if (err) {
      var locals = _.extend({
        error: err,
        availableDrivers: availableDrivers
      }, req.body);

      if (err === 'form_invalid') {
        locals.formErrors = formErrors;
      }

      return res.render('data-manager/edit-instance', locals);
    }

    return res.redirect('/data-manager/?success=true&editedInstance=' +
                        encodeURIComponent(req.body['@id']));
  }

  async.series([checkId, validateForm, doDelete, doInsert, doMove], render);
}

// TODO use reification
function submitDeleteInstance(req, res, next) {
  var id = req.params.id;

  function checkId(callback) {
    if (!id) {
      return callback('not_found');
    }

    for (var i = 0; i < driverInstances.length; ++i) {
      var instance = driverInstances[i];
      if (instance['@id'] === id) {
        return callback();
      }
    }

    return callback('not_found');
  }

  function doClear(callback) {
    clearInstanceData(id, callback);
  }

  function doDelete(callback) {
    var query =
      'delete { graph <' + metaGraphIri + '> { <' + id + '> ?y ?z } } ' +
      'where { graph <' + metaGraphIri + '> { <' + id + '> ?y ?z } }';

    conn.execQuery(query, function(err) {
      if (err) {
        return callback(err);
      }

      delete instanceObjects[id];
      callback();
    });
  }

  function render(err) {
    if (err) {
      return next(err);
    }

    return res.redirect('/data-manager/?success=true&deletedInstance=' +
                        encodeURIComponent(id));
  }

  async.series([checkId, doClear, doDelete], render);
}

// TODO use reification
function submitClearInstance(req, res, next) {
  var id = req.params.id;

  function checkId(callback) {
    if (!id) {
      return callback('not_found');
    }

    for (var i = 0; i < driverInstances.length; ++i) {
      var instance = driverInstances[i];
      if (instance['@id'] === id) {
        return callback();
      }
    }

    return callback('not_found');
  }

  function doClear(callback) {
    clearInstanceData(id, callback);
  }

  function render(err) {
    if (err) {
      return next(err);
    }

    return res.redirect('/data-manager/?success=true&deletedInstance=' +
                        encodeURIComponent(id));
  }

  async.series([checkId, doClear], render);
}

function submitFetchInstance(req, res, next) {
  var id = req.params.id;
  var theInstance;

  function checkId(callback) {
    if (!id) {
      return callback('not_found');
    }

    for (var i = 0; i < driverInstances.length; ++i) {
      var instance = driverInstances[i];
      if (instance['@id'] === id && instance.instance) {
        theInstance = instance;
        return callback();
      }
    }

    return callback('not_found');
  }

  function doFetch(callback) {
    if (theInstance.isFetching) {
      // The instance is currently fetching
      return callback();
    }

    theInstance.log('info', 'Fetching...');
    theInstance.instance.fetch();
    callback();
  }

  function render(err) {
    if (err) {
      return next(err);
    }

    return res.redirect('/data-manager/?success=true&fetchedInstance=' +
                        encodeURIComponent(id));
  }

  async.series([checkId, doFetch], render);
}

function submitFetchAllInstancesOfDriver(req, res, next) {
  var driverName = req.params.driverName;
  async.each(driverInstances, function(instance, callback) {
    if (instance['bm:driverName'] === driverName &&
        instance.instance && !instance.isFetching) {
      instance.isFetching = true;
      instance.log('info', 'Fetching...');
      instance.instance.fetch();
    }

    callback();
  }, function() {
    res.redirect('/data-manager/?success=true&fetchedDriver=' + driverName);
  });
}

// TODO: use authentication
router.use(init);
router.use(requireAuthentication);
router.get('/', index);
router.route('/instance/create')
  .get(createInstance)
  .post(submitCreateInstance);
router.get('/instance/view/:id', viewInstance);
router.route('/instance/edit/:id')
  .get(editInstance)
  .post(submitEditInstance);
router.post('/instance/delete/:id', submitDeleteInstance);
router.post('/instance/clear/:id', submitClearInstance);
router.post('/instance/fetch/:id', submitFetchInstance);
router.post('/driver/fetch/:driverName', submitFetchAllInstancesOfDriver);
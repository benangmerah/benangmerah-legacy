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
      instance.error('Query failed: ' + err);
      return callback(err);
    }

    var delta = _.now() - start;
    instance.log('info', 'Query completed in ' + delta + 'ms.');
    return callback();
  });
}, concurrency);

function DriverSparqlStream(options) {
  DriverSparqlStream.super_.call(this, { decodeStrings: false });
  this.instance = options.instance;
  this.graphUri = options.graphUri;
}

util.inherits(DriverSparqlStream, require('stream').Transform);

DriverSparqlStream.prototype.charCount = 0;
DriverSparqlStream.prototype.tripleBuffer = '';
DriverSparqlStream.prototype.fragmentBuffer = '';
DriverSparqlStream.prototype.queryCount = 0;
DriverSparqlStream.prototype.pendingQueryCount = 0;

DriverSparqlStream.prototype._transform = function(chunk, encoding, callback) {
  this.tripleBuffer += chunk;
  if (chunk === '.\n') {
    this.fragmentBuffer += this.tripleBuffer;
    this.charCount += this.tripleBuffer.length;
    this.tripleBuffer = '';

    if (this.charCount >= fragmentLength) {
      this.charCount = 0;
      this.commit();
      this.fragmentBuffer = '';
    }
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
  var fragment = self.fragmentBuffer;
  var baseQuery, query;

  if (self.graphUri) {
    baseQuery = 'INSERT DATA { GRAPH <%s> {\n%s} }\n';
    query = util.format(baseQuery, self.graphUri, fragment);
  }
  else {
    baseQuery = 'INSERT DATA {\n%s}\n';
    query = util.format(baseQuery, fragment);
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

    function initStreams() {
      sparqlStream = new DriverSparqlStream({
        instance: preparedInstance,
        graphUri: preparedInstance['@id']
      });
      tripleWriter = n3.Writer(sparqlStream);

      sparqlStream.on('end', onEnd);
    }
    function onEnd() {
      sparqlStream = undefined;
      tripleWriter = undefined;

      if (driverName.indexOf('meta-') === -1) {
        return;
      }

      preparedInstance.log('info',
        'Meta driver: refreshing driver instance cache...');
      fetchDriverInstances(function() {
        preparedInstance.log('finish', 'Idle.');
      });
    }

    initStreams();
    instance.on('addTriple', function(s, p, o) {
      if (!tripleWriter) {
        initStreams();
      }

      tripleWriter.addTriple(s, p, o);
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
           'WHERE { GRAPH ?g { ?x a bm:DriverInstance. ?x ?p ?o. } }',
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
    };

    return callback();
  });
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
    }, instancesGraphUri, function(err, data) {
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
    var baseQuery =
      'delete { graph ?g { <%s> ?y ?z } } where { graph ?g { <%s> ?y ?z } }';
    var query = util.format(baseQuery, id, id);

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
    var baseQuery = 'clear graph <%s>';
    var query = util.format(baseQuery, id);

    conn.execQuery(query, callback);
  }

  function doDelete(callback) {
    var baseQuery =
      'delete { graph ?g { <%s> ?y ?z } } where { graph ?g { <%s> ?y ?z } }';
    var query = util.format(baseQuery, id, id);

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
    var baseQuery = 'clear graph <%s>';
    var query = util.format(baseQuery, id);

    conn.execQuery(query, callback);
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
        theInstance = instance.instance;
        return callback();
      }
    }

    return callback('not_found');
  }

  function doFetch(callback) {
    theInstance.fetch();
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
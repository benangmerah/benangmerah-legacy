var util = require('util');

var async = require('async');
var conn = require('starmutt');
var express = require('express');
var logger = require('winston');
var n3 = require('n3');
var yaml = require('js-yaml');
var _ = require('lodash');

var shared = require('../shared');

var router = express.Router();
module.exports = router;

var initiated = false;
var INSTANCES_GRAPH_URI = 'tag:benangmerah.net:driver-instances';
var availableDrivers = [];
var driverInstances = [];
var driverDetails = {};

var sharedQueryQueue = async.queue(function(task, callback) {
  var query = task.query;
  var instance = task.instance;

  instance.info('Executing SPARQL query...');
  conn.execQuery(query, function(err) {
    if (err) {
      instance.error('Query failed: ' + err);
      return callback(err);
    }

    instance.info('Query successful');
    return callback();
  });
}, 1);

sharedQueryQueue.drain = function() {
  driverInstances.forEach(function(inst) {
    if (inst.lastLog.level === 'info') {
      var logObject = {
        level: 'finish',
        message: 'Idle.',
        timestamp: inst.lastLog.timestamp
      };
      inst.lastLog = logObject;
      inst.log.push(logObject);
    }
  });
  logger.info('Query queue has been drained.');
}

function instanceLog(instance, level, message, timestamp) {
  if (!timestamp) {
    timestamp = _.now();
  }

  var logObject = {
    level: level,
    message: message,
    timestamp: timestamp
  };

  instance.log.push(logObject);
  instance.lastLog = logObject;
  logger.log(level, instance['@id'] + ': ' + message);
}

function DriverSparqlStream(options, instance) {
  this.instance = instance;

  DriverSparqlStream.super_.call(this, { decodeStrings: false });
  if (options && options.graphUri) {
    this.graphUri = options.graphUri;
  }

  if (options && options.threshold) {
    this.threshold = options.threshold;
  }
  else {
    this.threshold = 104857;
  }
}

util.inherits(DriverSparqlStream, require('stream').Transform);

DriverSparqlStream.prototype.charCount = 0;
DriverSparqlStream.prototype.tripleBuffer = '';
DriverSparqlStream.prototype.fragmentBuffer = '';

DriverSparqlStream.prototype._transform = function(chunk, encoding, callback) {
  this.tripleBuffer += chunk;
  if (chunk === '.\n') {
    this.fragmentBuffer += this.tripleBuffer;
    this.charCount += this.tripleBuffer.length;
    this.tripleBuffer = '';

    if (this.charCount >= this.threshold) {
      this.charCount = 0;
      this.commit();
      this.fragmentBuffer = '';
    }
  }
  callback();
};

DriverSparqlStream.prototype._flush = function(callback) {
  this.commit();
  callback();
};

DriverSparqlStream.prototype.commit = function() {
  var fragment = this.fragmentBuffer;
  var baseQuery, query;

  if (this.graphUri) {
    baseQuery = 'INSERT DATA { GRAPH <%s> {\n%s} }\n';
    query = util.format(baseQuery, this.graphUri, fragment);
  }
  else {
    baseQuery = 'INSERT DATA {\n%s}\n';
    query = util.format(baseQuery, fragment);
  }
  sharedQueryQueue.push({ query: query, instance: this.instance });
};

function prepareInstance(rawDriverInstance) {
  var firstLogObject = {
    level: 'finish',
    message: 'Initialized.',
    timestamp: _.now()
  };

  var preparedInstance = _.extend(rawDriverInstance, {
    options: {},
    log: [firstLogObject],
    lastLog: firstLogObject
  });

  try {
    var optionsObject = yaml.safeLoad(preparedInstance['bm:optionsYAML']);
    preparedInstance.options = optionsObject;
  }
  catch (e) {
    preparedInstance['bm:enabled'] = false;
    preparedInstance.error = e;
  }

  if (preparedInstance['bm:enabled']) {
    var driverName = preparedInstance['bm:driverName'];
    if (!_.contains(availableDrivers, driverName)) {
      preparedInstance['bm:enabled'] = false;
    }
    else {
      try {
        var constructor = require(driverName);
        var instance = new constructor();
        preparedInstance.instance = instance;
        instance.setOptions(preparedInstance.options);

        var sparqlStream = new DriverSparqlStream({
          graphUri: preparedInstance['@id']
        }, instance);
        var n3writer = n3.Writer(sparqlStream);

        instance.on('addTriple', function(s, p, o) {
          n3writer.addTriple(s, p, o);
        });

        instance.on('log', function(level, message) {
          instanceLog(preparedInstance, level, message);
        });

        instance.on('finish', function() {
          n3writer.end();
          instanceLog(preparedInstance, 'info', 'Finished fetching.');
        });
      }
      catch (e) {
        preparedInstance['bm:enabled'] = false;
        preparedInstance.error = e;
      }
    }
  }

  return preparedInstance;
}

function initDataManager(callback, force) {
  if (initiated && !force) {
    return callback();
  }

  var dependencies = require('../package').dependencies;

  availableDrivers = [];
  Object.keys(dependencies).forEach(function(key) {
    if (/^benangmerah-driver-/.test(key)) {
      availableDrivers.push(key);
      driverDetails[key] = require(key + '/package');
    }
  });

  conn.getGraph({
    query: 'CONSTRUCT { ?x ?p ?o. } ' +
           'WHERE { GRAPH ?g { ?x a bm:DriverInstance. ?x ?p ?o. } }',
    form: 'compact',
    context: shared.context
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

    if (!_.isEmpty(graph)) {
      driverInstances = graph.map(prepareInstance);
    }

    initiated = true;

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
  initDataManager(next);
}

function index(req, res, next) {
  res.render('data-manager/index', {
    availableDrivers: availableDrivers,
    driverInstances: driverInstances,
    query: req.query,
    queryQueueLength: sharedQueryQueue.length()
  });
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
    }, INSTANCES_GRAPH_URI, function(err, data) {
      if (err) {
        return callback(err);
      }

      initDataManager(callback, true);
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

  async.series([initDataManager, getInstance], render);
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
    }, INSTANCES_GRAPH_URI, function(err, data) {
      if (err) {
        return callback(err);
      }

      initDataManager(callback, true);
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

      return res.render('data-manager/edit-instance', locals);
    }

    return res.redirect('/data-manager/?success=true&editedInstance=' +
                        encodeURIComponent(req.body['@id']));
  }

  async.series([checkId, validateForm, doDelete, doInsert], render);
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

  function doDelete(callback) {
    var baseQuery =
      'delete { graph ?g { <%s> ?y ?z } } where { graph ?g { <%s> ?y ?z } }';
    var query = util.format(baseQuery, id, id);

    conn.execQuery(query, callback);
  }

  function render(err) {
    if (err) {
      return next(err);
    }

    return res.redirect('/data-manager/?success=true&deletedInstance=' +
                        encodeURIComponent(id));
  }

  async.series([checkId, doDelete], render);
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
    theInstance.once('error', console.error);
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

function listDrivers(req, res, next) {
  res.send('List drivers');
}

function viewDriver(req, res, next) {
  res.send('View drivers');
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
router.get('/driver/view/:driverName', viewDriver);
router.get('/driver', listDrivers);
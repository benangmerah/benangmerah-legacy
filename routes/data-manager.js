var crypto = require('crypto');
var stream = require('stream');
var util = require('util');

var async = require('async');
var bodyParser = require('body-parser');
var config = require('config');
var conn = require('starmutt');
var express = require('express');
var logger = require('winston');
var n3 = require('n3');
var yaml = require('js-yaml');
var _ = require('lodash');

var shared = require('../shared');
var dataManager = require('../lib/data-manager');

var router = express.Router();
module.exports = router;

// TODO implement authentication
function requireAuthentication(req, res, next) {
  next();
}

function init(req, res, next) {
  res.locals.layout = 'layouts/data-manager';
  res.locals.availableDrivers = dataManager.availableDrivers;
  res.locals.driverDetails = dataManager.driverDetails;

  if (!req.initiated) {
    return dataManager.fetchDriverInstances(function() {
      req.initiated = true;
      next();
    });
  }

  next();
}

function validateInstanceForm(req, res, next) {
  var formErrors = [];

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
    res.locals.formErrors = formErrors;
    return next('form_invalid');
  }

  return next();
}

function bindInstance(req, res, next, instanceId) {
  console.log(instanceId);
  var instanceData = dataManager.getInstance(instanceId);
  if (!instanceData) {
    return next('not_found');
  }

  req.instance = instanceData;
  return next();
}

function index(req, res, next) {
  function render(err) {
    if (err) {
      res.locals.error = err;
    }

    res.locals.driverInstances = dataManager.driverInstances;
    res.locals.query = req.query;
    res.locals.queryQueueLength =
      dataManager.DriverSparqlStream.queue.length();

    res.render('data-manager/index');
  }

  dataManager.fetchDriverInstances(render);
}

function viewInstance(req, res, next) {
  _.assign(res.locals, req.instance);
  return res.render('data-manager/view-instance');
}

function createInstance(req, res, next) {
  res.locals.mode = 'create';
  res.render('data-manager/create-instance');
}

function submitCreateInstance(req, res, next) {
  function validateForm(callback) {
    validateInstanceForm(req, res, callback);
  }

  function doInsert(callback) {
    dataManager.createInstance(req.body, callback);
  }

  function render(err) {
    if (err) {
      _.assign(res.locals, req.body);
      res.locals.error = err;

      return res.render('data-manager/create-instance');
    }

    return res.redirect('/data-manager/?success=true&createdInstance=' +
                        encodeURIComponent(req.body['@id']));
  }

  async.series([validateForm, doInsert], render);
}

function editInstance(req, res, next) {
  _.assign(res.locals, req.instance);
  return res.render('data-manager/edit-instance');
}

function submitEditInstance(req, res, next) {
  function validateForm(callback) {
    validateInstanceForm(req, res, callback);
  }

  function doDelete(callback) {
    req.instance.deleteMeta(callback);
  }

  function doInsert(callback) {
    dataManager.createInstance(req.body, callback);
  }

  function render(err) {
    if (err) {
      _.assign(res.locals, req.body);
      res.locals.error = err;

      return res.render('data-manager/edit-instance');
    }

    return res.redirect('/data-manager/?success=true&editedInstance=' +
                        encodeURIComponent(req.body['@id']));
  }

  async.series([validateForm, doDelete, doInsert], render);
}

function submitDeleteInstance(req, res, next) {
  var id = req.params.instanceId;

  function render(err) {
    if (err) {
      return next(err);
    }

    return res.redirect('/data-manager/?success=true&deletedInstance=' +
                        encodeURIComponent(id));
  }

  req.instance.delete(render);
}

function submitClearInstance(req, res, next) {
  req.instance.clear();

  return res.redirect('/data-manager/?success=true&clearedInstance=' +
                      encodeURIComponent(req.params.instanceId));
}

function submitFetchInstance(req, res, next) {
  req.instance.fetch();

  return res.redirect('/data-manager/?success=true&fetchedInstance=' +
                      encodeURIComponent(req.params.instanceId));
}

function submitFetchAllInstancesOfDriver(req, res, next) {
  var driverName = req.params.driverName;
  dataManager.fetchAllInstancesOfDriver(driverName);
  res.redirect('/data-manager/?success=true&fetchedDriver=' + driverName);
}

// TODO: use authentication
router.use(bodyParser.urlencoded({ extended: true }));
router.use(init);
router.use(requireAuthentication);
router.get('/', index);
router.route('/instance/create')
  .get(createInstance)
  .post(submitCreateInstance);
router.post('/driver/fetch/:driverName', submitFetchAllInstancesOfDriver);

router.param('instanceId', bindInstance);
router.get('/instance/view/:instanceId', viewInstance);
router.route('/instance/edit/:instanceId')
  .get(editInstance)
  .post(submitEditInstance);
router.post('/instance/delete/:instanceId', submitDeleteInstance);
router.post('/instance/clear/:instanceId', submitClearInstance);
router.post('/instance/fetch/:instanceId', submitFetchInstance);
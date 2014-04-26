var util = require('util');
var express = require('express');
var url = require('url');
var async = require('async');
var conn = require('starmutt');

var ontologyDefinition = 'https://raw.githubusercontent.com/benangmerah/wilayah/master/ontology.ttl';
var redirectPlacesTo = 'https://raw.githubusercontent.com/benangmerah/wilayah/master/instances.ttl';

var router = express.Router();
module.exports = router;

function derefOntology(req, res, next) {
  res.redirect(303, ontologyDefinition);
}

function describeInternalResource(req, res, next) {
  var originalUrl = req.originalUrl;
  req.resourceURI = 'http://benangmerah.net' + originalUrl;
  console.log(req.resourceURI);
  req.url = req.resourceURI;
  next();
}

function describeResource(req, res, next) {
  if (!req.resourceURI) {
    next();
  }

  function execQuery(callback) {
    var query = util.format('select ?type where { <%s> a ?type }', req.resourceURI);
    conn.getColValues(query, callback);
  }

  function resourceType(type) {
    return this.indexOf(type) !== -1;
  }

  function render(err, col) {
    if (err) {
      return next(err);
    }

    res.json(col);
  }

  async.waterfall([execQuery], render);
}

router.use('/ontology', derefOntology);
router.use('/place', describeInternalResource);
router.use(describeResource);
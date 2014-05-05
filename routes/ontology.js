var util = require('util');
var express = require('express');
var url = require('url');
var async = require('async');
var conn = require('starmutt');
var cache = require('memory-cache');
var config = require('config');
var n3util = require('n3').Util;
var _ = require('lodash');

var shared = require('../shared');
var context = shared.context;

var lifetime = config.cachelifetime || 100;

var ontologyDefinition =
  'https://raw.githubusercontent.com/benangmerah/ontology/master/ontology.ttl';

var router = express.Router();
module.exports = router;

var forOntClass = shared.ontologyMiddleware;

function derefOntology(req, res, next) {
  res.redirect(303, ontologyDefinition);
}

function describeInternalResource(req, res, next) {
  var originalUrl = req.originalUrl;
  originalUrl = originalUrl.split('?')[0];
  req.resourceURI = 'http://benangmerah.net' + originalUrl;
  req.url = req.resourceURI;
  next();
}

function describeExternalResource(req, res, next) {
  req.resourceURI = req.params.resourceURI;
  next();
}

function describeProvinsi(req, res, next) {
  res.json('Hello provinsi');
}

function describeKota(req, res, next) {
  res.json('Hello kota');
}

function describePlace(req, res, next) {
  var vars = {};

  function execDescribeQuery(callback) {
    var describeQuery = util.format('describe <%s>', req.resourceURI);

    conn.getGraph({
      query: describeQuery,
      form: 'compact',
      context: shared.context
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      vars.thisPlace = data;
      return callback();
    });
  }

  function getParent(callback) {
    var parentQuery = util.format(
      'describe ?parent where { <%s> bm:hasParent ?parent }',
      req.resourceURI
    );

    conn.getGraph({
      query: parentQuery,
      form: 'compact',
      context: shared.context
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      if (data['@id']) {
        vars.parent = data;
      }

      return callback();
    });
  }

  function getChildren(callback) {
    var childrenQuery = util.format(
      'describe ?child where { ?child bm:hasParent <%s> }',
      req.resourceURI
    );

    conn.getGraph({
      query: childrenQuery,
      form: 'compact',
      context: shared.context
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      vars.children = data['@graph'];
      return callback();
    });
  }

  function getStats(callback) {
    var statsQuery = util.format(
      'select distinct ' +
      '?dataset ?datasetLabel ?measureLabel ?period ?measureValue { ' +
      '   ?o a qb:Observation. ' +
      '   ?o qb:dataSet ?dataset. ' +
      '   ?dataset rdfs:label ?datasetLabel. ' +
      '   ?o bm:refArea ?x. ' +
      '   { ?x owl:sameAs <%s>. } union { <%s> owl:sameAs ?x. } ' +
      '   ?o bm:refPeriod ?period. ' +
      '   ?o ?measure ?measureValue. ' +
      '   ?measure a qb:MeasureProperty. ' +
      '   ?measure rdfs:label ?measureLabel. ' +
      ' } order by asc(?measureLabel) asc(?period)',
       // TODO add language filter
       req.resourceURI, req.resourceURI);

    var start = _.now();
    conn.getResultsValues({ query: statsQuery, reasoning: 'QL' },
      function(err, data) {
              var end = _.now();
      console.log('Legacy query took %d msecs.', end - start);
        if (err) {
          return callback(err);
        }
        else {
          parseStats(data, callback);
        }
      });
  }

  function getDatacubes(callback) {
    var condition = util.format(
      '?observation bm:refArea ?x. { { ?x owl:sameAs <%s>. } ' +
      'union { <%s> owl:sameAs ?x. } }',
      req.resourceURI, req.resourceURI);

    shared.getDatacube(condition, ['bm:refArea'], function(err, datasets) {
      if (err) {
        return console.log(err);
      }

      vars.qbDatasets = datasets;
      callback();
    });
  }

  function parseStats(rows, callback) {
    var datasets = {};

    rows.forEach(function(row) {
      var dataset = datasets[row.dataset];
      if (!dataset) {
        dataset = datasets[row.dataset] = {};
        dataset.label = row.datasetLabel;
        dataset.data = {};
        dataset.periods = [];
      }

      if (dataset.periods.indexOf(row.period) === -1) {
        dataset.periods.push(row.period);
      }

      var measure = dataset.data[row.measureLabel];
      if (!measure) {
        measure = dataset.data[row.measureLabel] = {};
      }
      measure[row.period] = row.measureValue;
    });

    vars.datasets = datasets;

    callback();
  }

  function render(err) {
    console.log('Rendering..');
    if (err) {
      return next(err);
    }

    res.render('ontology/place', _.extend(vars, {
      title: shared.getPreferredLabel(vars.thisPlace)
    }));
  }

  if (req.query.legacy) {
    async.series([execDescribeQuery, getParent, getChildren, getStats], render);
  }
  else {
    async.series(
      [execDescribeQuery, getParent, getChildren, getDatacubes],
      render);
  }
}

function describeDataset(req, res, next) {
  function execDescribeQuery(callback) {
    var describeQuery = util.format('describe <%s>', req.resourceURI);

    conn.getGraph({
      query: describeQuery,
      form: 'compact',
      context: shared.context
    }, function(err, data) {
      callback(err, data);
    });
  }

  function render(err, data, datasets) {
    if (err) {
      return next(err);
    }

    var resource = _.extend({}, data);
    delete resource['@context'];

    var title = shared.getPreferredLabel(data);

    res.render('ontology/dataset', {
      title: title,
      resource: resource,
      datasets: datasets
    });
  }

  async.waterfall([execDescribeQuery], render);
}

function describeThing(req, res, next) {
  function execDescribeQuery(callback) {
    var describeQuery = util.format('describe <%s>', req.resourceURI);

    conn.getGraph({
      query: describeQuery,
      form: 'compact',
      context: shared.context
    }, function(err, data) {
      callback(err, data);
    });
  }

  function render(err, data) {
    if (err) {
      return next(err);
    }

    var resource = _.extend({}, data);
    delete resource['@context'];

    var title = shared.getPreferredLabel(data);

    res.render('ontology/thing', {
      title: title,
      resource: resource
    });
  }

  async.waterfall([execDescribeQuery], render);
}

function sameAsFallback(req, res, next) {
  if (!req.resourceURI) {
    return next();
  }

  var query = util.format(
    'select distinct ?twin ' +
    'where { { ?twin owl:sameAs <%s> } ' +
    'union { <%s> owl:sameAs ?twin } }', req.resourceURI);

  return conn.getColValues(query, function(err, col) {
    if (err) {
      return next(err);
    }
    if (col.length === 1) {
      return res.redirect(shared.getDescriptionPath(col[0]));
    }
    else {
      return next();
    }
  });
}

router.all('/ontology/*', derefOntology);
router.all('/place/*', describeInternalResource);
router.all('/resource/:resourceURI', describeExternalResource);

router.all('*', forOntClass('bm:Place'), describePlace);
router.all('*', forOntClass('qb:DataSet'), describeDataset);
router.all('*', forOntClass('owl:Thing'), describeThing);
router.all('*', forOntClass(), sameAsFallback);
var util = require('util');
var express = require('express');
var url = require('url');
var async = require('async');
var conn = require('starmutt');
var cache = require('memory-cache');
var config = require('config');
var _ = require('lodash');

var shared = require('../shared');
var context = shared.context;

var lifetime = config.cachelifetime || 100;

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
  req.url = req.resourceURI;
  ontologyRouter(req, res, next);
}

function describeExternalResource(req, res, next) {
  req.resourceURI = req.params.resourceURI;
  ontologyRouter(req, res, next);
}

var ontologyRoutes = [];
function ontologyRouter(req, res, next) {
  var uri = req.resourceURI;

  if (!uri) {
    next();
  }

  function resolveTypes(callback) {
    var query = util.format('select ?type where { <%s> a ?type }', uri);
    conn.getColValues({ query: query, reasoning: 'QL' }, function(err, resolvedTypes) {
      if (err) {
        callback(err);
      }
      else {
        cache.put('resolvedTypes:' + uri, resolvedTypes, lifetime);
        return callback(null, resolvedTypes);
      }
    });
  }

  function callMatchingRoute(err, resolvedTypes) {
    if (err) {
      return next(err);
    }

    var found = false;
    ontologyRoutes.forEach(function(route, idx) {
      if (found) {
        return;
      }

      var classURI = route.classURI;
      if (resolvedTypes.indexOf(classURI) !== -1) {
        console.log('Found: ' + classURI);
        cache.put('matchedOntologyRouteIndex:' + uri, idx, lifetime);
        ontologyRoutes[idx].callback(req, res, next);
        found = true;
        return;
      }
    });

    if (!found) {
      return next();
    }
  }

  var cachedRouteIndex = cache.get('matchedOntologyRouteIndex:' + uri);
  var cachedResolvedTypes = cache.get('resolvedTypes:' + uri);

  if (cachedRouteIndex) {
    console.log('Found in cache: ' + uri + ' a ' + ontologyRoutes[cachedRouteIndex].classURI);
    ontologyRoutes[cachedRouteIndex].callback(req, res, next);
    return;
  }
  else if (cachedResolvedTypes) {
    return callMatchingRoute(null, cachedResolvedTypes);
  }
  else {
    return resolveTypes(callMatchingRoute);
  }
}
ontologyRouter.route = function addOntologyRoute(classURI, callback) {
  ontologyRoutes.push({
    classURI: classURI,
    callback: callback
  });
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

      if (data['@id'])
        vars.parent = data;

      return callback();
    })
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
      'select distinct ?dataset ?datasetLabel ?measureLabel ?period ?measureValue { \
         ?o a qb:Observation. \
         ?o qb:dataSet ?dataset. \
         ?dataset rdfs:label ?datasetLabel. \
         ?o bm:refArea ?x. \
         { ?x owl:sameAs <%s>. } union { <%s> owl:sameAs ?x. } \
         ?o bm:refPeriod ?period. \
         ?o ?measure ?measureValue. \
         ?measure a qb:MeasureProperty. \
         ?measure rdfs:label ?measureLabel. \
       } order by asc(?measureLabel) asc(?period)',
       // TODO add language filter
       req.resourceURI, req.resourceURI);

    conn.getResultsValues({ query: statsQuery, reasoning: 'QL' },
      function(err, data) {
        if (err) {
          return callback(err);
        }
        else {
          parseStats(data, callback)
        }
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
    if (err) {
      return next(err);
    }

    res.render('ontology/place', _.extend(vars, {
      title: shared.getPreferredLabel(vars.thisPlace)
    }));
  }

  async.series([execDescribeQuery, getParent, getChildren, getStats], render);
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

  function render(err, data) {
    if (err) {
      return next(err)
    }

    var resource = _.extend({}, data);
    delete resource['@context'];

    var title = shared.getPreferredLabel(data);

    res.render('ontology/dataset', {
      title: title,
      resource: resource
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
      return next(err)
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

  var query = util.format('select distinct ?twin \
    where { { ?twin owl:sameAs <%s> } \
    union { <%s> owl:sameAs ?twin } } limit 2', req.resourceURI);

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

router.use('/ontology', derefOntology);
router.use('/place', describeInternalResource);
router.use('/resource/:resourceURI', describeExternalResource);

ontologyRouter.route(context.bm + 'Place', describePlace);
ontologyRouter.route(context.qb + 'DataSet', describeDataset);
ontologyRouter.route('http://www.w3.org/2002/07/owl#Thing', describeThing);

router.use('/resource', sameAsFallback);
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
  next();
}

function describeExternalResource(req, res, next) {
  req.resourceURI = req.params.resourceURI;
  next();
}

function ontology() {
  if (_.isEmpty(arguments)) {
    return function ontologyRequestOnly(req, res, next) {
      if (!req.resourceURI) {
        return next('route');
      }

      return next();
    }
  }

  var hits = []; // Array of resourceURIs that will match this route
  var typesToMatch = _.map(arguments, function(type) {
    if (n3util.isQName(type)) {
      return n3util.expandQName(type, shared.context);
    }

    return type;
  });

  return function handleOntologyRequest(req, res, next) {
    var uri = req.resourceURI;
    if (!uri) {
      return next('route');
    }
    if (hits.indexOf(uri) !== -1) {
      return next(); // In cache, carry on
    }

    function resolveTypes(callback) {
      var query = util.format('select ?type where { <%s> a ?type }', uri);
      conn.getColValues({ query: query, reasoning: 'QL' }, function(err, resolvedTypes) {
        if (err) {
          return next(err);
        }

        cache.put('resolvedTypes:' + uri, resolvedTypes, lifetime);
        return callback(resolvedTypes);
      });
    }

    function callMatchingRoute(resolvedTypes) {
      for (var i = 0; i < typesToMatch.length; i++) {
        var classURI = typesToMatch[i];
        if (resolvedTypes.indexOf(classURI) !== -1) {
          hits.push(uri);
          return next();
        }
      }

      // Not found, next route please
      return next('route');
    }

    var cachedResolvedTypes = cache.get('resolvedTypes:' + uri);
    if (cachedResolvedTypes) {
      return callMatchingRoute(cachedResolvedTypes);
    }
    else {
      return resolveTypes(callMatchingRoute);
    }
  }
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
    union { <%s> owl:sameAs ?twin } }', req.resourceURI);

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

router.all('*', ontology('bm:Place'), describePlace);
router.all('*', ontology('qb:DataSet'), describeDataset);
router.all('*', ontology('owl:Thing'), describeThing);
router.all('*', ontology(), sameAsFallback);
var util = require('util');
var express = require('express');
var url = require('url');
var async = require('async');
var conn = require('starmutt');
var cache = require('memory-cache');
var config = require('config');
var n3util = require('n3').Util;
var _ = require('lodash');
var _s = require('underscore.string');

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

  if (_s.endsWith(originalUrl, '/')) {
    return res.redirect(_s.rtrim(originalUrl, '/'));
  }

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
    var baseQuery = 'construct { <%s> ?p ?o } ' +
                    'where { graph ?g { <%s> ?p ?o } }';
    var describeQuery =
      util.format(baseQuery, req.resourceURI, req.resourceURI);

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
      'construct { ?parent ?x ?y } ' +
      'where { graph ?g { ' +
      '<%s> bm:hasParent ?parent. ?parent ?x ?y } }',
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
      'construct { ?child ?p ?o } ' +
      'where { graph ?g { ?child bm:hasParent <%s>. ?child ?p ?o. } }',
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

  function getDatacubes(callback) {
    // TODO union with other sameAs here
    var condition = util.format(
      'graph ?g { ?observation bm:refArea <%s> } ',
      vars.thisPlace['owl:sameAs']['@id']);

    shared.getDatacube(condition, ['bm:refArea'], function(err, datasets) {
      if (err) {
        return console.log(err);
      }

      vars.qbDatasets = datasets;

      callback();
    });
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

  async.series([
    function(callback) {
      async.parallel([execDescribeQuery, getParent, getChildren], callback);
    },
    getDatacubes
  ], render);
}

function describeDataset(req, res, next) {
  function execDescribeQuery(callback) {
    var baseQuery = 'construct { <%s> ?p ?o } ' +
                    'where { graph ?g { <%s> ?p ?o } }';
    var describeQuery =
      util.format(baseQuery, req.resourceURI, req.resourceURI);

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
    var baseQuery = 'construct { <%s> ?p ?o } ' +
                    'where { graph ?g { <%s> ?p ?o } }';
    var describeQuery =
      util.format(baseQuery, req.resourceURI, req.resourceURI);

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

function describeIndicator(req, res, next) {
  var selectedPeriod = req.query['bm:refPeriod'];
  var heatmapData = { max: 1, data: [] };
  var maxValue = 0;
  var rankings, periods, rankingsGraph;

  function execPeriodsQuery(callback) {
    var baseQuery =
      'select distinct ?year { graph ?g {' +
      '  [] a qb:Observation;' +
      '     <%s> [];' +
      '     bm:refPeriod ?year.' +
      '  } }' +
      'order by desc(?year)';

    var periodsQuery = util.format(baseQuery, req.resourceURI);

    conn.getCol(periodsQuery, function(err, col) {
      if (err) {
        return callback(err);
      }

      periods = _.pluck(col, 'value');

      if (!selectedPeriod || !_.contains(periods, selectedPeriod)) {
        selectedPeriod = periods[0];
      }

      callback();
    });
  }

  function execRankQuery(callback) {
    var baseQuery =
      'construct {' +
      '  ?x bm:value ?val;' +
      '    a qb:Observation;' +
      '    bm:refArea ?area.' +
      '  ?area rdfs:label ?o;' +
      '    geo:lat ?lat;' +
      '    geo:long ?long.' +
      '}' +
      'where {' +
      '  graph ?g {' +
      '    ?x a qb:Observation;' +
      '      bm:refPeriod "%s"^^xsd:gYear;' +
      '      <%s> ?val;' +
      '    bm:refArea ?areax.' +
      '  }' +
      '  graph ?h {' +
      '    ?area owl:sameAs ?areax.' +
      '    ?area rdfs:label ?o;' +
      '      geo:lat ?lat;' +
      '      geo:long ?long.' +
      '  }' +
      '  filter (lang(?o) = "") ' +
      '}' +
      'order by desc(?val)';
    var rankQuery = util.format(baseQuery, selectedPeriod, req.resourceURI);

    conn.getGraph({
      query: rankQuery,
      form: 'compact',
      context: shared.context,
      limit: 50000
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      rankingsGraph = shared.pointerizeGraph(data);

      rankings = [];
      _.forEach(rankingsGraph['@graph'], function(val) {
        if (val['@type'] !== 'qb:Observation') {
          return;
        }

        var idx = _.sortedIndex(rankings, val, function(v) {
          var sortValue;
          if (v['bm:value']['@type'] === 'xsd:decimal') {
            sortValue = parseFloat(v['bm:value']['@value']);
          }
          else {
            sortValue = shared.getLdValue(v['bm:value']);
          }

          if (sortValue > maxValue) {
            maxValue = sortValue;
          }

          return sortValue;
        });

        generateHeatmap(val);

        rankings.splice(idx, 0, val);
      });

      rankings.reverse();

      heatmapData.max = maxValue;

      callback();
    });
  }

  function generateHeatmap(observation) {
    var area = observation['bm:refArea'];
    var lat = area['geo:lat'];
    var lng = area['geo:long'];
    var value = parseFloat(shared.getLdValue(observation));
    heatmapData.data.push({
      lat: lat, lng: lng, value: value
    });
  }

  function execDescribeQuery(callback) {
    var baseQuery = 'construct { <%s> ?p ?o } ' +
                    'where { graph ?g { <%s> ?p ?o } }';
    var describeQuery =
      util.format(baseQuery, req.resourceURI, req.resourceURI);

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

    res.render('ontology/indicator', {
      title: title,
      resource: resource,
      rankings: rankings,
      periods: periods,
      selectedPeriod: selectedPeriod,
      heatmapJSON: JSON.stringify(heatmapData)
    });
  }

  async.waterfall([execPeriodsQuery, execRankQuery, execDescribeQuery], render);
}

function sameAsFallback(req, res, next) {
  if (!req.resourceURI) {
    return next();
  }

  var query = util.format(
    'select distinct ?twin ' +
    'where { graph ?g { { ?twin owl:sameAs <%s> } ' +
    'union { <%s> owl:sameAs ?twin } } }', req.resourceURI);

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
router.all('*', forOntClass('qb:MeasureProperty'), describeIndicator);
router.all('*', forOntClass('owl:Thing'), describeThing);
router.all('*', forOntClass(), sameAsFallback);
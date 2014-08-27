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
var Promise = require('bluebird');

var api = require('../lib/api');
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

function describePlace(req, res, next) {
  var id = req.resourceURI;

  var thisPlace, parent, children, datacubes;

  var describePromise = api.describe(id).then(function(data) {
    res.locals.thisPlace = data;
  });

  var parentPromise = api.parent(id).then(function(data) {
    if (data['@id']) {
      res.locals.parent = data;
    }
  });

  var childrenPromise = api.children(id).then(function(data) {
    res.locals.children = data;
  });

  var datacubesPromise = describePromise.then(function(data) {
    return api.datacubes({
      'bm:refArea': { '@id': res.locals.thisPlace['owl:sameAs']['@id'] }
    }).then(function(data) {
      res.locals.qbDatasets = data;
    });
  });

  Promise.all([
    describePromise, parentPromise, childrenPromise, datacubesPromise
  ]).then(function() {
    res.locals.title = shared.getPreferredLabel(res.locals.thisPlace);
    res.render('ontology/place');
  }).catch(next);
}

function describeDataset(req, res, next) {
  var describePromise = api.describe(req.resourceURI).then(function(resource) {
    delete resource['@context'];
    res.locals.resource = resource;
    res.locals.title = shared.getPreferredLabel(resource);
  });

  var measurePromise = describePromise.then(function() {
    var components = res.locals.resource['qb:component'];
    console.dir(components);
  });

  var publisherPromise = describePromise.then(function() {
    var publisher = res.locals.resource['dct:publisher'];
    return api.describe(publisher).then(function(data) {
      res.locals.resource['dct:publisher'] = data;
    });
  });

  var licensePromise = describePromise.then(function() {
    var license = res.locals.resource['dct:license'];
    return api.describe(license).then(function(data) {
      res.locals.resource['dct:license'] = data;
    });
  });

  var subjectPromise = describePromise.then(function() {
    var subject = res.locals.resource['dct:subject'];
    if (!_.isArray(subject)) {
      subject = [subject];
    }
    var subjectArray = [];
    var topics = [];
    var tags = [];
    var promise = Promise.resolve();
    _.forEach(subject, function(sub) {
      promise = promise.then(function() {
        return api.describe(sub).then(function(data) {
          var subj = data;
          if (shared.ldIsA(subj, 'bm:Topic')) {
            topics.push(subj);
          }
          else if (shared.ldIsA(subj, 'bm:Tag')) {
            tags.push(subj);
          }
        });
      });
    });

    promise = promise.then(function() {
      res.locals.topics = topics;
      res.locals.tags = tags;
    });

    return promise;
  });

  var otherDatasetsPromise = describePromise.then(function() {
    var pubId = res.locals.resource['dct:publisher']['@id'];
    return api.datasetsPublishedBy(pubId).then(function(datasets) {
      res.locals.resource['dct:publisher'].otherDatasets =
        _.filter(datasets, function(dataset) {
          return dataset['@id'] !== req.resourceURI;
        });
    });
  });

  var measuresQuery =
    'CONSTRUCT {' +
    '  ?x ?y ?z' +
    '} WHERE {' +
    '  <' + req.resourceURI + '> qb:structure [ qb:component [' +
    '    qb:measure ?x ] ].' +
    '  ?x ?y ?z. }';
  var measuresPromise = conn.getGraph({
    query: measuresQuery,
    form: 'compact',
    context: shared.context
  }).then(function(data) {
    if (data['@graph']) {
      res.locals.measures = data['@graph'];
    }
    else {
      delete data['@context'];
      res.locals.measures = [data];
    }
  });

  Promise.all(
    [describePromise, publisherPromise, licensePromise, subjectPromise,
     otherDatasetsPromise, measuresPromise]
  ).then(function() {
    res.render('ontology/dataset');
  }).catch(next);
}

function describeThing(req, res, next) {
  api.describe(req.resourceURI).then(function(resource) {
    delete resource['@context'];
    res.locals.resource = resource;
    res.locals.title = shared.getPreferredLabel(resource);
    res.render('ontology/thing');
  }).catch(next);
}

function describeIndicator(req, res, next) {
  var selectedPeriod = req.query['bm:refPeriod'];
  var heatmapData = { max: 1, data: [] };
  var rankings, periods, rankingsGraph;
  var resource;

  var describePromise = api.describe(req.resourceURI).then(function(data) {
    resource = data;
  });

  var periodsPromise = api.periods(req.resourceURI).then(function(data) {
    periods = data;
    if (!_.contains(periods, selectedPeriod)) {
      selectedPeriod = periods[0];
    }
  });

  var rankingsPromise = periodsPromise.then(function() {
    var conditions = {
      'bm:refPeriod': {
        '@value': selectedPeriod,
        '@type': 'xsd:gYear'
      }
    };

    return api.rankings({
      '@id': req.resourceURI,
      where: conditions
    });
  }).then(function(data) {
    rankings = data;

    // Generate heatmap data
    // Perhaps should be handled inside view, but handle that later
    _.forEach(rankings, function(observation) {
      var area = observation['bm:refArea'];
      var lat = area['geo:lat'];
      var lng = area['geo:long'];
      var value = parseFloat(shared.getLdValue(observation));
      heatmapData.data.push({
        lat: lat, lng: lng, value: value
      });

      if (value > heatmapData.max) {
        heatmapData.max = value;
      }
    });
  });

  Promise.all([describePromise, periodsPromise, rankingsPromise])
  .then(function() {
    delete resource['@context'];

    var title = shared.getPreferredLabel(resource);

    res.render('ontology/indicator', {
      title: title,
      resource: resource,
      rankings: rankings,
      periods: periods,
      selectedPeriod: selectedPeriod,
      heatmapJSON: JSON.stringify(heatmapData)
    });
  })
  .catch(next);
}

function sameAsFallback(req, res, next) {
  var sameAsPromise = api.sameAs(req.resourceURI);
  sameAsPromise.then(function(twins) {
    if (twins.length > 0) {
      res.redirect(shared.getDescriptionPath(twins[0]));
    }
    else {
      next();
    }
  }).catch(next);
}

router.all('/ontology/*', derefOntology);
router.all('/place/*', describeInternalResource);
router.all('/resource/:resourceURI', describeExternalResource);

router.all('*', forOntClass('bm:Place'), describePlace);
router.all('*', forOntClass('qb:DataSet'), describeDataset);
router.all('*', forOntClass('qb:MeasureProperty'), describeIndicator);
router.all('*', forOntClass('owl:Thing'), describeThing);
router.all('*', forOntClass(), sameAsFallback);
var util = require('util');
var querystring = require('querystring');
var express = require('express');
var url = require('url');
var async = require('async');
var conn = require('starmutt');
var cache = require('memory-cache');
var config = require('config');
var jsonld = require('jsonld');
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
  var topics = [];

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

      var datasetTitlesByHtmlId = {};
      _.forEach(data, function(dataset) {
        var htmlId = _s.slugify(dataset['@id'].substring(7));
        dataset.htmlId = htmlId;
        datasetTitlesByHtmlId[htmlId] =
          shared.getPreferredDatasetLabel(dataset);

        topics = _.union(topics, _.filter(dataset['dct:subject'],
          function(sub) {
            if (shared.ldIsA(sub, 'bm:Topic')) {
              if (!sub.datasets) {
                sub.datasets = [];
              }
              sub.datasets.push(dataset.htmlId);
              return true;
            }
          }));
      });

      res.locals.topicsJSON = {};
      _.forEach(topics, function(topic) {
        res.locals.topicsJSON[topic['@id']] = topic;
      });
      res.locals.topicsJSON = JSON.stringify(res.locals.topicsJSON);
      res.locals.datasetTitlesJSON = JSON.stringify(datasetTitlesByHtmlId);
    });
  });

  var iatiPromise = api.iatiActivities(id).then(function(data) {
    res.locals.iatiActivities = data['@graph'];
  });

  Promise.all([
    describePromise, parentPromise, childrenPromise,
    datacubesPromise, iatiPromise
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
  var heatmapData = { max: 1, data: [] };
  var rankings, dimensions, rankingsGraph,
      dataset, resource, otherMeasures;
  var dimensionObjects = [];
  var parents = [];
  var parentName;

  var params = req.query;
  var parentId = params['bm:hasParent'];

  var baseHrefObject = {};
  _.forEach(params, function(value, key) {
    if (!value) {
      return;
    }
    baseHrefObject[key] = shared.getLdValue(value);
  });

  var describePromise = api.describe(req.resourceURI).then(function(data) {
    delete data['@context'];
    resource = data;
  });

  var dimensionsPromise = api.siblingDimensions(req.resourceURI)
  .then(function(data) {
    dimensions = data;

    _.forEach(dimensions, function(dimension) {
      var id = dimension['@id'];

      dimension.values =
        _(dimension.values).sortBy(shared.getLdValue).reverse().value();

      if (id === 'bm:refPeriod') {
        dimension['rdfs:label'] = 'Perioda';
      }

      var selectedIndex = _.findIndex(dimension.values, function(value) {
        if (!params[id]) {
          return false;
        }

        return shared.getLdValue(value) === shared.getLdValue(params[id]);
      });
      if (!params[id] || selectedIndex === -1) {
        params[id] = dimension.values[0];
        dimension.selectedIndex = 0;
      }
      else {
        dimension.selectedIndex = selectedIndex;
      }
    });

    _.forEach(dimensions, function(dimension) {
      var id = dimension['@id'];
      _.forEach(dimension.values, function(value, key) {
        if (!_.isObject(value)) {
          value = { '@value': value };
          dimension.values[key] = value;
        }

        if (key === dimension.selectedIndex) {
          value.selected = true;
        }

        var hrefObject = _.clone(baseHrefObject);
        hrefObject[id] = shared.getLdValue(value);

        value.href =
          shared.getDescriptionPath(req.resourceURI) + '?' +
          querystring.stringify(hrefObject, params);
      });
    });
  });

  var rankingsPromise = dimensionsPromise.then(function() {
    if (_.isString(params['bm:refPeriod'])) {
      params['bm:refPeriod'] = {
        '@value': params['bm:refPeriod'],
        '@type': 'xsd:gYear'
      };
    }
    var conditions = _.clone(params);
    delete conditions['bm:hasParent'];

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

      var parent = area['bm:hasParent'];
      if (parent && !_.contains(parents, parent)) {
        parents.push(parent);
      }
    });

    _.forEach(parents, function(parent, key) {
      if (!parent) {
        delete parents[key];
        return;
      }
      var hrefObject = _.clone(baseHrefObject);
      hrefObject['bm:hasParent'] = parent['@id'];

      parent.href =
        shared.getDescriptionPath(req.resourceURI) + '?' +
        querystring.stringify(hrefObject);

      if (params['bm:hasParent'] === parent['@id']) {
        parent.selected = true;
      }
    });

    if (parentId) {
      rankings = _.filter(rankings, function(observation) {
        var area = observation['bm:refArea'];
        if (area['bm:hasParent']) {
          var matches = area['bm:hasParent']['@id'] === parentId;
          if (matches && !parentName) {
            parentName = shared.getPreferredLabel(area['bm:hasParent']);
          }

          return matches;
        }
      });
    }

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

  var datasetPromise = api.indicatorDataset(req.resourceURI)
  .then(function(data) {
    dataset = data;
  });

  var otherMeasuresPromise = datasetPromise.then(function() {
    var id = dataset['@id'];
    if (id) {
      return api.datasetMeasures(id).then(function(data) {
        otherMeasures = _.filter(data, function(obj) {
          return obj['@id'] !== req.resourceURI;
        });
      });
    }
  });

  var comparisonEnabled = false;
  var allObservations = [];
  var allAreas = {};
  var observationsByArea = {};
  var areaFullNameIndex = {};
  var periods = [];
  var comparisonPromise = dimensionsPromise.then(function() {
    if (_.findIndex(dimensions, function(d) {
      return (d['@id'] === 'bm:refPeriod');
    }) === -1) {
      return;
    }

    var periodDimensionIdx = _.findIndex(dimensions, function(d) {
      return d['@id'] === 'bm:refPeriod';
    });
    periods =
      _.map(dimensions[periodDimensionIdx].values, shared.getLdValue);

    comparisonEnabled = true;

    var allObservationsPromise = api.rankings({
      '@id': req.resourceURI,
      where: '?observation bm:refPeriod ?period.'
    }).then(function(data) {
      allObservations = data;

      _.forEach(allObservations, function(observation) {
        var area = observation['bm:refArea'];
        var parent = area['bm:hasParent'];
        var areaId = area['@id'];
        if (!allAreas[areaId]) {
          allAreas[areaId] = area;
        }

        var areaFullName = shared.getPreferredLabel(area);
        if (parent) {
          areaFullName += ', ' + shared.getPreferredLabel(parent);
        }
        if (!areaFullNameIndex[areaFullName]) {
          areaFullNameIndex[areaFullName] = areaId;
        }

        if (!observationsByArea[areaId]) {
          observationsByArea[areaId] = [];
        }

        observation[areaId] = observation['bm:value'];

        observationsByArea[areaId].push(observation);
      });
    });

    return allObservationsPromise;
  });

  Promise.all([
    describePromise, dimensionsPromise,
    rankingsPromise, datasetPromise,
    otherMeasuresPromise, comparisonPromise
  ]).then(function() {
    var title = shared.getPreferredLabel(resource);
    if (params['bm:refPeriod']) {
      var selectedPeriod = shared.getLdValue(params['bm:refPeriod']);
    }

    res.render('ontology/indicator', {
      title: title,
      resource: resource,
      rankings: rankings,
      dimensions: dimensions,
      dataset: dataset,
      parents: parents,
      parentName: parentName,
      comparisonEnabled: comparisonEnabled,
      observationsByAreaJSON: JSON.stringify(observationsByArea),
      areaFullNameIndexJSON: JSON.stringify(areaFullNameIndex),
      allAreas: JSON.stringify(allAreas),
      periodsJSON: JSON.stringify(periods),
      heatmapJSON: JSON.stringify(heatmapData)
    });
  })
  .catch(next);
}

function describeOrg(req, res, next) {
  api.describe(req.resourceURI).then(function(resource) {
    delete resource['@context'];
    res.locals.resource = resource;
    res.locals.title = shared.getPreferredLabel(resource);
    return api.datasetsPublishedBy(req.resourceURI).then(function(data) {
      res.locals.datasets = data;
    });
  }).then(function() {
    res.render('ontology/org');
  }).catch(next);
}

function describeSubject(req, res, next) {
  var id = req.resourceURI;

  var resource;
  var describePromise = api.describe(id).then(function(data) {
    resource = data;
  });

  var datasets;
  var datasetsPromise = api.describeAll({
    where: {
      '@type': 'qb:DataSet',
      'dct:subject': { '@id': id }
    }
  }).then(function(data) {
    datasets = data['@graph'];
  });

  Promise.all([describePromise, datasetsPromise])
  .then(function() {
    res.locals.resource = resource;
    res.locals.datasets = datasets;

    var typeLabel;
    if (shared.ldIsA(resource, 'bm:Topic')) {
      typeLabel = 'Topik';
    }
    else if (shared.ldIsA(resource, 'bm:Tag')) {
      typeLabel = 'Tagar';
    }
    res.locals.typeLabel = typeLabel;

    res.locals.title = shared.getPreferredLabel(resource);

    res.render('ontology/subject');
  }).catch(next);
}

function describeIatiActivity(req, res, next) {
  var activity;
  var describePromise = api.describeExtended(req.resourceURI, 3)
  .then(function(data) {
    delete data['@context'];
    activity = data;
  });

  Promise.all([
    describePromise
  ]).then(function() {
    res.locals.activity = activity;
    res.locals.title = shared.getPreferredLabel(activity);
    res.render('ontology/iati-activity');
  }).catch(next);
}

function describeIatiOrg(req, res, next) {
  var resource;
  var describePromise = api.describeExtended(req.resourceURI, 3)
  .then(function(data) {
    delete data['@context'];
    resource = data;
  });

  var reportedActivities = [];
  var participatedActivities = [];

  var activitiesPromise = describePromise.then(function() {
    var orgCode = resource['iati:organisation-code']['@id'];
    orgCode = shared.expandPrefixes(orgCode);

    var reportedPromise = api.describeAll({
      where:
        '?s a iati:activity. ?s iati:activity-reporting-org [' +
        'iati:organisation-code <' + orgCode + '> ].'
    }).then(function(data) {
      reportedActivities = data['@graph'];
    });

    var participatedPromise = conn.getGraph({
      query:
        'CONSTRUCT { ?s ?p ?o; iati:organisation-role ?role } WHERE {' +
        '?s a iati:activity; ?p ?o; iati:activity-participating-org [' +
        'iati:organisation-code <' + orgCode + '>; ' +
        'iati:organisation-role ?role ]. }',
      form: 'compact',
      context: shared.context
    }).then(function(data) {
      participatedActivities = data['@graph'] || data;
    });

    return Promise.all([reportedPromise, participatedPromise]);
  });

  Promise.all([
    describePromise, activitiesPromise
  ]).then(function() {
    res.locals.resource = resource;
    res.locals.title = shared.getPreferredLabel(resource);
    res.locals.reportedActivities = reportedActivities;
    res.locals.participatedActivities = participatedActivities;
    res.render('ontology/iati-organisation');
  }).catch(next);
}

function redirectIatiLocation(req, res, next) {
  var resource;
  var describePromise = api.describe(req.resourceURI).then(function(data) {
    delete data['@context'];
    resource = data;
  });

  var equiv;
  var seeAlsoPromise = describePromise.then(function() {
    var lat = resource['iati:latitude'];
    var lon = resource['iati:longitude'];
    var query =
      'SELECT ?x WHERE { ?x rdfs:seeAlso [ ' +
      'geo:lat "' + lat + '"; geo:long "' + lon + '"] }';

    return conn.getColValues(query).then(function(col) {
      equiv = col[0] || '';
    });
  });

  Promise.all([describePromise, seeAlsoPromise])
  .then(function() {
    if (equiv) {
      res.redirect(shared.getDescriptionPath(equiv));
    }
    else {
      next();
    }
  }).catch(next);
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

router.all('*', forOntClass('org:Organization'), describeOrg);
router.all('*', forOntClass('bm:Place'), describePlace);
router.all('*', forOntClass('qb:DataSet'), describeDataset);
router.all('*', forOntClass('qb:MeasureProperty'), describeIndicator);
router.all('*', forOntClass('bm:Topic'), describeSubject);
router.all('*', forOntClass('bm:Tag'), describeSubject);
router.all('*', forOntClass('iati:activity'), describeIatiActivity);
router.all('*', forOntClass('iati:organisation'), describeIatiOrg);
router.all('*', forOntClass('iati:location'), redirectIatiLocation);
router.all('*', forOntClass('owl:Thing'), describeThing);
router.all('*', forOntClass(), sameAsFallback);
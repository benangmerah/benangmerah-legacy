// Shared object

var util = require('util');
var cache = require('memory-cache');
var config = require('config');
var conn = require('starmutt');
var n3util = require('n3').Util;
var _ = require('lodash');
var traverse = require('traverse');
var jsonld = require('jsonld');
var async = require('async');

var cacheLifetime = config.cacheLifetime;

var shared = module.exports;

shared.rdfNS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
shared.rdfsNS = 'http://www.w3.org/2000/01/rdf-schema#';
shared.owlNS = 'http://www.w3.org/2002/07/owl#';
shared.xsdNS = 'http://www.w3.org/2001/XMLSchema#';
shared.geoNS = 'http://www.w3.org/2003/01/geo/wgs84_pos#';
shared.qbNS = 'http://purl.org/linked-data/cube#';
shared.bmNS = 'http://benangmerah.net/ontology/';

shared.context = shared.prefixes = {
  'rdf': shared.rdfNS,
  'rdfs': shared.rdfsNS,
  'owl': shared.owlNS,
  'xsd': shared.xsdNS,
  'geo': shared.geoNS,
  'qb': shared.qbNS,
  'bm': shared.bmNS
};

shared.getInferredTypes = function(uri, callback) {
  // Search cache
  var cacheEntry = cache.get('resolvedTypes:' + uri);
  if (cacheEntry) {
    return callback(null, cacheEntry);
  }

  var query = util.format('select ?type where { <%s> a ?type }', uri);
  conn.getColValues({ query: query, reasoning: 'QL' }, function(err, resolvedTypes) {
    if (err) {
      return callback(err);
    }

    cache.put('resolvedTypes:' + uri, resolvedTypes, cacheLifetime);
    return callback(null, resolvedTypes);
  });
};

shared.handleOntologyRequest = function(req, res, next) {
  var hitCache = this.hitCache;
  var typesToMatch = this.typesToMatch;

  var uri = req.resourceURI;
  if (!uri) {
    return next('route');
  }
  if (hitCache.indexOf(uri) !== -1) {
    return next(); // In cache, carry on
  }

  return shared.getInferredTypes(uri, function(err, resolvedTypes) {
    if (err) {
      return next(err);
    }

    // Search for a match
    for (var i = 0; i < typesToMatch.length; i++) {
      var classURI = typesToMatch[i];
      if (resolvedTypes.indexOf(classURI) !== -1) {
        hitCache.push(uri);
        return next();
      }
    }

    // Not found, next route please
    return next('route');
  });
};

shared.ontologyMiddleware = function() {
  var hitCache = [];
  var ontologyRouterCaches = cache.get('ontologyRouterCaches');
  if (!ontologyRouterCaches) {
    cache.put('ontologyRouterCaches', [hitCache]);
  }
  else {
    ontologyRouterCaches.push(hitCache);
    cache.put('ontologyRouterCaches', ontologyRouterCaches);
  }

  var typesToMatch = _.map(arguments, function(type) {
    if (n3util.isQName(type)) {
      return n3util.expandQName(type, shared.context);
    }

    return type;
  });

  return shared.handleOntologyRequest.bind({
    hitCache: hitCache,
    typesToMatch: typesToMatch
  });
};

shared.getLdValue = function(ldObj, altAttr) {
  if (typeof ldObj == 'string') {
    return ldObj;
  }

  if (ldObj['@value']) {
    return ldObj['@value'];
  }

  if (altAttr === true && ldObj['@id']) {
    return ldObj['@id'];
  }

  return undefined;
};

shared.getDescriptionPath = function(resourceURI) {
  if (!resourceURI) {
    return '';
  }
  if (resourceURI.indexOf('http://benangmerah.net') === 0) {
    return resourceURI.substring('http://benangmerah.net'.length);
  }
  else {
    return '/resource/' + encodeURIComponent(resourceURI);
  }
};

shared.getPreferredLabel = function(jsonLdResource) {
  var labels;
  if (jsonLdResource['rdfs:label']) {
    labels = jsonLdResource['rdfs:label'];
  }
  else if (jsonLdResource[shared.rdfsNS + 'label']) {
    labels = jsonLdResource[shared.rdfsNS + 'label'];
  }
  else if (jsonLdResource['@id']) {
    return shared.getPropertyName(jsonLdResource['@id']);
  }
  else {
    return '';
  }

  if (typeof labels == 'string') {
    return labels;
  }

  if (!(labels instanceof Array)) {
    console.log(labels);
    return shared.getLdValue(labels);
  }

  var preferredLabel = '';

  labels.forEach(function(label) {
    if (label['@lang']) {
      // locale support
      // if label['@lang'] != locale && preferredLabel then return;
    }

    var labelValue = shared.getLdValue(label);
    if (labelValue.length > preferredLabel.length) {
      preferredLabel = labelValue;
    }
  });

  return preferredLabel;
};

shared.getPropertyName = function(propertyName) {
  var delimiters = ['#', '/', ':'];

  for (var i = 0; i < delimiters.length; ++i) {
    var delimiter = delimiters[i];
    var index = propertyName.lastIndexOf(delimiter);
    if (index !== -1) {
      return propertyName.substring(index + 1);
    }
  }
};

shared.ldIsA = function(ldObj, typeToMatch) {
  return _.contains(ldObj['@type'], typeToMatch);
};

// This function below is quite useful!
shared.pointerizeGraph = function(graph) {
  var resourceMap = {};

  traverse(graph).forEach(function(resource) {
    var id = resource['@id'];
    if (id && _.size(resource) > _.size(resourceMap[id])) {
      resourceMap[id] = resource;
    }
  });

  traverse(graph).forEach(function(resource) {
    var self = this;
    var id = resource['@id'];
    if (id && resourceMap[id] && resource !== resourceMap[id]) {
      self.delete();
      self.update(resourceMap[id], true);
    }
  });

  return graph;
};

// Return: JSON-LD datasets according to conditions
shared.getDatacube = function(conditions, fixedProperties, callback) {
  var automaticFilter = false;
  if (!callback) {
    callback = fixedProperties;
    fixedProperties = [];
    automaticFilter = true;
  }
  else if (_.isString(fixedProperties)) {
    fixedProperties = [fixedProperties];
  }

  var conditionsString = "";
  var graph = {};
  var datasets = [];
  var dsds = {};
  var dimensionProperties = {};
  var measureProperties = {};
  var observations = [];

  function generateConditionsString(callback) {
    if (_.isString(conditions)) {
      conditionsString = conditions;
      return callback();
    }

    if (automaticFilter) {
      fixedProperties = _.keys(conditions);
    }

    // get conditions.observation and conditions.dataset
    var context = _.extend({ dataset: "qb:dataSet" }, shared.context);
    conditions['@context'] = _.extend(context, conditions['@context']);
    conditions['@id'] = 'tag:sparql-param:?observation';
    jsonld.normalize(conditions, {format:'application/nquads'},
      function(err, string) {
        if (err) {
          return callback(err);
        }
        string = string.replace(/<tag:sparql-param:\?observation>/g, '?observation');
        conditionsString = string;
        callback();
      });
  }

  function execDatacubeQuery(callback) {
    var query = util.format(
      'construct { ' +
      '  ?dataset ?datasetP ?datasetO. ' +
      '  ?dataset qb:structure ?dsd. ' +
      '  ?dsd qb:component ?component. ' +
      '  ?component ?componentP ?componentO. ' +
      '  ?dsd a qb:DataStructureDefinition. ' +
      '  ?observation qb:dataSet ?dataset. ' +
      '  ?observation ?observationP ?observationO. ' +
      '  ?observation a qb:Observation. ' +
      '  ?property a ?propertyType. ' +
      '  ?property rdfs:label ?propertyLabel. ' +
      '} where { ' +
      '  ?dataset a qb:DataSet. ' +
      '  ?dataset ?datasetP ?datasetO. ' +
      '  ?observation qb:dataSet ?dataset. ' +
      '  ?dsd a qb:DataStructureDefinition. ' +
      '  ?dataset qb:structure ?dsd. ' +
      '  ?dsd qb:component ?component. ' +
      '  ?component ?componentP ?componentO. ' +
      '  { { ?component qb:dimension ?property } union { ?component qb:measure ?property } }. ' +
      '  ?component qb:order ?order. ' +
      '  ?property a ?propertyType. ' +
      '  ?property rdfs:label ?propertyLabel. ' +
      '  ?observation a qb:Observation. ' +
      '  ?observation qb:dataSet ?dataset. ' +
      '  ?observation ?observationP ?observationO. ' +
      '  { {?observationP a qb:DimensionProperty} union {?observationP a qb:MeasureProperty} }. ' +
      '  %s ' + // Conditions
      '} ', conditionsString);

    // console.log(query);
    var start = _.now();
    conn.getGraph({ query: query, context: shared.context, form: 'compact' }, function(err, data) {
      var end = _.now();
      console.log('Query took %d msecs.', end - start);
      if (err) {
        return callback(err);
      }

      shared.pointerizeGraph(data);
      graph = data['@graph'];

      // console.log(graph);

      if (!graph) {
        // console.log('Graph is empty');
        return callback('empty_graph');
      }

      return callback();
    });
  }

  function siftGraph(callback) {
    // console.log('Sifting graph...');
    var isA = shared.ldIsA;

    graph.forEach(function(resource) {
      var id = resource['@id'];
      var type = resource['@type'];
      // console.log('%s a %s', resource['@id'], type);
      // console.log('fixed = %j', _.contains(fixedProperties, id));
      if (isA(resource, 'qb:DataSet')) {
        datasets.push(_.clone(resource));
      }
      else if (isA(resource, 'qb:DataStructureDefinition')) {
        dsds[id] = resource;
      }
      else if (isA(resource, 'qb:DimensionProperty')) {
        dimensionProperties[id] = resource;
      }
      else if (isA(resource, 'qb:MeasureProperty')) {
        measureProperties[id] = resource;
      }
      else if (isA(resource, 'qb:Observation')) {
        observations.push(resource);
      }
    });

    async.map(datasets, processDataset, callback);
  }

  function processDataset(dataset, callback) {
    // console.log('Processing dataset...');
    // Generate dataset.dimensions, dataset.measures
    
    // An ordered array of the dataset's dimensions, according to its DSD
    dataset.dimensions = [];

    // An ordered array of the dataset's measures, according to its DSD
    dataset.measures = [];

    var dsd = dataset['qb:structure'];

    var components = dsd['qb:component'];
    if (_.isArray(components)) {
      components = _.sortBy(dsd['qb:component'], function(component) {
        return shared.getLdValue(component['qb:order']);
      });
    }
    else {
      components = [components];
    }

    components.forEach(function(component) {
      var id;
      if (component['qb:dimension']) {
        id = component['qb:dimension']['@id'];
        if (_.contains(fixedProperties, id)) {
          return;
        }
        // Clone because we are adding some properties that are not relevant
        // outside of the datacube
        dataset.dimensions.push(_.clone(component['qb:dimension']));
      }
      else if (component['qb:measure']) {
        id = component['qb:measure']['@id'];
        if (_.contains(fixedProperties, id)) {
          return;
        }
        // Clone because we are adding some properties that are not relevant
        // outside of the datacube
        dataset.measures.push(_.clone(component['qb:measure']));
      }
    });

    dataset.observations = _.filter(observations, function(observation) {
      return observation['qb:dataSet']['@id'] === dataset['@id'];
    });

    // Generate dataset.datacube
    // dataset.datacube is an n-dimensional array
    // datacube[dimension1][dimension2][..dimensionN]
    //    [measure] = [measureValue]

    dataset.datacube = {};
    var nDimensions = dataset.dimensions.length;

    dataset.observations.forEach(function(observation) {
      var cursor = dataset.datacube;
      var path = [];
      dataset.dimensions.forEach(function(dimension, idx) {
        var dimensionId = dimension['@id'];
        // console.log('Walking: %s', dimensionId);
        var obsDimValue = observation[dimensionId];
        var nextIndex = '';
        if (_.isEmpty(obsDimValue)) {
          return;
        }

        if (_.isUndefined(dimension.values)) {
          dimension.values = [];
        }
        if (!_.contains(dimension.values, obsDimValue)) {
          dimension.values.push(obsDimValue);
        }

        nextIndex = shared.getLdValue(obsDimValue, true);
        path.push(nextIndex);

        if (_.isUndefined(cursor[nextIndex])) {
          cursor[nextIndex] = {};
        }
        cursor = cursor[nextIndex];
      });

      // console.log(path.join(' / '));
      dataset.measures.forEach(function(measure) {
        var measureId = measure['@id'];
        cursor[measureId] = observation[measureId];
        // console.log('%s: %s', measureId, observation[measureId]);
      });
    });

    callback(null, dataset);
  }

  // console.log('Datacube starting...');
  async.series([generateConditionsString, execDatacubeQuery, siftGraph],
    function(err) {
      // console.log('Datacube finished.');
      if (err === 'empty_graph') {
        return callback(null, []);
      }
      if (err) {
        return callback(err);
      }
      return callback(null, datasets);
    });
};
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
var naturalSort = require('javascript-natural-sort');

var cacheLifetime = config.cacheLifetime;

var shared = module.exports;

shared.RDF_NS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
shared.RDFS_NS = 'http://www.w3.org/2000/01/rdf-schema#';
shared.OWL_NS = 'http://www.w3.org/2002/07/owl#';
shared.XSD_NS = 'http://www.w3.org/2001/XMLSchema#';
shared.GEO_NS = 'http://www.w3.org/2003/01/geo/wgs84_pos#';
shared.QB_NS = 'http://purl.org/linked-data/cube#';
shared.BM_NS = 'http://benangmerah.net/ontology/';
shared.DCT_NS = 'http://purl.org/dc/terms/';

shared.context = shared.prefixes = {
  'rdf': shared.RDF_NS,
  'rdfs': shared.RDFS_NS,
  'owl': shared.OWL_NS,
  'xsd': shared.XSD_NS,
  'geo': shared.GEO_NS,
  'qb': shared.QB_NS,
  'bm': shared.BM_NS,
  'dct': shared.DCT_NS
};

shared.getInferredTypes = function(uri, callback) {
  // Search cache
  var cacheEntry = cache.get('resolvedTypes:' + uri);
  if (cacheEntry) {
    return callback(null, cacheEntry);
  }

  var query =
    util.format('select ?type where { graph ?g { <%s> a ?type } }', uri);
  conn.getColValues(
    { query: query, reasoning: 'QL' }, function(err, resolvedTypes) {
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
  else if (jsonLdResource[shared.RDFS_NS + 'label']) {
    labels = jsonLdResource[shared.RDFS_NS + 'label'];
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

  var conditionsString = '';
  var allGraph = [];
  var datasetIds = [];
  var propertyIds = [];
  var dsdIds = [];
  var observationsGraph = [];
  var datasetsGraph = [];
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
    var context = _.extend({ dataset: 'qb:dataSet' }, shared.context);
    conditions['@context'] = _.extend(context, conditions['@context']);
    conditions['@id'] = 'tag:sparql-param:?observation';
    jsonld.normalize(conditions, {format:'application/nquads'},
      function(err, string) {
        if (err) {
          return callback(err);
        }
        string = string.replace(/<tag:sparql-param:\?observation>/g, 
                                '?observation');
        conditionsString = 'graph ?g { ' + string + ' } ';
        callback();
      });
  }

  function getObservations(callback) {
    var query = 'construct { ?observation ?p ?o } ' +
                'where { graph ?g { ' +
                '?observation a qb:Observation. ' +
                '?observation ?p ?o. } ' +
                conditionsString + ' }';

    var start = _.now();
    conn.getGraph(query, function(err, graph) {
      if (err) {
        return callback(err);
      }

      if (graph.length === 0) {
        return callback('empty_graph');
      }

      allGraph = _.union(allGraph, graph);

      graph.forEach(function(subgraph) {
        var dataset = subgraph[shared.context.qb + 'dataSet'];
        var ids = _.pluck(dataset, '@id');
        datasetIds = _.union(datasetIds, ids);

        var properties = _.keys(subgraph);
        properties = _.filter(properties, function(prop) {
          return prop.indexOf(shared.context.qb) === -1;
        });
        propertyIds = _.union(propertyIds, properties);
      });

      callback();
    });
  }

  function getProperties(callback) {
    async.mapSeries(propertyIds, function(propertyId, callback) {
      var baseQuery = 'construct { <%s> ?p ?o. } ' +
                      'where { graph ?g { <%s> ?p ?o. } }';

      var query = util.format(baseQuery, propertyId, propertyId);
      
      conn.getGraph(query, function(err, graph) {
        if (err) {
          return callback(err);
        }

        allGraph = _.union(allGraph, graph);
        graph.forEach(function(subgraph) {
          var structure = subgraph[shared.context.qb + 'structure'];
          var ids = _.pluck(structure, '@id');
          dsdIds = _.union(dsdIds, ids);
        });

        return callback(null);
      });
    }, function(err) {
      if (err) {
        return callback(err);
      }

      return callback();
    });
  }

  function getDatasets(callback) {
    async.mapSeries(datasetIds, function(datasetId, callback) {
      var baseQuery = 'construct { <%s> ?p ?o. } ' +
                      'where { graph ?g { <%s> ?p ?o. } }';

      var query = util.format(baseQuery, datasetId, datasetId);
      
      conn.getGraph(query, function(err, graph) {
        if (err) {
          return callback(err);
        }

        allGraph = _.union(allGraph, graph);
        graph.forEach(function(subgraph) {
          var structure = subgraph[shared.context.qb + 'structure'];
          var ids = _.pluck(structure, '@id');
          dsdIds = _.union(dsdIds, ids);
        });

        return callback(null);
      });
    }, function(err) {
      if (err) {
        return callback(err);
      }

      return callback();
    });
  }

  function getDsds(callback) {
    async.mapSeries(dsdIds, function(dsdId, callback) {
      var baseQuery =
        'construct { <%s> ?p ?o. <%s> qb:component ?c. ?c ?cP ?cO. } ' +
        'where { graph ?g { <%s> ?p ?o. <%s> qb:component ?c. ?c ?cP ?cO. } }';

      var query = util.format(baseQuery, dsdId, dsdId, dsdId, dsdId);
      
      // console.log(query);
      conn.getGraph(query, function(err, graph) {
        // console.log(graph);
        if (err) {
          return callback(err);
        }

        allGraph = _.union(allGraph, graph);

        return callback(null);
      });
    }, function(err) {
      if (err) {
        return callback(err);
      }

      return callback();
    });
  }

  function compactGraph(callback) {
    jsonld.compact(allGraph, shared.context, function(err, compacted) {
      shared.pointerizeGraph(compacted);
      allGraph = compacted;

      callback();
    });
  }

  function siftGraph(callback) {
    // console.log('Sifting graph...');
    var isA = shared.ldIsA;

    var graph = allGraph['@graph'];
    if (!graph) {
      return callback('empty_graph');
    }

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

    async.mapSeries(datasets, processDataset, function(err, results) {
      if (err) {
        return callback(err);
      }

      datasets = _.compact(results);
      callback();
    });
  }

  function processDataset(dataset, callback) {
    // console.log('Processing dataset...');
    // Generate dataset.dimensions, dataset.measures
    
    // An ordered array of the dataset's dimensions, according to its DSD
    dataset.dimensions = [];

    // An ordered array of the dataset's measures, according to its DSD
    dataset.measures = [];

    var dsd = dataset['qb:structure'];
    if (!dsd) {
      return callback(null, undefined);
    }

    var components = dsd['qb:component'];
    if (!components) {
      return callback(null, undefined);
    }

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
        var obsDimValueLiteral = shared.getLdValue(obsDimValue);
        var nextIndex = '';
        if (_.isEmpty(obsDimValue)) {
          return;
        }

        if (_.isUndefined(dimension.values)) {
          dimension.values = [];
          dimension.literalValues = [];
        }

        if (!_.contains(dimension.literalValues, obsDimValueLiteral)) {
          dimension.values.push(obsDimValue);
          dimension.literalValues.push(obsDimValueLiteral);
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

    dataset.dimensions.forEach(function(dimension) {
      dimension.values = dimension.values.sort(function(a, b) {
        var valA = shared.getLdValue(a);
        var valB = shared.getLdValue(b);
        return naturalSort(valA, valB);
      });
    });

    callback(null, dataset);
  }

  // console.log('Datacube starting...');
  async.series(
    [generateConditionsString, getObservations, getProperties,
     getDatasets, getDsds, compactGraph, siftGraph],
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

shared.addRank = function(items, sortKey) {
  var previousValue, lastRank;
  var index = 1;

  _.forEach(items, function(item) {
    var rank, sameAsPrevious;
    var sortValue = shared.getLdValue(item[sortKey]);

    if (previousValue === sortValue) {
      rank = lastRank;
      sameAsPrevious = true;
    }
    else {
      rank = index;
      sameAsPrevious = false;
    }

    item.rank = rank;
    item.sameAsPrevious = sameAsPrevious;

    ++index;
  })
};
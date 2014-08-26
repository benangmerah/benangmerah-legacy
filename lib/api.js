// BenangMerah API
// Designed to be accessible from within Node and from the client, via Express
// All methods from API should accept a params object as its only argument
// and return a Promise.
// Client-based requests will receive a JSON object based on the Promise results
// Goal: DRY
var _ = require('lodash');
var conn = require('starmutt');
var jsonld = require('jsonld');
var Promise = require('bluebird');
var n3util = require('n3').Util;
var naturalSort = require('javascript-natural-sort');

var shared = require('./shared');

var api = {};
module.exports = api;

function getId(params) {
  return shared.sanitizeUri(
    _.isString(params) ? params :
    params['@id'] || params.id);
}

api.describe = function(params) {
  var id = getId(params);
  var raw = arguments[1] || params.raw;

  var describeQuery =
    'CONSTRUCT { <' + id + '> ?p ?o } ' +
    'WHERE { <' + id + '> ?p ?o }';

  var queryOptions = { query: describeQuery };
  if (!raw) {
    queryOptions.form = 'compact';
    queryOptions.context = shared.context;
  }

  return conn.getGraph(queryOptions);
};

api.parent = function(params) {
  var id = getId(params);

  var parentQuery =
    'CONSTRUCT { ?parent ?p ?o } ' +
    'WHERE { <' + id + '> bm:hasParent ?parent. ' +
    '?parent ?p ?o }';

  return new Promise(function(resolve, reject) {
    conn.getGraph({
      query: parentQuery,
      form: 'compact',
      context: shared.context
    }, function(err, data) {
      if (err) {
        return reject(err);
      }

      resolve(data);
    });
  });
};

api.children = function(params) {
  var id = getId(params);

  var childrenQuery =
    'CONSTRUCT { ?child ?p ?o } ' +
    'WHERE { ?child bm:hasParent <' + id + '>. ' +
    '?child ?p ?o }';

  return new Promise(function(resolve, reject) {
    conn.getGraph({
      query: childrenQuery,
      form: 'compact',
      context: shared.context
    }, function(err, data) {
      if (err) {
        return reject(err);
      }

      resolve(data['@graph']);
    });
  });
};

api.datacubes = function(params) {
  console.log('Fetching datacubes...');
  var conditions = params;
  var fixedProperties = [];
  if (params.fixedProperties) {
    if (_.isString(params.fixedProperties)) {
      fixedProperties = [params.fixedProperties];
    }
    else {
      fixedProperties = params.fixedProperties;
    }
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

  var jsonldPromises = jsonld.promises();

  // Generate conditions string
  console.log('Generating observations query...');
  conditionsString =
    shared.generateConditionsString(conditions, 'observation');

  // conditionsString now ready to be used.

  // Fetch observations
  var observationsQuery = 
    'construct { ?observation ?p ?o } ' +
    'where { ' +
    '?observation a qb:Observation. ' +
    '?observation ?p ?o. ' +
    conditionsString + ' }';

  console.log('observations query: ' + observationsQuery);

  var mergeGraph = function(graph) {
    allGraph = _.union(allGraph, graph);
    return graph;
  };

  // Fetch observations
  console.log('Fetching observations...');
  var promise = conn.getGraph(observationsQuery).then(function(graph) {
    console.log('Fetched observations.');
    if (graph.length === 0) {
      return Promise.reject(new Error('empty_graph'));
    }

    mergeGraph(graph);

    _.forEach(graph, function(subgraph) {
      var dataset = subgraph[shared.context.qb + 'dataSet'];
      var ids = _.pluck(dataset, '@id');
      datasetIds = _.union(datasetIds, ids);

      var properties = _.keys(subgraph);
      properties = _.filter(properties, function(prop) {
        return prop.indexOf(shared.context.qb) === -1;
      });
      propertyIds = _.union(propertyIds, properties);
    });

    // Fetch properties
    var propPromises = Promise.all(_.map(propertyIds, function(propertyId) {
      console.log('Mapping property: ' + propertyId);
      return api.describe(propertyId, true).then(mergeGraph);
    }));

    // Fetch datasets
    var datasetPromises = Promise.all(_.map(datasetIds, function(datasetId) {
      console.log('Mapping dataset: ' + datasetId);
      return api.describe(datasetId, true).then(function(graph) {
        mergeGraph(graph);

        _.forEach(graph, function(subgraph) {
          var structure = subgraph[shared.context.qb + 'structure'];
          var ids = _.pluck(structure, '@id');
          dsdIds = _.union(dsdIds, ids);
        });
      });
    }));

    // get DSDs
    var dsdPromises = datasetPromises.then(function() {
      return Promise.all(_.map(dsdIds, function(dsdId) {
        console.log('Mapping dsd: ' + dsdId);
        var dsdQuery =
          'CONSTRUCT { <' + dsdId + '> ?p ?o. ' +
          '<' + dsdId + '> qb:component ?c. ?c ?cP ?cO. } ' +
          'WHERE { <' + dsdId + '> ?p ?o. ' +
          '<' + dsdId + '> qb:component ?c. ?c ?cP ?cO. }';

        return conn.getGraph(dsdQuery).then(mergeGraph);
      }));
    });

    return Promise.all([propPromises, dsdPromises]);
  })
  .then(function() {
    return jsonldPromises.compact(allGraph, shared.context)
      .then(function(compacted) {
        shared.pointerizeGraph(compacted);
        allGraph = compacted;
      });
  })
  .then(function() {
    console.log('Processing datasets...');
    // Process datasets
    var isA = shared.ldIsA;

    var graph = allGraph['@graph'];
    if (!graph) {
      throw new Error('empty_graph');
    }

    _.forEach(graph, function(resource) {
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

    _.map(datasets, function(dataset) {
      // An ordered array of the dataset's dimensions, according to its DSD
      dataset.dimensions = [];

      // An ordered array of the dataset's measures, according to its DSD
      dataset.measures = [];

      var dsd = dataset['qb:structure'];
      var components = dsd['qb:component'];
      if (!dsd || !components) {
        return;
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

        dimension.literalValues.sort(naturalSort);

        var id = dimension['@id'];
        dataset.observations = dataset.observations.sort(function(a, b) {
          var valA = shared.getLdValue(a[id]);
          var valB = shared.getLdValue(b[id]);
          return naturalSort(valA, valB);
        });
      });

      return true;
    });
  }).then(function() {
    return datasets;
  });

  return promise;
};

api.sameAs = function(params) {
  var id = getId(params);

  var sameAsQuery =
    'select distinct ?twin ' +
    'where { { ?twin owl:sameAs <' + id + '> } ' +
    'union { <' + id + '> owl:sameAs ?twin } }';

  return conn.getColValues(sameAsQuery).then(function(col) {
    return col;
  });
};

api.periods = function(params) {
  var id = getId(params);

  var periodsQuery =
    'select distinct ?period { ' +
    '  [] a qb:Observation;' +
    '     <' + id + '> [];' +
    '     bm:refPeriod ?period.' +
    '  }' +
    'order by desc(?period)';

  return conn.getColValues(periodsQuery);
};

api.latestPeriod = function(params) {
  return api.periods(params).then(function(periods) {
    return periods[0];
  });
};

api.rankings = function(params) {
  var id = getId(params);
  var conditions = params.where;

  if (!id || !conditions) {
    return Promise
      .reject(new Error('Invalid parameters supplied for api.rankings.'));
  }

  // Not yet being used
  var context = _.assign({
    value: 'bm:value'
  }, shared.context);

  var conditionsString =
    shared.generateConditionsString(conditions, 'observation');

  var rankingsQUery =
    'construct {' +
    '  ?observation bm:value ?val;' +
    '    a qb:Observation;' +
    '    bm:refArea ?area.' +
    '  ?area rdfs:label ?label;' +
    '    geo:lat ?lat;' +
    '    geo:long ?long.' +
    '}' +
    'where {' +
    '  ?observation a qb:Observation;' +
    '    <' + id + '> ?val;' +
    '    bm:refArea ?areax.' +
    conditionsString +
    '  ?area owl:sameAs ?areax.' +
    '  ?area rdfs:label ?label;' +
    '    geo:lat ?lat;' +
    '    geo:long ?long.' +
    '  filter (lang(?label) = "") ' +
    '}';

  var promise = conn.getGraph({
    query: rankingsQUery,
    form: 'compact',
    context: shared.context,
    limit: 50000
  }).then(function(data) {
    var rankingsGraph = shared.pointerizeGraph(data);

    var rankings = [];
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

        return sortValue;
      });

      rankings.splice(idx, 0, val);
    });

    rankings.reverse();

    return rankings;
  });

  return promise;
};
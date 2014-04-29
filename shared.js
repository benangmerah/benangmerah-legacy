// Shared object

var util = require('util');
var cache = require('memory-cache');
var config = require('config');
var conn = require('starmutt');
var n3util = require('n3').Util;
var _ = require('lodash');

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
}

shared.getInferredTypes = function(uri, callback) {
  // Search cache
  var cacheEntry = cache.get('resolvedTypes:' + uri);
  if (cacheEntry) {
    return callback(err, cacheEntry);
  }

  var query = util.format('select ?type where { <%s> a ?type }', uri);
  conn.getColValues({ query: query, reasoning: 'QL' }, function(err, resolvedTypes) {
    if (err) {
      return callback(err);
    }

    cache.put('resolvedTypes:' + uri, resolvedTypes, cacheLifetime);
    return callback(null, resolvedTypes);
  });
}

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
}

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
}

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
}

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
}

shared.getPreferredLabel = function(jsonLdResource) {
  if (jsonLdResource['rdfs:label']) {
    var labels = jsonLdResource['rdfs:label'];
  }
  else if (jsonLdResource[shared.rdfsNS + 'label']) {
    var labels = jsonLdResource[shared.rdfsNS + 'label'];
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
  })

  return preferredLabel;
}

shared.getPropertyName = function(propertyName) {
  var delimiters = ['#', '/', ':'];

  for (var i = 0; i < delimiters.length; ++i) {
    var delimiter = delimiters[i];
    var index = propertyName.lastIndexOf(delimiter);
    if (index !== -1) {
      return propertyName.substring(index + 1);
    }
  }
}
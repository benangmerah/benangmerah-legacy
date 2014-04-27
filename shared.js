// Shared object

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

shared.getLdValue = function(ldObj) {
  if (typeof ldObj == 'string') {
    return ldObj;
  }

  if (ldObj['@value']) {
    return ldObj['@value'];
  }
}

shared.getDescriptionPath = function(resourceURI) {
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
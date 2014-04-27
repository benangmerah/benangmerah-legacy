var util = require('util');
var express = require('express');
var url = require('url');
var async = require('async');
var conn = require('starmutt');
var cache = require('memory-cache');
var config = require('config');

var lifetime = config.cachelifetime || 100;

var ontologyDefinition = 'https://raw.githubusercontent.com/benangmerah/wilayah/master/ontology.ttl';
var redirectPlacesTo = 'https://raw.githubusercontent.com/benangmerah/wilayah/master/instances.ttl';

var rdfNS = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
var rdfsNS = 'http://www.w3.org/2000/01/rdf-schema#';
var owlNS = 'http://www.w3.org/2002/07/owl#';
var xsdNS = 'http://www.w3.org/2001/XMLSchema#';
var ontNS = 'http://benangmerah.net/ontology/';
var placeNS = 'http://benangmerah.net/place/idn/';
var bpsNS = 'http://benangmerah.net/place/idn/bps/';
var geoNS = 'http://www.w3.org/2003/01/geo/wgs84_pos#';
var qbNS = 'http://purl.org/linked-data/cube#';

var context = {
  'rdf': rdfNS,
  'rdfs': rdfsNS,
  'owl': owlNS,
  'xsd': xsdNS,
  'bps': bpsNS,
  'geo': geoNS,
  'qb': qbNS,
  'bm': ontNS,
  '': 'http://data.ukp.go.id/dataset/penduduk-miskin-dan-indeks-kemiskinan#'
}

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
        callback(null, resolvedTypes);
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
      next();
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
  res.json('Hello place');
}

function describeThing(req, res, next) {
  function execDescribeQuery(callback) {
    var getLabelsQuery = util.format('describe <%s>', req.resourceURI);

    conn.getGraph({
      query: getLabelsQuery,
      form: 'compact',
      context: {
        'bm': 'http://benangmerah.net/ontology/',
        'geo': 'http://www.w3.org/2003/01/geo/wgs84_pos#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'owl': 'http://www.w3.org/2002/07/owl#'
      }
    }, function(err, data) {
      callback(err, data);
    });
  }

  function render(err, data) {
    if (err) {
      return next(err)
    }

    res.render('ontology/thing', {
      resource: data
    });
  }

  async.waterfall([execDescribeQuery], render);
}

function sameAsFallback(req, res, next) {
  if (!req.resourceURI) {
    next();
  }

  var query = util.format('select distinct ?twin \
    where { { ?twin owl:sameAs <%s> } \
    union { <%s> owl:sameAs ?twin } }', req.resourceURI);
  conn.getColValues(query, function(err, col) {
    if (err) {
      return next(err);
    }
    if (col.length === 1) {
      res.send('Redirectable to ' + col[0]);
    }
  });
}

router.use('/ontology', derefOntology);
router.use('/place', describeInternalResource);
router.use(ontologyRouter);

ontologyRouter.route('http://www.w3.org/2002/07/owl#Thing', describeThing);
ontologyRouter.route('http://benangmerah.net/ontology/Place', describePlace);
ontologyRouter.route('http://benangmerah.net/ontology/Provinsi', describeProvinsi);
ontologyRouter.route('http://benangmerah.net/ontology/Kota', describeKota);

router.use(sameAsFallback);
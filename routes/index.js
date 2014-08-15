var util = require('util');
var url = require('url');

var _s = require('underscore.string');
var async = require('async');
var conn = require('starmutt');
var express = require('express');

var shared = require('../shared');

var router = express.Router();
module.exports = router;

function index(req, res, next) {
  function execQuery(callback) {
    var query =
      'select distinct ?uri ?label ' +
      'where { graph ?g { ?uri a bm:Provinsi. ?uri rdfs:label ?label } }';

    conn.getResults(query, callback);
  }

  function processResults(results, callback) {
    var provinceLabels = {};

    results.forEach(function(result) {
      var uri = result.uri.value;
      var existingLabel = provinceLabels[uri];

      if (/^Prov/.test(result.label.value)) {
        // This label is too verbose
        return;
      }

      if (!existingLabel) {
        var label = result.label.value;
        provinceLabels[uri] = result.label;
        return;
      }

      var existingLang = existingLabel['xml:lang'];
      if (existingLang !== 'id' && result.label['xml:lang'] === 'id') {
        // Override the label
        provinceLabels[uri] = result.label;
      }
    });

    var provinces = [];
    for (var provinceURI in provinceLabels) {
      provinces.push({
        path: url.parse(provinceURI).path, // Remove the domain name
        name: provinceLabels[provinceURI].value
      });
    }

    callback(null, provinces);
  }

  function render(err, provinces) {
    // `provinces` is an array of { path: ..., name: ... }
    if (err) {
      next(err);
    }
    else {
      res.render('home/index', {
        provinces: provinces
      });
    }
  }

  async.waterfall([execQuery, processResults], render);
}

function search(req, res, next) {
  var searchQuery = req.query.q;
  var searchResults = [];
  var resultsGraph;

  function execSearchQuery(callback) {
    if (!searchQuery) {
      return callback('no_query');
    }

    var baseQuery = 
      'construct { ?s ?p ?o. ?s bm:score ?score. } ' +
      'where { graph ?g { ' +
      '  ?s ?p ?o. ' +
      '  ?s a ?type. ' +
      '  ?s rdfs:label ?l. ' +
      '  ( ?l ?score ) <http://jena.hpl.hp.com/ARQ/property#textMatch> ' +
      '  ( "%s" 0.5 50 ). ' +
      '  filter(?type != bm:DriverInstance) ' +
      '} }';

    var query = util.format(baseQuery, searchQuery.replace(/"/g, '\"'));

    console.log('Querying..');
    conn.getGraph({
      query: query,
      form: 'compact',
      context: shared.context
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      console.log(data);
      resultsGraph = data['@graph'];

      return callback();
    });
  }

  function processResults(callback) {
    if (!resultsGraph) {
      return callback();
    }

    resultsGraph = shared.pointerizeGraph(resultsGraph);

    resultsGraph.forEach(function(result) {
      if (!result['bm:score']) {
        return;
      }

      searchResults.push(result);
      if (result['rdfs:comment']) {
        result['rdfs:comment'] = _s.truncate(result['rdfs:comment'], 80);
      }
    });

    callback();
  }

  function render(err) {
    if (err instanceof Error) {
      return next(err);
    }
    if (err) {
      res.render('home/search');
    }

    res.locals.searchQuery = searchQuery;
    res.locals.searchResults = searchResults;
    console.log(searchResults);
    res.render('home/search');
  }

  async.series([execSearchQuery, processResults], render);
}

router.all('/', index);
router.all('/search', search);
var util = require('util');
var url = require('url');

var _s = require('underscore.string');
var async = require('async');
var conn = require('starmutt');
var express = require('express');

var shared = require('../shared');
var api = require('../lib/api');

var router = express.Router();
module.exports = router;

function index(req, res, next) {
  function execQuery(callback) {
    var query =
      'select distinct ?uri ?label ' +
      'where { ?uri a bm:Provinsi. ?uri rdfs:label ?label }';

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
  var searchPromise = api.search(searchQuery).then(function(results) {
    return results['@graph'];
  }).then(function(searchResults) {
    res.locals.searchQuery = searchQuery;
    res.locals.searchResults = searchResults;
    console.log(searchResults);
    res.render('home/search');
  }).catch(next);
}

router.all('/', index);
router.all('/search', search);
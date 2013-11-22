// BenangMerah driver for World Bank projects

var util = require('util'),
    async = require('async'),
    request = require('request'),
    models = require('../models'),
    Activity = models.Activity;

// Metadata for this driver
exports.meta = {
  title: 'World Bank Projects',
  description: 'A driver for pulling projects from the World Bank Projects API.'
};

function parseWorldBankProject(worldBankProject) {
  var act = new Activity;

  // id
  act._id = 'world-bank-project:' + worldBankProject.id;

  // title
  act.title = worldBankProject.project_name;

  // abstract
  if (worldBankProject.project_abstract)
    if (typeof worldBankProject.project_abstract == 'string')
      act.description = worldBankProject.project_abstract;
    else if (typeof worldBankProject.project_abstract.cdata == 'string')
      act.description = worldBankProject.project_abstract.cdata;

  // locations
  if (worldBankProject.locations)
    worldBankProject.locations.forEach(function(loc) {
      act.locations.push({
        name: loc.geoLocName,
        coordinates: {
          latitude: loc.latitude,
          longitude: loc.longitude
        }
      });
    });

  // Permalink
  act.url = worldBankProject.url;

  act._raw = worldBankProject;

  return act;
}

var pull = exports.pull = function(ref, params, callback) {
  // We ignore the ref, since there's only one source for this driver

  if (!params)
    params = {};

  var chunkSize = params.chunkSize || 25;
  var inParallel = params.parallel || true;

  var allParsedProjects = [];
  var requestQueue = [];

  function buildURL(page) {
    var p = page * chunkSize;
    var urlbase = 'http://search.worldbank.org/api/v2/projects?format=json&countrycode_exact=ID&source=IBRD&kw=N&os=%d&rows=%d';
    return util.format(urlbase, p, chunkSize);
  }

  function requestChunk(page, done) {
    request(buildURL(page), function(err, res, body) {
      if (err)
        done(err);
      else {
        try {
          var chunk = JSON.parse(body);
          done(null, chunk);
        }
        catch (e) {
          done(e);
        }
      }
    });
  }

  function requestChunkWrapper(page, done) {
    requestChunk(page, function(err, data) {
      if (err)
        done(err);
      else {
        for (var id in data.projects) {
          var parsedProject = parseWorldBankProject(data.projects[id]);
          allParsedProjects.push(parsedProject);
        }

        done();
      }
    })
  }

  // Steps:
  // 1. Find out number of projects
  // 2. Add a callback to requestQueue
  // 3. Execute the callbacks in requestQueue in series

  requestChunk(0, function(err, meta) {
    if (err)
      callback(err);
    else {
      // meta is an object containing metadata
      var totalProjects = meta.total;
      var pages = totalProjects / chunkSize;

      for (var i = 0; i < pages; ++i) {
        requestQueue.push(requestChunkWrapper.bind(null, i));
      }

      // Should we run in series or in parallel?
      var execQueue = inParallel ? async.parallel : async.series;

      // Execute the queue
      execQueue(requestQueue, function(err) {
        if (err)
          callback(err);
        else
          // No error; pass the parsed projects
          callback(null, {activities: allParsedProjects});
      })
    }
  })
}
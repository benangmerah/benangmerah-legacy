// BenangMerah driver for PNPM projects

var querystring = require('querystring'),
    async = require('async'),
    request = require('request'),
    models = require('../models'),
    Activity = models.Activity;

// Metadata for this driver
exports.meta = {
  title: 'PNPM Active Locations',
  description: 'A driver for the PNPM Mandiri Active Locations API.'
};

var locationLess = 0;

function parsePNPMProject(p) {
  // Strategy: treat each PNPM Location as a different project
  var act = new Activity;

  // id
  var u = function(str) { return str ? str.replace(/ /g, '_') : '' }
  act._id = 'pnpm-location://' + u(p.province) + '/' + u(p.kabupaten) + '/' + u(p.kecamatan);

  // title: For now, just make it the same
  act.title = 'PNPM Mandiri';

  if (!p.location)
    locationLess++;

  act.locations.push({
    name: p.kecamatan,
    coordinates: p.location ? {
      latitude: p.location.latitude,
      longitude: p.location.longitude
    } : {},
    administrative: {
      province: p.province,
      kabupaten: p.kabupaten,
      kecamatan: p.kecamatan
    }
  });

  act._raw = p;

  return act;
}

var pull = exports.pull = function(ref, params, callback) {
  // We ignore the ref, since there's only one source for this driver

  if (!params)
    params = {};

  var chunkSize = params.chunkSize || 1000;
  var inParallel = params.parallel || true;
  var appToken = params.appToken;

  var allParsedProjects = [];
  var locationIDs = [];
  var requestQueue = [];

  function buildURL(pageOrParams) {
    var endpoint = 'https://pnpm.socrata.com/resource/active-pnpm-locations-013014.json';

    if (typeof pageOrParams == 'number') {
      var page = pageOrParams;
      var offset = page * chunkSize;

      var params = {
        '$offset': offset,
        '$limit': chunkSize
      };
    }
    else {
      var params = pageOrParams;
    }

    if (appToken)
      params['$$app_token'] = appToken;

    return endpoint + '?' + querystring.stringify(params);
  }

  function requestCount(done) {
    request(buildURL({'$select':'count(*)'}), function(err, res, body) {
      if (err)
        done(err);
      else {
        try {
          var chunk = JSON.parse(body);
          done(null, chunk[0].count);
        }
        catch (e) {
          done(e);
        }
      }
    });
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
        data.forEach(function(project) {
          var parsedProject = parsePNPMProject(project);
          var _id = parsedProject._id;

          if (locationIDs.indexOf(_id) < 0) {
            locationIDs.push(_id);
            allParsedProjects.push(parsedProject);
          }
        });

        done();
      }
    })
  }

  // Steps:
  // 1. Find out number of projects
  // 2. Add a callback to requestQueue
  // 3. Execute the callbacks in requestQueue in series

  requestCount(function(err, count) {
    if (err)
      callback(err);
    else {
      var pages = Math.ceil(count / chunkSize);

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
          callback(null, { activities: allParsedProjects });
      })
    }
  })
}
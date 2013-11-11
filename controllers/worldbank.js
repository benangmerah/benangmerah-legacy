// World Bank BenangMerah Module Scaffold

var http = require('http'),
    async = require('async'),
    util = require('util');

var Activity = require('../models').Activity;

// Parse World Bank Project into BenangMerah standard
// This seems like a good start for a modular API
// @return Activity
var parseWorldBankProject = exports.parseWorldBankProject = function(worldBankProject) {
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

exports.index = function (req, res) {
  // http://search.worldbank.org/api/v2/projects?format=json&countrycode_exact=ID&source=IBRD&kw=N

  var chunkSize = 10;
  var allProjects = [];

  function buildQuery(page) {
    var p = page * chunkSize;
    return {
      hostname: 'search.worldbank.org',
      port: 80,
      path: '/api/v2/projects?format=json&countrycode_exact=ID&source=IBRD&kw=N&os=' + parseInt(p) + '&rows=' + chunkSize,
      method: 'GET'
    };
  }

  var pageRequestQueue = []; // array of functions to be run in series
  // Usage: pageRequest.bind(null, <pageNumber>)
  function pageRequest(page, done) {
    util.log('Requesting page ' + page);
    var opts = buildQuery(page);
    util.log('GET http://' + opts.hostname + opts.path);
    var greq = http.request(opts, function (wbres) {
      var status = wbres.statusCode;
      var headers = wbres.headers;

      wbres.setEncoding('utf8');

      var body = '';
      wbres.on('data', function (chunk) {
        body += chunk;
      });
      wbres.on('end', function () {
        util.log('Response for page ' + page + ' received.');
        obj = JSON.parse(body);
        for (var id in obj.projects) {
          var project = parseWorldBankProject(obj.projects[id]);
          util.log('Pushing project ' + id);
          allProjects.push(project);
        }

        done();
      })
    });

    greq.end();
  }

  var processActivities = function(activities, done) {
    // activities: array of BenangMerah Activities
    // f.s. data is entered into db

    async.map(activities, function(activity, done) {
      // Remove any existing activity
      activity.save(function(err, doc) {
        if (err)
          done(err);
        else
          done(null, doc);
      });
    }, done);
  }

  var render = function(err) {
    if (err)
      res.render('worldbank/uploaded', {error: err});
    else
      res.render('worldbank/uploaded', {success: true, projects: allProjects});
  }

  var metaQ = buildQuery(0);

  var wbreq = http.request(metaQ, function (wbres) {
    util.log('hello');
    require('util').inspect(wbres);
    var status = wbres.statusCode;
    var headers = wbres.headers;

    wbres.setEncoding('utf8');

    var body = '';
    wbres.on('data', function (chunk) {
      body += chunk;
    });
    wbres.on('end', function () {
      obj = JSON.parse(body);

      // We have: number of projects.
      total = obj.total;
      pages = obj.total / chunkSize;

      util.log('Metadata received. ' + pages + ' pages found.');

      // We are at page 0.
      for (i=0; i<pages; i++) {
        util.log('Pushing request for page ' + i + ' to queue.');
        pageRequestQueue.push(pageRequest.bind(null, i));
      }

      util.log('Executing queue...');
      async.series(pageRequestQueue, function(err) {
        if (err)
          render(err);
        else
          processActivities(allProjects, render);
      });
    })
  });

  wbreq.end();
}
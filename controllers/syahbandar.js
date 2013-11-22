var async = require('async'),
    moment = require('moment'),
    models = require('../models'),
    drivers = require('../drivers'),
    Datasource = models.Datasource;

// GET /syahbandar/index
// * List all datasources
// * List all enabled drivers
exports.index = function(req, res) {
  var availableDrivers = drivers.meta;
  Datasource.find().lean().exec(function(err, datasources) {
    if (err) {
      // Handle error
    }
    else {
      datasources.forEach(function(ds, idx) {
        var driver = ds.driver;
        var meta = availableDrivers[driver];
        if (meta && meta.title)
          ds.driverTitle = meta.title;
        else {
          ds.driverTitle = driver;
          if (!meta)
            ds.driverInvalid = true;
        }

        ds.displayLastPull = ds.lastPull ? moment(ds.lastPull).fromNow() : 'never';
        ds.displayNextPull = ds.nextPull ? moment(ds.nextPull).fromNow() : 'never';

        datasources[idx] = ds;
      })
      res.render('syahbandar/index', {
        datasources: datasources,
        drivers: availableDrivers
      })
    }
  })
}

// GET /syahbandar/details/:id
// Details of a datasource
exports.details = function(req, res) {

}

// GET /syahbandar/edit/:id
exports.edit = function(req, res) {

}

// POST /syahbandar/edit/:id
exports.processEdit = function(req, res) {

}

// GET /syahbandar/add
// Add a new datasource
exports.add = function(req, res) {

}

// POST /syahbandar/add
// Add a new datasource
exports.processAdd = function(req, res) {

}

// POST /syahbandar/pull
// Pull from a datasource
// TODO have the processing logic elsewhere
exports.pull = function(req, res) {
  function pullDatasource(datasource, callback) {
    if (!(datasource instanceof Datasource))
      callback('Invalid datasource.');
    else {
      var driverName = datasource.driver;
      var driver = drivers[driverName];

      if (!driver)
        callback('Invalid driver.');
      else {
        driver.pull(datasource.ref, datasource.params, function(err, data) {
          if (err)
            callback(err);
          else {
            var collectionWrapper = function(collection, callback2) {
              async.map(collection, function(doc, callback3) {
                // Override any existing doc
                // THIS DOES NOT YET WORK. PLEASE FIX.
                doc.save(function(err, doc) {
                  if (err)
                    callback3(err);
                  else
                    callback3(null, doc);
                });
              }, callback2);
            }

            var collectionQueue = {};
            for (var collection in data) {
              collectionQueue[collection] = collectionWrapper.bind(null, data[collection]);
            }
            async.parallel(collectionQueue, callback)
          }
        });
      }
    }
  }

  var id = req.params.id;
  Datasource.findById(id, function(err, ds) {
    if (err)
      res.render('syahbandar/pull', {err: err});
    else {
      console.log(ds);
      pullDatasource(ds, function(err, saved) {
        if (err)
          res.render('syahbandar/pull', {err: err, datasource: ds});
        else {
          console.log(saved.length);
          var counts = {};
          for (var collection in saved) {
            counts[collection] = saved[collection].length;
          }

          res.render('syahbandar/pull', {datasource: ds, counts: counts});
        }
      });
    }
  });
}
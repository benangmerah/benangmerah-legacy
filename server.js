var express = require('express');
var mongoose = require('mongoose');
var stardog = require('stardog');
var path = require('path');
var async = require('async');
var config = require('config');

// express middleware
var exphbs  = require('express3-handlebars');
var lessMiddleware = require('less-middleware');
var favicon = require('static-favicon');
var logger = require('morgan');
var bodyParser = require('body-parser');
var methodOverride = require('method-override');
var serveStatic = require('serve-static');
var errorHandler = require('errorhandler');

var conn = require('./starmutt');
var router = require('./router');

// express: init
var app = express();
module.exports = app;

// express: settings
app.set('views', __dirname + '/views');

// express: handlebars view engine
app.engine('handlebars', exphbs({defaultLayout: 'main', helpers: require('./views/helpers')}));
app.set('view engine', 'handlebars');

// express: middleware before routing
app.use(favicon());
app.use(logger('dev'));
app.use(bodyParser());
app.use(methodOverride());
app.use(lessMiddleware(
  'src/less',
  { dest: 'public/css', force: ('development' == app.get('env')) },
  {},
  { compress: !('development' == app.get('env')) }
));
app.use(serveStatic(path.join(__dirname, 'public')));

// app router
app.use(router);

// express: error handler
if ('development' == app.get('env')) {
  app.use(errorHandler({ dumpExceptions: true, showStack: true }));
}

function initMongoDb(callback) {
  if (!config.mongodb) {
    return callback();
  }

  // mongoose
  mongoose.connect(config.db);
  var db = mongoose.connection;

  // start listening
  db.on('error', callback);
  db.once('open', function() {
    callback();
  });
}

function initStardog(callback) {
  if (!config.stardog) {
    return callback();
  }

  conn.setEndpoint(config.stardog.endpoint);
  conn.setCredentials(config.stardog.username, config.stardog.password);
  conn.setDefaultDatabase(config.stardog.database);

  callback();
}

async.parallel([initMongoDb, initStardog],
  function listen(err, x) {
    if (err) console.log(err);
    app.listen(config.port || 3000, function() {
      console.log('BenangMerah running in ' + app.get('env') + ' mode on port ' + config.port + '.');
    });
  });
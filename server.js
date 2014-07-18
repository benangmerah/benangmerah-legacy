// BenangMerah
// To run: node server.js

// modules
var express = require('express');
var config = require('config');
var conn = require('starmutt');

// express middleware
var hbs = require('hbs');
var helperLib = require('handlebars-helpers');
var lessMiddleware = require('less-middleware');
var favicon = require('serve-favicon');
var logger = require('morgan');
var bodyParser = require('body-parser');
var methodOverride = require('method-override');
var serveStatic = require('serve-static');
var errorHandler = require('errorhandler');
var outputCache = require('express-output-cache');

var helpers = require('./helpers');
var routes = require('./routes');
var ontologyRoutes = require('./routes/ontology');
var dataManager = require('./routes/data-manager');

// CONFIG INIT ---
if (!config.port) {
  config.port = process.env.PORT || 3000;
}

// EXPRESS INIT ---

// express: init
var app = express();
module.exports = app;

// express: settings
app.set('views', __dirname + '/views');

// express: handlebars view engine
hbs.registerPartials(__dirname + '/views/partials');
helperLib.register(hbs, {});
for (var helper in helpers) {
  hbs.registerHelper(helper, helpers[helper]);
}

app.set('view engine', 'hbs');
app.set('view options', { layout: 'layouts/main' });

// express: middleware before routing
app.use(favicon(__dirname + '/public/favicon.ico'));
app.use(logger('dev'));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(methodOverride());
app.use('/css', lessMiddleware(
  __dirname + '/src/less',
  { dest: __dirname + '/public/css',
    force: (app.get('env') === 'development'),
    compiler: {
      compress: ('development' !== app.get('env')) } }
));
app.use(serveStatic(__dirname + '/public'));

// app router
app.use('/data-manager', dataManager);
app.use(outputCache(config.outputCache));
app.use(routes);
app.use(ontologyRoutes);

// debugging
outputCache.on('hit', function(key, req) {
  console.log('cache HIT: ' + req.originalUrl);
});
outputCache.on('miss', function(key, req) {
  console.log('cache MISS: ' + req.originalUrl);
});
outputCache.on('save', function(key, obj) {
  console.log('cache SAVE: ' + obj.body.length);
});
if ('development' == app.get('env')) {
  app.use(errorHandler({ dumpExceptions: true, showStack: true }));
}

// STARDOG INIT ---
if (config.stardog) {
  conn.setEndpoint(config.stardog.endpoint);
  conn.setCredentials(config.stardog.username, config.stardog.password);
  conn.setDefaultDatabase(config.stardog.database);
}

// RUN EXPRESS ---
app.listen(config.port, function() {
  console.log('BenangMerah running in ' + app.get('env') +
              ' mode on port ' + config.port + '.');
});
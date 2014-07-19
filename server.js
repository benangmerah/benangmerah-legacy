// BenangMerah
// To run: node server.js

// modules
var express = require('express');
var config = require('config');
var conn = require('starmutt');
var redis = require('redis');

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
if (!config.outputCache) {
  config.outputCache = {};
}

// STARDOG INIT ---
if (config.stardog) {
  conn.setEndpoint(config.stardog.endpoint);
  conn.setCredentials(config.stardog.username, config.stardog.password);
  conn.setDefaultDatabase(config.stardog.database);
}

// REDIS INIT ---
if (config.redis) {
  try {
    var redisClient = redis.createClient();
    config.outputCache.cacheClient = redisClient;

    if (config.stardog && config.stardog.cache) {
      if (typeof config.cache === 'object') {
        conn.setCacheClient(redisClient, config.stardog.cache.ttl);
      }
      else {
        conn.setCacheClient(redisClient);
      }
    }
  }
  catch (e) {
    console.log('Failed creating redis client.');
    config.outputCache.skipCache = true;
    conn.setCacheClient(null);
  }
}
else {
  config.outputCache.skipCache = true;
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
app.use(serveStatic(__dirname + '/public'));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(methodOverride());
app.use('/css', lessMiddleware(
  __dirname + '/src/less',
  { dest: __dirname + '/public/css',
    force: (app.get('env') === 'development'),
    compiler: {
      compress: ('development' !== app.get('env')) } }
));

// Data Manager - do not cache
app.use('/data-manager', dataManager);

// Everything else - cache
app.use(outputCache(config.outputCache));
app.use(routes);
app.use(ontologyRoutes);

// DEBUGGING ---

if ('development' === app.get('env')) {
  app.use(errorHandler({ dumpExceptions: true, showStack: true }));
}
if (config.redis) {
  function onHit(key) {
    console.log('Cache HIT  ' + key);
  }
  function onMiss(key) {
    console.log('Cache MISS ' + key);
  }
  function onPut(key) {
    console.log('Cache PUT  ' + key);
  }
  conn.cacheEvents.on('hit', onHit);
  conn.cacheEvents.on('miss', onMiss);
  conn.cacheEvents.on('put', onPut);
  outputCache.on('hit', onHit);
  outputCache.on('miss', onMiss);
  outputCache.on('put', onPut);
}

// RUN EXPRESS ---
app.listen(config.port, function() {
  console.log('BenangMerah running in ' + app.get('env') +
              ' mode on port ' + config.port + '.');
});
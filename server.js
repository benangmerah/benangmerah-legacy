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
var favicon = require('static-favicon');
var logger = require('morgan');
var bodyParser = require('body-parser');
var methodOverride = require('method-override');
var serveStatic = require('serve-static');
var errorHandler = require('errorhandler');

// local express-related modules
var helpers = require('./helpers');
var routes = require('./routes');
var ontologyRouter = require('./routes/ontology');

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

app.engine('handlebars', hbs.__express);
app.set('view engine', 'hbs');
app.set('view options', { layout: 'layouts/main' })

// express: middleware before routing
app.use(favicon());
app.use(logger('dev'));
app.use(bodyParser());
app.use(methodOverride());
app.use('/css', lessMiddleware(
  __dirname + '/src/less',
  { dest: __dirname + '/public/css', force: true },
  {},
  { compress: !('development' == app.get('env')) }
));
app.use(serveStatic(__dirname + '/public'));

// app router
app.use(routes);
app.use(ontologyRouter);

// express: error handler
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
  console.log('BenangMerah running in ' + app.get('env') + ' mode on port ' + config.port + '.');
});
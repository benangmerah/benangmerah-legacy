var express = require('express');
var mongoose = require('mongoose');
var path = require('path');

// express middleware
var exphbs  = require('express3-handlebars');
var lessMiddleware = require('less-middleware');
var favicon = require('static-favicon');
var logger = require('morgan');
var bodyParser = require('body-parser');
var methodOverride = require('method-override');
var serveStatic = require('serve-static');
var errorHandler = require('errorhandler');

var config = require('./config');
var router = require('./router');

// express: init
var app = express();

// express: settings
app.set('port', config.port);
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

// app router
app.use(router);

// express: middleware after routing
app.use(serveStatic(path.join(__dirname, 'public')));

// express: error handler
if ('development' == app.get('env')) {
  app.use(errorHandler({ dumpExceptions: true, showStack: true }));
}

// db is enabled
if (config.db) {
  // mongoose
  mongoose.connect(config.db);
  var db = mongoose.connection;

  // start listening
  db.on('error', console.error.bind(console, 'Connection error:'));
  db.once('open', function() {
    app.listen(config.port, function() {
      console.log('BenangMerah running in ' + app.get('env') + ' mode on port ' + config.port + '.');
    });
  });
}
else {
  app.listen(config.port, function() {
    console.log('BenangMerah running in ' + app.get('env') + ' mode on port ' + config.port + '.');
  });
}
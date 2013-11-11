var express = require('express'),
    exphbs  = require('express3-handlebars'),
    lessMiddleware = require('less-middleware'),
    mongoose = require('mongoose'),
    path = require('path'),
    loadRoutes = require('./routes');

// express: init
var app = express();

// app config
var config = require('./config')[app.get('env') || 'development'];

// express: settings
app.set('port', config.port);
app.set('views', __dirname + '/views');

// express: handlebars view engine
app.engine('handlebars', exphbs({defaultLayout: 'main', helpers: require('./views/helpers')}));
app.set('view engine', 'handlebars');

// express: middleware
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(app.router);
app.use(lessMiddleware({
    dest: path.join(__dirname, 'public/css'),
    src: path.join(__dirname, 'src/less'),
    prefix: '/css',
    compress: !('development' == app.get('env')),
    force: ('development' == app.get('env'))
}));
app.use(express.static(path.join(__dirname, 'public')));

// express: error handler
if ('development' == app.get('env')) {
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
}

// routes
require('./routes')(app);

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
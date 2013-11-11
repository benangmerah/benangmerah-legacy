var controllers = require('require-all')({
  dirname     :  __dirname + '/controllers',
  filter      :  /(.+)\.js$/,
  excludeDirs :  /^\.(git|svn)$/
});

module.exports = function(app) {
  app.get('/', controllers.home.index);
  app.get('/worldbank/index', controllers.worldbank.index);
}
var controllers = require('require-all')({
  dirname     :  __dirname + '/controllers',
  filter      :  /(.+)\.js$/,
  excludeDirs :  /^\.(git|svn)$/
});

module.exports = function(app) {
  app.get('/', controllers.home.index);
  app.get('/worldbank/index', controllers.worldbank.index);
  app.get('/api/points.json', controllers.api.points);
  app.get('/map', controllers.activity.map);
  app.get('/activities', controllers.activity.index);
  app.get('/activity/:id', controllers.activity.view);
  app.get('/syahbandar/index', controllers.syahbandar.index);
  app.get('/syahbandar/details/:id', controllers.syahbandar.details);
  app.get('/syahbandar/edit/:id', controllers.syahbandar.edit);
  app.post('/syahbandar/edit/:id', controllers.syahbandar.processEdit);
  app.post('/syahbandar/pull/:id', controllers.syahbandar.pull);
  app.get('/syahbandar', controllers.syahbandar.index);
}
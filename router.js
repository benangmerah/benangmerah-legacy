var express = require('express');
var requireAll = require('require-all');

var router = express.Router();

var controllers = requireAll({
  dirname     :  __dirname + '/controllers',
  filter      :  /(.+)\.js$/,
  excludeDirs :  /^\.(git|svn)$/
});

router.get('/', controllers.home.index);
router.get('/worldbank/index', controllers.worldbank.index);
router.get('/api/points.json', controllers.api.points);
router.get('/map', controllers.activity.map);
router.get('/activities', controllers.activity.index);
router.get('/activity/:id', controllers.activity.view);
router.get('/syahbandar/index', controllers.syahbandar.index);
router.get('/syahbandar/details/:id', controllers.syahbandar.details);
router.get('/syahbandar/edit/:id', controllers.syahbandar.edit);
router.post('/syahbandar/edit/:id', controllers.syahbandar.processEdit);
router.post('/syahbandar/pull/:id', controllers.syahbandar.pull);
router.get('/syahbandar', controllers.syahbandar.index);

module.exports = router;
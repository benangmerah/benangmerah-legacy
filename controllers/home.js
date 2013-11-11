var Activity = require('../models').Activity;

exports.index = function(req, res) {
  Activity.count({}, function(err, count) {
    res.render('home/index', {
      activityCount: count,
      orgCount: 1,
      maps: true
    });
  })
}
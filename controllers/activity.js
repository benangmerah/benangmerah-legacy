var Activity = require('../models').Activity,
    async = require('async');

exports.map = function(req, res) {
  res.render('activity/map', {maps: true});
}

exports.index = function(req, res) {
  Activity.find().lean().exec(function(err, data) {
    res.render('activity/index', {activities: data});
  })
}

exports.view = function(req, res) {
  Activity.findById(req.params.id, function(err, activity) {
    async.map(activity.relatedActivities, function(ra, done) {
      if (ra.ref) {
        Activity.findById(ra.ref, 'title').lean()
        .exec(function(err, doc) {
          if (err)
            done(err);
          else {
            ra.title = doc.title;
            done(null, ra);
          }
        })
      }
      else
        done(null);
    }, function(err, relatedActivities) {
      if (err)
        console.log(err);
      if (!err)
        activity.relatedActivities = relatedActivities;
    });

    res.render('activity/view', {error: err, activity: activity});
  })
}
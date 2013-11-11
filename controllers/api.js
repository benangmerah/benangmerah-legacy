var Activity = require('../models').Activity,
    markdown = require('marked');

exports.points = function(req, res) {
  Activity.find()
    .select({_id: 1, title: 1, description: 1, locations: 1, reportingOrgs: 1})
    .exec(function(err, activities) {
      if (err)
        res.render('map', {layout: 'map-page', error: err});
      else {
        points = [];
        activities.forEach(function(activity) {
          activity.locations.forEach(function(location) {
            point = {
              lat: location.coordinates.latitude,
              lon: location.coordinates.longitude,
              ref: activity._id,
              title: activity.title,
              reportingOrgNames: [],
              // description: activity.description,
              descriptionHTML: markdown(activity.description || ''),
              adm: location.name
            };

            activity.reportingOrgs.forEach(function(org) {
              point.reportingOrgNames.push(org.name)
            });

            points.push(point);
          });
        });
        res.json(points);
      }
    })
}
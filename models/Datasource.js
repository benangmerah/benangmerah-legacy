var querystring = require('querystring'),
    mongoose = require('mongoose'),
    moment = require('moment'),
    Schema = mongoose.Schema;

var datasourceSchema = new Schema({
  _id: String,          // A unique, no-spaces but human-readable identifier
  title: String,        // Full title of the datasource
  description: String,  // A description for this datasource
  driver: String,       // The driver used to pull from this datasource. Must be in /drivers
  ref: String,          // A URI or other identifier used by the driver to identify where to pull from
  lastPull: Date,       // When the datasource was last pulled
  nextPull: Date,       // When to do the next pull
  interval: String,     // Pull interval: [Number] (days|months|years)
  paramsQS: String      // Parameters to feed the driver (such as API key) in query string form
});

datasourceSchema.virtual('params')
  .get(function() {
    return this.paramsQS ? querystring.parse(this.paramsQS.trim()) : {};
  })
  .set(function(v){
    this.paramsQS = querystring.stringify(v);
  });

datasourceSchema.methods.getIntervalDuration = function() {
  var interval = this.interval;
  var split = interval.split(' ');
  var duration = moment.duration(split[0], split[1]);
  return duration;
}

datasourceSchema.virtual('driverTitle').get(function() {
  var meta = require('../drivers').meta[this.driver];
  return meta ? meta.title : this.driver;
});

module.exports = mongoose.model('Datasource', datasourceSchema);
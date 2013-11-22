var mongoose = require('mongoose'),
    moment = require('moment'),
    Schema = mongoose.Schema;

var datasourceSchema = new Schema({
  _id: String,          // A unique, no-spaces but human-readable identifier
  description: String,  // A description for this datasource
  driver: String,       // The driver used to pull from this datasource. Must be in /drivers
  ref: String,          // A URI or other identifier used by the driver to identify where to pull from
  nextPull: Date,       // When to do the next pull
  interval: String,     // Pull interval: [Number] (days|months|years)
  paramsJSON: String    // A JSON-encoded object of parameters to feed the driver, such as API key
});

datasourceSchema.virtual('params')
  .get(function() {
    return JSON.parse(this.paramsJSON);
  })
  .set(function(v){
    this.paramsJSON = JSON.stringify(v);
  });

datasourceSchema.methods.getIntervalDuration = function() {
  var interval = this.interval;
  var split = interval.split(' ');
  var duration = moment.duration(split[0], split[1]);
  return duration;
}

module.exports = mongoose.model('Datasource', datasourceSchema);
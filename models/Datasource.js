var mongoose = require('mongoose'),
    Schema = mongoose.Schema;

var datasourceSchema = new Schema({
  _id: String,          // A unique, no-spaces but human-readable identifier
  description: String,  // A description for this datasource
  driver: String,       // The driver used to pull from this datasource. Must be in /drivers
  ref: String,          // A URI or other identifier used by the driver to identify where to pull from
  paramsJSON: String    // A JSON-encoded object of parameters to feed the driver, such as API key
});

datasourceSchema.virtual('params')
  .get(function() {
    return JSON.parse(this.paramsJSON);
  })
  .set(function(v){
    this.paramsJSON = JSON.stringify(v);
  });

module.exports = mongoose.model('Datasource', datasourceSchema);
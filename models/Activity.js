var mongoose = require('mongoose'),
    Schema = mongoose.Schema;

var documentLinkSchema = new Schema({
  title: String,
  url: String,
  format: String,
  categoryCodes: [String]
});

var orgRefSchema = new Schema({
  name: String,
  role: String,
  type: Number,
  ref: String
});

var locationSchema = new Schema({
  name: String,
  description: String,
  coordinates: {
    latitude: String,
    longitude: String,
    precision: Number
  },
  administrative: {
    province: String,
    kabupaten: String,
    kecamatan: String,
    desa: String,
    country: String,
    adm1: String,
    adm2: String,
    adm3: String,
    adm4: String,
    adm5: String
  }
});
locationSchema.methods.fullAdm = function(separator) {
  if (!separator)
    separator = ', ';

  var output = '';
  for (i=5; i>=1; i--) {
    key = 'adm' + i;
    value = this.administrative[key];
    if (value)
      output += value + ', ';
  }

  output += require('./countrycodes')[this.administrative.country];

  return output;
};

relatedActivitySchema = new Schema({
  type: String,
  ref: {
    type: String,
    ref: 'Activity'
  }
});

var activitySchema = new Schema({
  _id: String, // A unique URI or pseudo-URI to identify the activity
  driver: String, // The name of the driver this activity comes from
  providerRef: String, // An identifier for the activity, as assigned by the project owner
  title: String, // Title/name of the project
  description: String, // Description of the project
  url: String, // A permalink to the project
  locations: [locationSchema],
  status: String,
  documentLinks: [documentLinkSchema],
  participatingOrgs: [orgRefSchema],
  reportingOrgs: [orgRefSchema],
  relatedActivities: [relatedActivitySchema],
  _rawJSON: String
});

activitySchema.virtual('_raw')
.get(function() {
  return JSON.parse(this._rawJSON);
})
.set(function(v) {
  this._rawJSON = JSON.stringify(v);
});

module.exports = mongoose.model('Activity', activitySchema);
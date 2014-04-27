// helpers
// All members of module.exports will be registered as a hbs helper

var shared = require('./shared');
var Handlebars = require('hbs').handlebars;

var helpers = module.exports;

helpers.ldValue = function(value) {
  return shared.getLdValue(value);
}

// Show a preferred label based on locale and other heuristics
helpers.preferredLabel = function() {
  if (arguments.length === 1) {
    var resource = this;
  }
  else {
    var resource = arguments[0];
  }

  return shared.getPreferredLabel(resource);
}

helpers.descriptionLink = function(value) {
  if (typeof value === 'string') {
    var uri = value;
  }
  else {
    var uri = value['@id'];
  }

  var descriptionPath = shared.getDescriptionPath(uri);

  return new Handlebars.SafeString('<a href="' + descriptionPath + '">' + uri + '</a>');
}

helpers.ldObject = function(ldObj) {
  if (ldObj instanceof Array) {
    ldObj = ldObj.map(helpers.ldObject);
    return ldObj.join(', ');
  }
  if (typeof ldObj === 'string') {
    return ldObj;
  }
  else if (ldObj['@id']) {
    return helpers.descriptionLink(ldObj);
  }
  else {
    return helpers.ldValue(ldObj);
  }
}
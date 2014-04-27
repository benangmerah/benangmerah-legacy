// helpers
// All members of module.exports will be registered as a hbs helper

var _ = require('lodash');
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

helpers.descriptionLink = function(value, lbl, options) {
  var hash = (options && options.hash) || {};
  if (typeof value === 'string') {
    var uri = value;
  }
  else {
    var uri = value['@id'];
  }

  if (hash.raw) {
    var descriptionPath = uri;
  }
  else {
    var descriptionPath = shared.getDescriptionPath(uri);
  }

  var label;
  if (arguments.length === 3 && lbl)
    label = lbl;
  if (!label)
    label = shared.getPreferredLabel(value);
  if (!label)
    label = uri;

  return new Handlebars.SafeString('<a href="' + descriptionPath + '">' + label + '</a>');
}

helpers.ldObject = function(ldObj) {
  if (ldObj instanceof Array) {
    ldObj = ldObj.map(helpers.ldObject);
    return new Handlebars.SafeString(ldObj.join(', '));
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

helpers.rawLdObject = function(ldObj) {
  if (ldObj instanceof Array) {
    ldObj = ldObj.map(helpers.ldObject);
    return new Handlebars.SafeString(ldObj.join(', '));
  }
  if (typeof ldObj === 'string') {
    return ldObj;
  }
  else if (ldObj['@id']) {
    return helpers.descriptionLink(ldObj, null, {raw: true});
  }
  else {
    return helpers.ldValue(ldObj);
  }
}

var deferredBlocks = [];

helpers.defer = function(options) {
  var deferredBlock = options.fn(this);
  deferredBlocks.push(deferredBlock);
  return '';
}

helpers.flush = function() {
  var output = deferredBlocks.join('');

  delete deferredBlocks;
  deferredBlocks = [];

  return new Handlebars.SafeString(output);
}

helpers.periodValues = function(periods, options) {
  var output = '';
  var self = this;
  periods.forEach(function(period) {
    output += options.fn(self, {data: { value: self[period] } });
  });

  return output;
}

helpers.ifCollection = function(collection, options) {
  if (!_.isEmpty(collection)) {
    return options.fn(this);
  }
  else {
    return options.inverse(this);
  }
}

helpers.link = function(text, options) {
  var hash = (options && options.hash) || {};
  var attrs = [];

  if (!hash.href) {
    hash.href = text;

    for(var prop in hash) {
      attrs.push(prop + '="' + hash[prop] + '"');
    }

    return new Handlebars.SafeString(
      "<a " + attrs.join(" ") + ">" + text + "</a>"
    );
  }
}

helpers.trim = function(text) {
  return text.trim();
}
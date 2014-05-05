// helpers
// All members of module.exports will be registered as a hbs helper

var entities = require('entities');
var traverse = require('traverse');
var _ = require('lodash');

var shared = require('./shared');
var Handlebars = require('hbs').handlebars;

var helpers = module.exports;

helpers.ldValue = function(value) {
  return shared.getLdValue(value);
};

// Show a preferred label based on locale and other heuristics
helpers.preferredLabel = function(resource, options) {
  if (arguments.length === 1) {
    resource = this;
  }
  else {
    resource = arguments[0];
  }

  return shared.getPreferredLabel(resource);
};

helpers.descriptionLink = function(value, lbl, options) {
  var hash = (options && options.hash) || {};
  var uri;
  if (typeof value === 'string') {
    uri = value;
  }
  else {
    uri = value['@id'];
  }

  var descriptionPath;
  if (hash.raw) {
    descriptionPath = uri;
  }
  else {
    descriptionPath = shared.getDescriptionPath(uri);
  }

  var label;
  if (arguments.length === 3 && lbl)
    label = lbl;
  if (!label)
    label = shared.getPreferredLabel(value);
  if (!label)
    label = uri;

  return new Handlebars.SafeString('<a href="' + descriptionPath + '">' + label + '</a>');
};

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
};

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
};

var deferredBlocks = [];

helpers.defer = function(options) {
  var deferredBlock = options.fn(this);
  deferredBlocks.push(deferredBlock);
  return '';
};

helpers.flush = function() {
  var output = deferredBlocks.join('');

  deferredBlocks.length = 0;

  return new Handlebars.SafeString(output);
};

helpers.periodValues = function(periods, options) {
  var output = '';
  var self = this;
  periods.forEach(function(period) {
    output += options.fn(self, {data: { value: self[period] } });
  });

  return output;
};

helpers.ifCollection = function(collection, options) {
  if (!_.isEmpty(collection)) {
    return options.fn(this);
  }
  else {
    return options.inverse(this);
  }
};

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
};

helpers.trim = function(text) {
  return text.trim();
};

helpers.datacubeTable = function(dataset, options) {
  if (!options) {
    options = dataset;
    dataset = this;
  }

  var getLabel = shared.getPreferredLabel;
  var getValue = shared.getLdValue;
  // Columns: dimensions, using colspan
  // First column: Measurement
  // Output: table

  if (options) {
    var hash = options.hash || {};
    if (hash.defaultHeader) {
      var defaultHeader = hash.defaultHeader;
      delete hash.defaultHeader;
    }
    if (hash.dimensionLabels) {
      var dimensionLabels = hash.dimensionLabels;
      delete hash.dimensionLabels;
    }
  }

  var output = '<table';
  if (options && !_.isEmpty(options.hash)) {
    _.forIn(options.hash, function(value, attr) {
      output += ' ' + attr + '=' + '"' + value + '"';
    });
  }
  output += '>';

  // Header
  var dimensions = dataset.dimensions;

  output += '\n  <thead>';
  dimensions.forEach(function(dimension, idx) {
    var values = dimension.values;

    var nextDimension = dimensions[idx + 1];
    var colSpan = '1';
    if (nextDimension) {
      colSpan = nextDimension.values.length;
    }

    var firstColumn = dimensionLabels ? getLabel(dimension) : '';

    output += '\n    <tr>';
    output += '\n      <th>' + firstColumn + '</th>';
    // console.log(values);
    values.forEach(function(value) {
      output += '\n      <th colspan="' + colSpan + '">' + helpers.ldObject(value) + '</th>';
    });
    output += '\n    </tr>';
  });
  output += '\n  </thead>';

  // Body
  var measures = dataset.measures;

  output += '\n  <tbody>';
  measures.forEach(function(measure, idx) {
    var measureId = measure['@id'];
    var measureLabel = getLabel(measure);
    var cursor = dataset.datacube;

    output += '\n    <tr>';
    output += '\n      <th>' + measureLabel + '</th>';
    traverse(dataset.datacube).forEach(function() {
      if (this.level === dimensions.length) {
        var text = helpers.ldObject(this.node[measureId]);
        output += '\n      <td>' + text + '</td>';
      }
    });
    output += '\n    </tr>';
  });
  output += '\n  </tbody>';

  // End table
  output += '\n</table>';

  return new Handlebars.SafeString(output);
};

helpers.input = function(property, options) {
  var hash = (options && options.hash) || {};
  var attrs = [];
  for (var prop in hash) {
    attrs.push(prop + '="' + hash[prop] + '"');
  }

  attrs.push('name="' + property + '"');
  if (_.isString(this[property])) {
    attrs.push('value="' + entities.encodeHTML(this[property]) + '"');
  }

  return new Handlebars.SafeString('<input ' + attrs.join(' ') + '>');
};

helpers.textarea = function(property, options) {
  var hash = (options && options.hash) || {};
  var attrs = [];
  for (var prop in hash) {
    attrs.push(prop + '="' + hash[prop] + '"');
  }

  attrs.push('name="' + property + '"');

  var value = '';
  if (_.isString(this[property])) {
    value = entities.encodeHTML(this[property]);
  }

  return new Handlebars.SafeString('<textarea ' + attrs.join(' ') + '>' + value + '</textarea>');
};

helpers.select = function(property, choices, options) {
  var self = this;
  var hash = (options && options.hash) || {};
  var attrs = [];
  for (var prop in hash) {
    attrs.push(prop + '="' + hash[prop] + '"');
  }

  attrs.push('name="' + property + '"');

  var optionsString = '';
  if (_.isArray(choices)) {
    _.forEach(choices, function(value) {
      optionsString += '<option value="';
      optionsString += entities.encodeHTML(value) + '"';

      if (self[property] == value) {
        optionsString += ' selected';
      }

      optionsString += '>' + entities.encodeHTML(value);
      optionsString += '</optionsString>';
    });
  }
  else if (_.isObject(choices)) {
    _.forIn(choices, function(label, value) {
      optionsString += '<option value="';
      optionsString += entities.encodeHTML(value) + '"';

      if (self[property] == value) {
        optionsString += ' selected';
      }

      optionsString += '>' + entities.encodeHTML(label);
      optionsString += '</optionsString>';
    });
  }

  return new Handlebars.SafeString('<select ' + attrs.join(' ') + '>' + optionsString + '</select>');
};
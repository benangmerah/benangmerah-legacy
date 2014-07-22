// helpers
// All members of module.exports will be registered as a hbs helper

var entities = require('entities');
var traverse = require('traverse');
var _ = require('lodash');
var _s = require('underscore.string');

var shared = require('./shared');
var Handlebars = require('hbs').handlebars;

require('helper-moment').register(Handlebars, {});

var helpers = module.exports;

helpers.ldValue = function(value) {
  var theValue = shared.getLdValue(value);
  if (value['@type'] === 'xsd:decimal') {
    var dot = theValue.indexOf('.');
    var decimals = (dot > -1) ? theValue.length - 1 - dot : 0;
    return _s.numberFormat(parseFloat(theValue), decimals, ',', '.');
  }

  return theValue;
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

helpers.preferredDatasetLabel = function(resource, options) {
  if (arguments.length === 1) {
    resource = this;
  }
  else {
    resource = arguments[0];
  }

  var preferredLabel = shared.getPreferredLabel(resource);

  var perIndex = preferredLabel.toLowerCase().lastIndexOf(' per ');
  if (perIndex > -1) {
    return preferredLabel.substring(0, perIndex);
  }

  return preferredLabel;
}

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
  if (arguments.length === 3 && lbl) {
    label = lbl;
  }
  if (!label) {
    label = shared.getPreferredLabel(value);
  }
  if (!label) {
    label = uri;
  }

  return new Handlebars.SafeString(
    '<a href="' + descriptionPath + '">' + label + '</a>');
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

helpers.extendedLabel = function(ldObj, helpText) {
  var html = '<span>' + shared.getPreferredLabel(ldObj) +
             ' ' + helpers.descriptionLink(ldObj,
              '<i class="glyphicon glyphicon-info-sign more-info"></i>', {}) +
             '</span>';

  return new Handlebars.SafeString(html);
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
};

helpers.leaderboard = function(context, rankKey, options) {
  // Forked from Handlebars#each

  if (!options) {
    throw new Error('Must pass iterator to #leaderboard');
  }

  var fn = options.fn;
  var i = 0;
  var rank = 0;
  var ret = '', data;

  var contextPath;

  if (_.isFunction(context)) { context = context.call(this); }

  if (options.data) {
    data = _.extend({}, options.data);
    data._parent = options.data;
  }

  var previousValue;
  var lastRank;

  if (context && typeof context === 'object') {
    _.forEach(context, function(item, i) {
      ++rank;

      if (data) {
        var thisValue = item[rankKey];
        thisValue = shared.getLdValue(thisValue);

        if (thisValue === previousValue) {
          data.rank = lastRank;
          data.sameAsPrevious = true;
        }
        else {
          lastRank = rank;
          data.rank = rank;
          data.sameAsPrevious = false;
        }

        data.index = i;
        data.first = (i === 0);
        data.last = (i === (context.length - 1));

        if (contextPath) {
          data.contextPath = contextPath + i;
        }

        previousValue = thisValue;
      }

      ret = ret + fn(item, { data: data });

      ++i;
    });
  }

  return ret;
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
      '<a ' + attrs.join(' ') + '>' + text + '</a>'
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

  var dimensions = dataset.dimensions;
  var measures = dataset.measures;

  if (measures.length === 1 && dimensions.length > 1) {
    var shiftedDimensions = _.clone(dimensions);
    var firstDimension = shiftedDimensions.shift();

    // Header
    output += '\n  <thead>';
    shiftedDimensions.forEach(function(dimension, idx) {
      var values = dimension.values;

      var previousDimension = shiftedDimensions[idx - 1];
      var repeat = 1;
      if (previousDimension) {
        repeat = previousDimension.values.length;
      }

      var nextDimension = shiftedDimensions[idx + 1];
      var colSpan = '1';
      if (nextDimension) {
        colSpan = nextDimension.values.length;
      }

      var firstColumn = dimensionLabels ? getLabel(dimension) : '';

      output += '\n    <tr>';
      output += '\n      <th>' + firstColumn + '</th>';
      // console.log(values);
      _.times(repeat, function() {
        values.forEach(function(value) {
          output += '\n      <th colspan="' + colSpan + '">';
          output += helpers.ldObject(value) + '</th>';
        });
      });

      output += '\n    </tr>';
    });
    output += '\n  </thead>';

    // Body
    output += '\n  <tbody>';
    var measure = measures[0];
    var measureId = measure['@id'];
    var measureLabel = getLabel(measure);
    firstDimension.literalValues.forEach(function(value) {
      var cursor = dataset.datacube[value];

      output += '\n    <tr>';
      output += '\n      <th>';
      output += helpers.ldObject(value);
      output += '</th>';
      traverse(cursor).forEach(function() {
        if (this.level < shiftedDimensions.length) {
          this.keys = shiftedDimensions[this.level].literalValues;
        }
        else if (this.level === shiftedDimensions.length) {
          var text = this.node ? helpers.ldObject(this.node[measureId]) : '';
          output += '\n      <td>' + text + '</td>';
        }
      });
      output += '\n    </tr>';
    });
    output += '\n  </tbody>';
  }
  else {
    // Header

    output += '\n  <thead>';
    dimensions.forEach(function(dimension, idx) {
      var values = dimension.values;

      var previousDimension = dimensions[idx - 1];
      var repeat = 1;
      if (previousDimension) {
        repeat = previousDimension.values.length;
      }

      var nextDimension = dimensions[idx + 1];
      var colSpan = '1';
      if (nextDimension) {
        colSpan = nextDimension.values.length;
      }

      var firstColumn = dimensionLabels ? getLabel(dimension) : '';

      output += '\n    <tr>';
      output += '\n      <th>' + firstColumn + '</th>';
      // console.log(values);
      _.times(repeat, function() {
        values.forEach(function(value) {
          output += '\n      <th colspan="' + colSpan + '">';
          output += helpers.ldObject(value) + '</th>';
        });
      });

      output += '\n    </tr>';
    });
    output += '\n  </thead>';

    // Body

    output += '\n  <tbody>';
    measures.forEach(function(measure, idx) {
      var measureId = measure['@id'];
      var measureText = helpers.extendedLabel(measure);
      var cursor = dataset.datacube;

      output += '\n    <tr>';
      output += '\n      <th>' + measureText + '</th>';
      traverse(dataset.datacube).forEach(function() {
        if (this.level < dimensions.length) {
          this.keys = dataset.dimensions[this.level].literalValues;
        }
        else if (this.level === dimensions.length) {
          var text = this.node ? helpers.ldObject(this.node[measureId]) : '';
          output += '\n      <td>' + text + '</td>';
        }
      });
      output += '\n    </tr>';
    });
    output += '\n  </tbody>';
  }

  // End table
  output += '\n</table>';

  return new Handlebars.SafeString(output);
};

helpers.not = function(value, options) {
  if (!value) {
    return options.fn(this);
  }
}

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

  return new Handlebars.SafeString(
    '<textarea ' + attrs.join(' ') + '>' + value + '</textarea>');
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

  return new Handlebars.SafeString(
    '<select ' + attrs.join(' ') + '>' + optionsString + '</select>');
};

helpers.logLevelClass = function(logLevel, options) {
  if (!options) {
    logLevel = this.level;
  }

  if (logLevel === 'error') {
    return 'danger';
  }
  else if (logLevel === 'warn') {
    return 'warning';
  }
  else if (logLevel === 'finish') {
    return 'success';
  }
  else {
    return 'info';
  }
}

helpers.json = function(value) {
  return new Handlebars.SafeString(JSON.stringify(value));
}

helpers.datasetJson = function(dataset) {
  var obj = {
    dimensions: _.map(dataset.dimensions, function(v) {
      return _.omit(v, '@type');
    }),
    measures: _.map(dataset.measures, function(v) {
      return _.omit(v, '@type');
    }),
    observations: _.map(dataset.observations, function(v) {
      return _.omit(v, '@id', '@type', 'qb:dataSet', 'bm:refArea');
    })
  };

  return helpers.json(obj);
}
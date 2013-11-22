var markdown = require('marked'),
    moment = require('moment');

exports.json = function(obj) {
  return JSON.stringify(obj);
}

exports.markdown = function(text) {
  return text ? markdown(text.trim()) : '';
}

exports.map = function(lat, lon) {
  // http://maps.google.com/maps?z=12&t=m&q=loc:38.9419+-78.3020
  return 'http://maps.google.com/maps?z=12&t=m&q=loc:' + lat + '+' + lon;
}

exports.code = function(collection, code) {
  console.log(collection);
  return codelists.getName(collection, code);
}

exports.date = function(date, format) {
  return moment(date).format(format);
}

exports.if_eq = function(a, b, options) {
  if (a == b)
    return options.fn(this);
}
var markdown = require('marked');

exports.json = function(obj) {
  return JSON.stringify(obj);
}

exports.markdown = function(text) {
  return text ? markdown(text.trim()) : '';
}

map = exports.map = function(lat, lon) {
  // http://maps.google.com/maps?z=12&t=m&q=loc:38.9419+-78.3020
  return 'http://maps.google.com/maps?z=12&t=m&q=loc:' + lat + '+' + lon;
}

code = exports.code = function(collection, code) {
  console.log(collection);
  return codelists.getName(collection, code);
}
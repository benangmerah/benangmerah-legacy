var drivers = require('require-all')({
  dirname     :  __dirname + '/drivers',
  filter      :  /(.+)\.js$/,
  excludeDirs :  /^\.(git|svn)$/
});

var meta = {};

for (var driver in drivers) {
  var driverMeta = drivers[driver].meta;
  meta[driver] = driverMeta || {};
}

drivers.meta = meta;

module.exports = drivers;
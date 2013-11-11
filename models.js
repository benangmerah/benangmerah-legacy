var models = require('require-all')({
  dirname     :  __dirname + '/models',
  filter      :  /(.+)\.js$/,
  excludeDirs :  /^\.(git|svn)$/
});

module.exports = models;
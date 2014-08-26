// BenangMerah server

var app = require('./app');
var config = require('config');

var port = parseInt(process.argv[2]) || config.port || process.env.PORT;

app.listen(port, function() {
  console.error('BenangMerah');
  console.error('===========\n');
  console.error('Port:        %s', port);
  console.error('Environment: %s', app.get('env'));
});
// Define configuration here
var config = {
  // General config
  general: {

  },

  // Per-environment config, overrides general config according to environment
  environments: {
    development: {
      db: 'mongodb://localhost/benangmerah',
      port: 3000
    },
    production: {
      db: '', // MongoDB connection string
      port: process.env.PORT || 80
    },

    // For deployment on Windows Azure Web Sites
    azure: {
      db: process.env.CUSTOMCONNSTR_benangmerah,
      port: process.env.PORT
    }
  }
}

// Process the config based on NODE_ENV

var processedConfig = config.general;
var env = process.env.NODE_ENV || 'development';

if (config.environments[env]) {
  for (var key in config.environments[env]) {
    processedConfig[key] = config.environments[env][key];
  }
}

module.exports = processedConfig;
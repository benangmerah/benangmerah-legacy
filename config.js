config = module.exports = {
  development: {
    db: 'mongodb://localhost/benangmerah',
    skipdb: true,
    port: 3000
  },
  production: {
    db: '', // MongoDB connection string
    port: 80
  },

  // For deployment on Windows Azure
  // Not 'production' because it only makes sense in Windows Azure
  azure: {
    db: process.env.CUSTOMCONNSTR_benangmerah,
    port: process.env.PORT
  }
}
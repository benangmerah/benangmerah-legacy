config = module.exports = {
  development: {
    db: 'mongodb://localhost/benangmerah',
    skipdb: true,
    port: 3000
  },
  production: {
    db: process.env.CUSTOMCONNSTR_aidglasses,
    port: process.env.PORT || 80
  }
}
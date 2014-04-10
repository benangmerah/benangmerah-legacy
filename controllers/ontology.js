var redirectTo = 'https://raw.githubusercontent.com/benangmerah/wilayah/master/ontology.ttl';

exports.deref = function(req, res, next) {
  res.redirect(303, redirectTo);
};
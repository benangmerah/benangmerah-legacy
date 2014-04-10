var redirectTo = 'https://raw.githubusercontent.com/benangmerah/wilayah/master/ontology.ttl';
var redirectPlacesTo = 'https://raw.githubusercontent.com/benangmerah/wilayah/master/instances.ttl';

exports.deref = function(req, res, next) {
  res.redirect(303, redirectTo);
};

exports.derefPlaces = function(req, res, next) {
  res.redirect(303, redirectPlacesTo);
}
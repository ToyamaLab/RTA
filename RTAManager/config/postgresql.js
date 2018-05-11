var options = {
    // initialization options;
};

var pgp = require("pg-promise")(options);

let connection = {
    host     : 'spacia.db.ics.keio.ac.jp',
    user     : 'shu',
    password : 'shu',
    database : 'rta_databases'
};

var db = pgp(connection);
module.exports = db;

exports.close = function() {
  pgp.end();
};

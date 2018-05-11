const express = require('express');
const router = express.Router();
const fs = require('fs-extra');
const util = require('util');
const path = require('path');
const constant = require('../../config/constant');
const helper = require('../../helper/helper');

router.get('/:tbname', function(req, res, next) {

  var rtadir = constant.rtadir;

  var crypted = fs.readFileSync(path.join(rtadir, req.params.tbname + '.rta'), 'utf8');
  var decrypted = helper.decrypt(crypted);
  var json = JSON.parse(decrypted, 'utf8');

  var dbms = json.dbms;

  switch (dbms) {
    case 'mysql':

      break;

    case 'postgresql':
      var options = {};

      var pgp = require("pg-promise")(options);

      let connection = {
          host     : json.host,
          user     : json.user,
          password : json.password,
          database : json.dbname
      };

      var db = pgp(connection);

      var sql = 'SELECT * FROM $1~';
      db.result(sql, json.tbname)
        .then(result => {
          res.render('content', {
            title: 'Express',
            tbname: json.tbname,
            rows: result.rows,
            fields: result.fields
          });
        })

        .catch(function (err) {
          console.log('ERROR:', err);
        });

      pgp.end();

      break;

    default:
      console.log('dbms ' + dbms + ' is not supported.');
  }

});

module.exports = router;


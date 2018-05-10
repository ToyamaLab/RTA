const express = require('express');
const router = express.Router();
const util = require('util');
const fs = require('fs-extra');
const path = require('path');
const constant = require('../../config/constant');
const helper = require('../../helper/helper');

router.get('/', function(req, res, next) {

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

  var sql = 'SELECT * FROM dbinfo';
  db.result(sql)
    .then(result => {
      res.render('ptl', {
        title: 'Express',
        rows: result.rows,
        fields: result.fields
      });
    })

    .catch(function (err) {
      console.log('ERROR:', err);
    });

  pgp.end();


});

router.get('/detail/:id/', function(req, res, next) {
  var pgp = require("pg-promise")(options);

  var options = {
      // initialization options;
  };

  let connection = {
      host     : 'spacia.db.ics.keio.ac.jp',
      user     : 'shu',
      password : 'shu',
      database : 'rta_databases'
  };

  var db = pgp(connection);

  var sql = 'SELECT * FROM dbinfo d INNER JOIN column_info c ON d.id = c.table_id WHERE d.id = $1';
  db.result(sql, [req.params.id])
    .then(result => {

      res.render('detail', {
        title: 'Table Scheme',
        tbname: result.rows[0].table_name,
        rows: result.rows,
        fields: result.fields
      });
    })

    .catch(function (err) {
      console.log('ERROR:', err);
    });

  pgp.end();
});

router.get('/download/:id/', function(req, res, next) {
  var pgp = require("pg-promise")(options);
  var rtadir = constant.rtadir;

  var options = {
      // initialization options;
  };

  let connection = {
      host     : 'spacia.db.ics.keio.ac.jp',
      user     : 'shu',
      password : 'shu',
      database : 'rta_databases'
  };

  var db = pgp(connection);

  var sql = 'SELECT * FROM dbinfo d WHERE d.id = $1';
  db.one(sql, req.params.id)
    .then(result => {
      var obj = {};
      // console.log(util.inspect(result));
      obj["dbms"] = result.dbms;
      obj["host"] = result.host;
      obj["user"] = result.user_name;
      obj["password"] = result.password;
      obj["dbname"] = result.db_name;
      obj["tbname"] = result.table_name;
      obj["accessname"] = result.access_name;
      obj["accessmethod"] = result.access_method;
      obj["description"] = result.description;
      var json = JSON.stringify(obj, undefined, 2);
      var crypted = helper.encrypt(json);

      fs.writeFile(path.join(rtadir, result.access_name + '.rta'), crypted);
      fs.writeFile(path.join(rtadir, result.access_name + '.json'), json);
    })

    .catch(function (err) {
      console.log('ERROR:', err);
    });

  pgp.end();
});

module.exports = router;

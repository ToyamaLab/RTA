const express = require('express');
const router = express.Router();
const fs = require('fs-extra');
const path = require('path');
const constant = require('../../config/constant');
const helper = require('../../helper/helper');

router.get('/', function(req, res, next) {

  var rtadir = constant.rtadir;

  fs.mkdirsSync(rtadir);

  fs.readdir(rtadir, function(err, files) {
    if (err) throw (err);
    var jsonList = [];
    files.filter(function(file) {
        return fs.statSync(path.join(rtadir, file)).isFile() && /.*\.rta$/.test(file);
    }).forEach(function (file) {
      var crypted = fs.readFileSync(path.join(rtadir, file), 'utf8');
      var decrypted = helper.decrypt(crypted);
      var json = JSON.parse(decrypted, 'utf8');
      jsonList.push(json);
    });

    res.render('dashboard', {
      title: 'Express',
      rtafiles: jsonList
    });

  });

});

module.exports = router;

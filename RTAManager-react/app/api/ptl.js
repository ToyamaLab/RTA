const db = require('../helpers/pg');

exports.getDbInfo = function(req, res, next) {
  db.query(
    'SELECT * FROM dbinfo',
    [],
    function(err, result){
      if (err){
        console.log(err);
        return res.status(400).send('エラーが発生しました');
      }
      return res.send(JSON.stringify(result), undefined, 2);
    }
  );
}

exports.getColInfo = function(req, res, next) {
  db.query(
    'SELECT * FROM dbinfo d INNER JOIN column_info c ON d.id = c.table_id WHERE d.id = $1',
    [req.params.id],
    function(err, result){
      if (err){
        console.log(err);
        return res.status(400).send('エラーが発生しました');
      }
        console.log(result);
      return res.send(JSON.stringify(result), undefined, 2);
    }
  );
}

exports.getContent = function(req, res, next) {
  db.query(
    'SELECT * FROM ' + req.params.tbname + ' LIMIT 50',
    [],
    function(err, result){
      if (err){
        console.log(err);
        return res.status(400).send('エラーが発生しました');
      }
        console.log(result);
      return res.send(JSON.stringify(result), undefined, 2);
    }
  );
}


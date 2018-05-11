const { Pool } = require('pg');
const pool = new Pool({
  host: '131.113.101.113',
  user: 'shu',
  password: 'shu',
  database: 'rta_databases',
});

/**
 * poatgreSQLに接続してSQLを実行する
 * @param sql 実行したいSQL
 * @param values SQLに指定するパラメータ
 * @param callback SQL実行後、処理するイベント
 */

exports.query = function(sql, values, callback) {
  console.log(sql, values);

  pool.connect(function(err, conn, done) {
    if (err) {
      return callback(err);
    }
    try {
      done();
      conn.query(sql, values, function(err, res) {
        if (err) {
          callback(err);
        } else {
          callback(null, res);
        }
      });
    } catch(e) {
      callback(e);
    }
  });
};

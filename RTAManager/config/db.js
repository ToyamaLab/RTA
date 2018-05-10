let mysql      = require('mysql');
let connection = mysql.createConnection({
    host     : 'localhost',
    user     : 'root',
    password : 'root',
    database : 'rta_databases'
});

connection.connect(function(err) {
  if(err) {
    console.error('error connecting' + err.stack);
  }
  console.log('connected as id ' + connection.threadId);
}
);

(function(){
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

  connection.query('SELECT * FROM stocks limit 100;', function(err, rows, fields) {
    if (err) {
      throw err;
      console.log(err);
    } else {
      console.log('No Error');
    }
    var length = rows.length;
    if(length > 0){
      let table = document.querySelectorAll('[elc-js=SelectResultTbl]')[0]
      let tableHead = table.querySelectorAll('thead')[0];
      let tableFoot = table.querySelectorAll('tfoot')[0];
      let tableBody = table.querySelectorAll('tbody')[0];
      tableBody.innerHTML = '';

        tableHead.insertAdjacentHTML('beforeend','<tr></tr>');
        for(var i = 0; i < fields.length; i++) {
          tableHead.lastElementChild.insertAdjacentHTML('beforeend','<th>' + fields[i].name + '</th>');
        }

        tableFoot.insertAdjacentHTML('beforeend','<tr></tr>');
        for(var i = 0; i < fields.length; i++) {
          tableFoot.lastElementChild.insertAdjacentHTML('beforeend','<th>' + fields[i].name + '</th>');
        }

      for(var i = 0; i < length; i++) {
        tableBody.insertAdjacentHTML('beforeend','<tr></tr>');
        let tableTr = tableBody.lastElementChild;

        for(var j = 0; j < fields.length; j++) {
          // console.log(fields);
          var fieldName = fields[j].name;
          tableTr.insertAdjacentHTML('beforeend','<td>' + rows[i][fieldName] + '</td>');
        }
      }
    }
  });

  connection.end();
}());



(function(){
    let clickEventName = 'click';
    let clickButtonEventHandler = function(){
      $('#dataTable').DataTable();
    };
    document.querySelectorAll('[elc-js=ExecuteQUeryBtn]')[0].addEventListener(clickEventName, clickButtonEventHandler);
}());


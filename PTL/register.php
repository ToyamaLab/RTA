<?php 

session_start();

include 'header.php';

?>

<div class="container">
  <div class="row">
    <div id = col class="col-lg-6 col-lg-offset-3">

      <?php if (!empty($_SESSION['error'])) :
        echo "<div class=\"alert bg-danger text-danger\">" . $_SESSION['error'] . "</div>";
        unset($_SESSION['error']);
        endif;
      ?>

      <h1 class="page-header">Table Registration</h1>

      <div class="form-group">
          <label>Table Type</label>
        <select id="table_type_select" onchange="change_form()" name="table_type" class="form-control">
          <option value='' disabled selected style='display:none;'>Please select</option>
          <option value="dbms">DBMS</option>
          <option value="lod">LOD</option>
          <option value="csv" disabled>CSV</option>
        </select>
      </div>

      <script>
        var current_table_type;
        function change_form(){
          var table_type = document.getElementById("table_type_select").value;
          switch(table_type){
            case "dbms":
              var dbms_html = (function(){/*
                <form id="registerForm" action="./register_table.php" method="post">

                <div class="form-group">
                  <label>DBMS</label>
                <select name="dbms" class="form-control">
                  <option value='' disabled selected style='display:none;'>Please select</option>
                  <option value="mysql">MySQL</option>
                  <option value="pgsql">PostgreSQL</option>
                  <option value="sqlite" disabled>Sqlite</option>
                  <option value="oracle" disabled>Oracle</option>
                </select>
                </div>

                <div class="form-group">
                  <label>Host Name or IP Address</label>
                  <input type="text" name="host" size="40" class="form-control">
                </div>

                <div class="form-group">
                  <label>User</label>
                  <input type="text" name="user_name" size="40" class="form-control">
                </div>

                <div class="form-group">
                  <label>Password</label>
                  <input type="password" name="password" size="40" class="form-control">
                  <!-- <input type="text" name="password" size="40" class="form-control"> -->
                </div>

                <div class="form-group">
                  <label>Database Name</label>
                  <input type="text" name="db_name" size="40" class="form-control">
                </div>

                <div class="form-group">
                  <label>Table Names</label>
                  <input type="text" name="table_names" size="40" class="form-control" placeholder="table1,table2,...">
                </div>

                <div class="form-group">
                  <label>Access Method</label>
                  <input type="text" name="access_method" size="40" class="form-control" readonly value="direct">
                  <!-- <select name="access_method" class="form-control">
                  <option value='' disabled selected style='display:none;'>Please select</option>
                  <option value="direct">Directly Access</option>
                   <option value="api" disabled>API</option>-->

                </select>
                </div>

                <div class="form-group">
                  <input type="submit" value="Register" class="btn btn-primary col-lg-6 col-lg-offset-3">
                </div>

                </form>
              */}).toString().match(/(?:\/\*(?:[\s\S]*?)\*\/)/).pop().replace(/^\/\*/, "").replace(/\*\/$/, "");
              $("#registerForm").remove();
              $("#col").append(dbms_html);
              break;

            case "lod":
              var lod_html = (function(){/*
                <form id="registerForm" action="./register_sparql.php" method="post">
                  <div class="form-group">
                    <label>Sparql Endpoint</label>
                  <select name="sparql_endpoint" class="form-control">
                    <option value='' disabled selected style='display:none;'>Please select</option>
                    <option value="http://dbpedia.org/sparql">DBpedia</option>
                    <option value="http://ja.dbpedia.org/sparql">DBpedia_Japanese</option>
                  </select>
                  </div>

                  <div class="form-group">
                    <label>Sparql Query</label>
                  <textarea name="sparql_query" class="form-control" rows="10"></textarea>
                  </div>

                  <div class="form-group">
                    <input type="submit" value="Register" class="btn btn-primary col-lg-6 col-lg-offset-3">
                  </div>
                </form>
              */}).toString().match(/(?:\/\*(?:[\s\S]*?)\*\/)/).pop().replace(/^\/\*/, "").replace(/\*\/$/, "");
              $("#registerForm").remove();
              $("#col").append(lod_html);
              break;

            case "csv":
              break;
            }
          }

      </script>


    </div>
  </div>
</div>

<? include 'footer.php'; ?>

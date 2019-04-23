<?php 

session_start();

include 'header.php';

?>

<div class="container">
  <div class="row">
    <div class="col-lg-6 col-lg-offset-3">

      <?php if (!empty($_SESSION['error'])) :
        echo "<div class=\"alert bg-danger text-danger\">" . $_SESSION['error'] . "</div>";
        unset($_SESSION['error']);
        endif;
      ?>

      <h1 class="page-header">Table Registration</h1>

    	<form id="registerForm" action="./register_table2.php" method="post" enctype="multipart/form-data">

      	<div class="form-group">
        	<label>Host Name or IP Address</label>
          <input type="text" name="host" size="40" class="form-control">
      	</div>

      	<div class="form-group">
        	<label>Replicate User</label>
          <input type="text" name="user_name" size="40" class="form-control">
      	</div>

      	<div class="form-group">
        	<label>Replicate User Password</label>
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
          <label>Damp File</label>
          <input type="file" name="fname">
        </div>
      	<div class="form-group">
      	  <input type="submit" value="Register" class="btn btn-primary col-lg-6 col-lg-offset-3">
      	</div>

    	</form>


    </div>
  </div>
</div>

<? include 'footer.php'; ?>

<?php
//設定ファイル読み込み
require_once('config.php');
require_once('functions.php');

session_start();

if($_SERVER["REQUEST_METHOD"] != "POST") {
	$_SESSION['error'] = 'Please register again.';
	header('Location: ./register.php');
	exit;
}

// ini_set( 'display_errors', 1 );

include 'header.php';
print<<<HEAD
<div class="container">
    <div class="row">
      <div class="col-lg-8 col-lg-offset-2">
HEAD;

//取得した値の代入
$dbms = $_POST['dbms'];
$host = $_POST['host'];
$user_name = $_POST['user_name'];
$password = $_POST['password'];
$db_name = $_POST['db_name'];
$table_names = $_POST['table_names'];
$access_name = $_POST['access_name'];
$access_method = $_POST['access_method'];

$message = '';

$table_names = str_replace(array(" ", "　"), "", $table_names);
$tables_array = explode(',', $table_names);

// アクセス先判定
if (($con = connectDB($dbms, $host, $user_name, $password, $db_name)) == -1) {
	$message = ('Wrong access information or no access priviledge.');
} else {

	$err = false;

	foreach($tables_array as $table_name) {
		$sql = "SELECT * FROM ".$table_name." LIMIT 1";
		$stmt = $con->prepare($sql);
		// 利用可能カラム判定
		if ($stmt->execute()) {
			// DBへの接続
			$con2 = connectLocalDB();

			if ($dbms == 'pgsql') $dbms = 'postgresql';

			$sql2 = 'INSERT INTO dbinfo (dbms, host, user_name, password, db_name, table_name, access_method) VALUES (?, ?, ?, ?, ?, ?, ?)';
			$stmt2 = $con2->prepare($sql2);
			if ($stmt2->execute(array($dbms, $host, $user_name, $password, $db_name, $table_name, $access_method))) {

				$sql2 = "SELECT lastval()";
				$stmt2 = $con2->prepare($sql2);
				$stmt2->execute();
				$row = $stmt2->fetch(PDO::FETCH_ASSOC);
				foreach(range(0, $stmt->columnCount() - 1) as $column_index) {
			 		$meta = $stmt->getColumnMeta($column_index);

			 		$sql2 = 'INSERT INTO column_info (table_id, number, type, name) VALUES (?, ?, ?, ?)';
					$stmt2 = $con2->prepare($sql2);
					$stmt2->execute(array($row['lastval'], $column_index, $meta['native_type'], $meta['name']));
				}
			} else {
				$err = true;
				$message = 'Failure While adding table ' . $table_name . '\'s info.';
			}
		} else {
			$err = true;
			$message = 'No columns available for table '. $table_name.$stmt->errorCode(). ' '.$stmt->errorInfo();
		}
	}

	# カラムの登録画面
	if(!$err) :
		print "<h1 class=\"page-header\">Registration of Access Table Information</h1>";
		print "<form id=\"addInfoForm\" action=\"./register_column.php\" method=\"post\">";
		$index = 0;
		foreach($tables_array as $table_name) :
?>
		<h2><?= $table_name ?></h2>

        <div class="form-group">
        	<label>Access Name</label>
			<input type="text" id="access_name" name="tables[<?= $index ?>][access_name]" size="40" class="form-control" value="<?= $table_name ?>">
        </div>
        <div class="form-group">
        	<label>Table Description</label>
			<input type="text" name="tables[<?= $index ?>][table_description]" size="40" class="form-control">
        </div>

	    <table class="table">
		    <thead>
		    	<tr>
			        <th>Column Name</th>
	            	<th>Data Type</th>
			        <th>Column Description</th>
		    	</tr>
		    </thead>
		    <tbody>

		    <?php 
		    $sql3 = "SELECT * from dbinfo d INNER JOIN column_info c ON d.id = c.table_id WHERE d.table_name = ? ORDER BY number";
		    $stmt3 = $con2->prepare($sql3);
		    $stmt3->execute(array($table_name));

			while($row = $stmt3->fetch(PDO::FETCH_ASSOC)):
				if ($row['number'] == 0) :
			?>
			<input type="hidden" name="tables[<?= $index ?>][table_id]" value="<?= $row['table_id'] ?>">
			<?php endif; ?>
		    <tr>
		        <td><?= $row['name'] ?></td>
            	<td><?= $row['type'] ?></td>
		        <td><input type="text" name="tables[<?= $index ?>][column][<?= $row['number'] ?>][description]" size="40" class="form-control"></td>
		        <input type="hidden" name="tables[<?= $index ?>][column][<?= $row['number'] ?>][number]" value="<?= $row['number'] ?>">
		    </tr>
        	<?php
        	endwhile;
			$index++;
        	?>
		    </tbody>
		</table>
		<?php endforeach; ?>
	    <div class="form-group">
      	  	<input type="submit" value="Register" class="btn btn-primary col-lg-6 col-lg-offset-3">
      	</div>
		</form>

<?php
	else:
		print "<p>" . $message . "</p>";
	endif;

}
?>

<script>
$(function(){
    $(window).on('beforeunload', function() {
        return 'Edit is not completed. Move to another page?';
    });
    $("input[type=submit]").click(function() {
        $(window).off('beforeunload');
    });
});
</script>

<?php include 'footer.php'; ?>

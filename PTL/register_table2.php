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
$dbms = 'mysql';
$host = $_POST['host'];
$user_name = $_POST['user_name'];
$password = $_POST['password'];
$db_name = $_POST['db_name'];
$table_names = $_POST['table_names'];
$access_method = 'WebService';
$port = 9000;

$host_cloud = 'zoe.db.ics.keio.ac.jp';
$user_cloud = 'root';
$password_cloud = 'Ssql.5591';

move_uploaded_file($_FILES["fname"]["tmp_name"],'files/'.$_FILES["fname"]["name"]);
$tempfile = 'files/'.$_FILES["fname"]["name"];

#mysql -h zoe.db.ics.keio.ac.jp -u root -p -P 9000 sample2 < /home_allex/toyama/kosaka/dump.sql
$cmd = "mysql "." -h ".$host_cloud." -u ".$user_cloud ." --password=".$password_cloud." -P 9000 ".$db_name." < ".$tempfile;
shell_exec($cmd);

$message = '';

$table_names = str_replace(array(" ", "　"), "", $table_names);
$tables_array = explode(',', $table_names);

// Cloud DBへの接続
if (($con = connectDB2($dbms, $host_cloud, $user_cloud, $password_cloud, $db_name,$port)) == -1) {
	$message = ('Wrong access information or no access priviledge.');
	echo $message;
} else {
	$err = false;
	foreach($tables_array as $table_name) {
		$sql = "SELECT * FROM ".$table_name." LIMIT 1";
		$stmt = $con->prepare($sql);
		// 利用可能カラム判定
		if ($stmt->execute()) {
			// Local DB(テーブルの登録)への接続
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

	# MASTER_LOG_FILE,POSをmysqldumpから抽出
	$fp = fopen($tempfile, "r");
	while (!feof($fp)) {
		$line = fgets($fp);
  		if(preg_match("/CHANGE MASTER TO/",$line)){
  			#echo '<br>Match:'.$line;
			$arr = explode(" ",$line);
  			$count = count($arr);
  			for($i = 0; $i < $count; $i++){
  				if(preg_match("/MASTER_LOG_FILE/",$arr[$i])){
					#echo '<br>Match:'.$arr[$i];
					$arr2 = explode("'",$arr[$i]);
  					$MASTER_LOG_FILE = $arr2[1];
					#echo '<br>'.$LOG_FILE;
  				}
  				if(preg_match("/MASTER_LOG_POS/",$arr[$i])){
					#echo '<br>Match:'.$arr[$i];
  					$arr2 = explode("=",$arr[$i]);
  					$MASTER_LOG_POS = substr($arr2[1],0,-2);
					#echo '<br>'.$LOG_POS;
  				}
  			}
  		}
	}
	fclose($fp);
	$change_master_query = 
	"CHANGE MASTER TO MASTER_HOST='".$host."',MASTER_USER='".$user_name."',MASTER_PASSWORD='".$password."',MASTER_PORT=9000,MASTER_LOG_FILE='".$MASTER_LOG_FILE."',MASTER_LOG_POS=".$MASTER_LOG_POS.",MASTER_CONNECT_RETRY=10 for channel 'master2'";
	$cmd2 = "mysql "." -h ".$host_cloud." -u ".$user_cloud ." --password=".$password_cloud." -P 9000 -e \"".$change_master_query."\"";
	shell_exec($cmd2);
	$cmd3 = "mysql "." -h ".$host_cloud." -u ".$user_cloud ." --password=".$password_cloud."-P 9000 -e \"start slave for channel 'master2'\"";
	shell_exec($cmd3);
	#echo '<br>'.$cmd2;
	#echo '<br>'.$cmd3;

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

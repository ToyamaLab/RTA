<?php
require_once('config.php');
require_once('functions.php');

session_start();

if($_SERVER["REQUEST_METHOD"] != "POST"){
	$_SESSION['error'] = 'Please register again';
	header('Location: ./register.php');
	exit;
}

include 'header.php';
print<<<HEAD
<div class="container">
    <div class="row">
      <div class="col-lg-6 col-lg-offset-3">
HEAD;

$con = pgConnectLocalDB();
$sparql_endpoint = $_POST['sparql_endpoint'];
$sparql_query = $_POST['sparql_query'];
$access_name = $_POST['access_name'];
$table_description = $_POST['table_description'];


$sql = "INSERT INTO sparql(sparql_query, sparql_endpoint, sparql_access_name, sparql_table_description) VALUES($1,$2,$3,$4) RETURNING sparql_id";
$result = pg_prepare($con, 'my_query', $sql);
$result = pg_execute($con, 'my_query', array($sparql_query, $sparql_endpoint, $access_name, $table_description));
$row = pg_fetch_assoc($result,0);


for ($i = 0; $i < count($_POST['data_type']); $i++) {
	pg_insert($con, 'sparql_column', array("sparql_column_name" => $_POST['column_name'][$i],
	"sparql_column_datatype" => $_POST['data_type'][$i],
	"sparql_column_description" => $_POST['column_description'][$i],
	"sparql_id" => $row['sparql_id']));
}

$sql2 = "INSERT INTO dbinfo(access_name, access_method, description) VALUES($1, $2, $3)";
$result = pg_prepare($con, 'my_query2', $sql2);
$result = pg_execute($con, 'my_query2', array($access_name, 'linked', $table_description));

?>


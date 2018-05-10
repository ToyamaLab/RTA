<?php
require_once('config.php');
require_once('functions.php');
ini_set ('display_errors', 1);
// DBへの接続
$dbh = connectLocalDB();

$t_id = $_GET["t_id"];
$sql = 'SELECT * FROM dbinfo WHERE id = ?';
$stmt = $dbh->prepare($sql);
$stmt->execute(array($t_id));
$row = $stmt->fetch(PDO::FETCH_ASSOC);

$dbms = $row['dbms'] == 'postgresql' ? 'pgsql' : $row['dbms'];
$con = connectDB($dbms, $row['host'], $row["user_name"], $row["password"], $row["db_name"]);
$sql2 = 'SELECT * FROM ' . $row['table_name'] . ' LIMIT 50000';
$stmt2 = $con->prepare($sql2);
$stmt2->execute(array());
$rows = $stmt2->fetchAll(PDO::FETCH_ASSOC);
echo json_encode(array('data' => $rows), JSON_PRETTY_PRINT);
?>

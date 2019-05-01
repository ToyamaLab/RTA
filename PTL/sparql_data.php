<?php
require_once('config.php');
require_once('functions.php');
require_once('sparqllib.php');
// DBへの接続
$dbh = connectLocalDB();

$t_id = $_GET["t_id"];
$sql1 = 'SELECT s.sparql_id, s.sparql_endpoint, s.sparql_query 
FROM sparql s JOIN dbinfo d ON s.sparql_access_name = d.access_name WHERE d.id = ?';
$stmt1 = $dbh->prepare($sql1);
$stmt1->execute(array($t_id));
$row1 = $stmt1->fetch(PDO::FETCH_ASSOC);
// echo "$row1= ".$row1;

$sql2 = 'SELECT sparql_column_name FROM sparql_column WHERE sparql_id = ?';
$stmt2 = $dbh->prepare($sql2);
$stmt2->execute(array($row1['sparql_id']));
$rows2 = $stmt2->fetchAll(PDO::FETCH_ASSOC);
// echo "$rows2= ".$rows2;

$sparql_endpoint = $row1['sparql_endpoint'];
$sparql_query = $row1['sparql_query'];
$sparql_data = sparql_get($sparql_endpoint, $sparql_query);



for ($i = 0; $i < count($sparql_data); $i++) {
	for($j = 0; $j < count($rows2); $j++){
		$data[$i][$rows2[$j]['sparql_column_name']] = $sparql_data[$i][$rows2[$j]['sparql_column_name']];
	}
}

echo json_encode(array('data' => $data), JSON_PRETTY_PRINT);

// echo($sparql_data[0]['country']);

// echo json_encode(array('data' => $sparql_data), JSON_PRETTY_PRINT);
?>

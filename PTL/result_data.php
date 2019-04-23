<?php
require_once('config.php');
require_once('functions.php');
sleep(2);
$con = connectLocalDB();
$stmt = $con->prepare($_GET['query']);
$stmt->execute(array());
$rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
echo json_encode(array('data' => $rows), JSON_PRETTY_PRINT);
?>

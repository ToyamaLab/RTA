<?php

//設定ファイル読み込み
require_once('config.php');
require_once('functions.php');

// DBへの接続
$con_local = connectLocalDB();

print $column = $_POST['column'];
print $t_id = $_POST['t_id'];
print $value = $_POST['value'];

$sql = 'UPDATE column_info SET description = ? WHERE table_id = ? AND name = ?';
$stmt = $con_local->prepare($sql);
$stmt->execute(array($value, $t_id, $column));

?>
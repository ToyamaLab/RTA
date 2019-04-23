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

include 'header.php';
print<<<HEAD
<div class="container">
    <div class="row">
      <div class="col-lg-6 col-lg-offset-3">
HEAD;

$con = connectLocalDB();

foreach ($_POST['tables'] as $table) {
    $table_id = $table['table_id'];
    $access_name = $table['access_name'];
    $table_description = $table['table_description'];
    echo $table_description;

    $sql = "UPDATE dbinfo SET access_name = ?, description = ? WHERE id = ?";
    $stmt = $con->prepare($sql);
    $stmt = $stmt->execute(array($access_name, $table_description, $table_id));

    foreach ($table['column'] as $column) {
        $column_description = $column['description'];
        $number = $column['number'];

        $sql2 = "UPDATE column_info SET description = ? WHERE table_id = ? AND number = ?";
        $stmt2 = $con->prepare($sql2);
        $stmt2 = $stmt2->execute(array($column_description, $table_id, $number));
    }
}


?>

<?php include 'footer.php'; ?>

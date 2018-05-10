<?php
//設定ファイル読み込み
require_once('config.php');
require_once('functions.php');

// DBへの接続
$dbh = connectLocalDB();

$sql = 'SELECT p.name, p.alias, d.id, d.table_name, d.description  FROM dbinfo d INNER JOIN publishers p ON d.pub_id = p.id ORDER BY d.pub_id';
$stmt = $dbh->prepare($sql);
$stmt->execute(array());

?>

<?php include 'header.php'; ?>

<div class="container">
    <div class="row">
        <div class="col-lg-10 col-lg-offset-1">
            <h1 class="page-header">Data Catalog</h1>
            <table class="table table-striped" id="list-table">
                <thead>
                    <tr>
                        <th>Publisher</th>
                        <th>Alias</th>
                        <th>Access Name</th>
                        <th>Description</th>
                        <th>Detail</th>
                        <th>Content</th>
                    </tr>
                </thead>
                <tbody>
               <?php while($row = $stmt->fetch(PDO::FETCH_ASSOC)): ?>
                <tr>
                    <td><?= $row['name'] ?></td>
                    <td><?= $row['alias'] ?></td>
                    <td>#<?= $row['table_name'] ?></td>
                    <td><?= $row['description'] ?></td>
                    <td><a href="detail.php?t_id=<?= $row['id'] ?>">Detail</a></td>
                    <td><a href="content.php?t_id=<?= $row['id'] ?>">Content</a></td>
                </tr>
                <?php endwhile; ?>
                </tbody>
            </table>
        </div>
    </div>
</div>

<script>
jQuery(function($){
    $("#list-table").DataTable();
});
</script>

<?php include 'footer.php'; ?>

<?php
ini_set('display_errors',1);

require_once('config.php');
require_once('functions.php');

// DBへの接続
$dbh = connectLocalDB();

$t_id = $_GET["t_id"];
$sql = 'SELECT d.table_name, c.* FROM column_info c INNER JOIN dbinfo d ON c.table_id = d.id WHERE c.table_id = ? ORDER BY c.number';
# $sql = 'SELECT * FROM column_info WHERE table_id = ? ORDER BY number';
$stmt = $dbh->prepare($sql);
$stmt->execute(array($t_id));
$columns = $stmt->fetchAll(PDO::FETCH_ASSOC);

?>

<?php include 'header.php'; ?>

<div class="container">
    <div class="row">
        <div class="col-lg-12">
        <h1 class="page-header">#<? echo $columns[0]['table_name']; ?></h1>
            <table class="table table-striped" id="content-table">
                <thead>
                    <tr>
                        <?php foreach($columns as $c): ?>
                        <th><?php echo $c['name'] ?></th>
                        <?php endforeach; ?>
                    </tr>
                </thead>
            </table>
        </div>
    </div>
</div>

<script>
jQuery(function($){
    $("#content-table").DataTable({
        scrollX: true,
        ajax: 'data.php?t_id=<?php echo $_GET['t_id'] ?>',
        columns: [
            <?php foreach($columns as $c): ?>
            { "data": "<?php echo $c['name']; ?>" },
            <?php endforeach; ?>
        ]
    });
});
</script>

<?php include 'footer.php'; ?>


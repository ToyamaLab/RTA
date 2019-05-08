<?php
require_once('config.php');
require_once('functions.php');
require_once('sparqllib.php');

$dbh = connectLocalDB();

$t_id = $_GET["t_id"];
$sql1 = 'SELECT access_name, access_method FROM dbinfo WHERE id = ?';
$stmt1 = $dbh->prepare($sql1);
$stmt1->execute(array($t_id));
$row1 = $stmt1->fetch(PDO::FETCH_ASSOC);
$access_method = $row1['access_method'];
if($access_method == 'linked'){
    $access_name = $row1['access_name'];
    $sql2 = 'SELECT sparql_column_name FROM sparql_column sc JOIN sparql s ON sc.sparql_id = s.sparql_id where s.sparql_access_name = ?';
    $stmt2 = $dbh->prepare($sql2);
    $stmt2->execute(array($access_name));
    $columns = $stmt2->fetchAll(PDO::FETCH_ASSOC);
}else{
    $sql2 = 'SELECT d.access_name, c.* FROM column_info c INNER JOIN dbinfo d ON c.table_id = d.id WHERE c.table_id = ? ORDER BY c.number';
    $stmt2 = $dbh->prepare($sql2);
    $stmt2->execute(array($t_id));
    $columns = $stmt2->fetchAll(PDO::FETCH_ASSOC);
    $access_name = $columns[0]['access_name'];
}




?>

<?php include 'header.php'; ?>

<div class="container">
    <div class="row">
        <div class="col-lg-12">
        <h1 class="page-header">#<? echo $access_name ?></h1>
            <table class="table table-striped" id="content-table">
                <thead>
                    <tr>
                        <?php if($access_method == 'linked'):?>
                            <?php foreach($columns as $c): ?>
                            <th><?php echo $c['sparql_column_name'] ?></th>
                            <?php endforeach; ?>
                        <?php else:?>
                            <?php foreach($columns as $c): ?>
                            <th><?php echo $c['name'] ?></th>
                            <?php endforeach; ?>
                        <?php endif; ?>
                    </tr>
                </thead>
            </table>
        </div>
    </div>
</div>

<?php if($access_method =='linked'): ?>
    <script>
        jQuery(function($){
            $("#content-table").DataTable({
                scrollX: true,
                ajax: 'sparql_data.php?t_id=<?php echo $_GET['t_id'] ?>',
                columns: [
                    <?php foreach($columns as $c): ?>
                    { "data": "<?php echo $c['sparql_column_name']; ?>" },
                    <?php endforeach; ?>
                ]
            });
        });
    </script>
<?php else:?>
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
<?php endif; ?>


<?php include 'footer.php'; ?>


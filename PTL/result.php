<?php
require_once('config.php');
require_once('functions.php');

# require_once('./rta/rta.php');
# rtaSetConfig("./config.rta");
# $resultQuery = rtaExec($_POST['query']);
# 
# $con = new PDO('pgsql:host=localhost;dbname=tmp', 'shu', 'shu');
# 
# $stmt = $con->prepare($resultQuery);
# $stmt = $con->prepare('select * from result_171215001828');
# $stmt->execute(array());
# $columns = $stmt->fetchAll(PDO::FETCH_ASSOC);

$con = connectLocalDB();
$sql = str_replace('#', '', $_POST['query']);
$stmt = $con->prepare($sql);
$stmt->execute(array());
$count = $stmt->columnCount();
?>

<?php include 'header.php'; ?>

<div class="container">
    <div class="row">
        <div class="col-lg-12">
            <h1>Result</h1>
            <table class="table table-striped" id="result-table">
                <thead>
                    <tr>
                    <?php
                        if ($count != 0) {
                            foreach(range(0, $count - 1) as $column_index):
                                $meta = $stmt->getColumnMeta($column_index);
                                echo "<th>".$meta['name']."</th>";
                            endforeach;
                        } else {
                            echo "結果を表示できません。クエリを再確認してください。";
                        }
                    ?>
                    </tr>
                </thead>
                <tbody>
                </tbody>
            </table>
        </div>
    </div>
</div>

<script>
$(document).ready(function(){
    $("#result-table").DataTable({
        scrollX: true,
         ajax: "result_data.php?query=<?php echo str_replace("\r\n", ' ', $sql) ?>",
            columns: [
                <?php
                    foreach(range(0, $count - 1) as $column_index):
                        $meta = $stmt->getColumnMeta($column_index);
                        echo "{ \"data\": \"".$meta['name']."\" },";
                    endforeach;
                ?>
                ]
    });
});
</script>

<?php include 'footer.php'; ?>


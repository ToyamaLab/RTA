<?php

//設定ファイル読み込み
require_once('config.php');
require_once('functions.php');

// DBへの接続
$con_local = connectLocalDB();

$t_id = $_GET['t_id'];

$sql1 = 'SELECT access_name, access_method FROM dbinfo WHERE id = ?';
$stmt1 = $con_local->prepare($sql1);
$stmt1->execute(array($t_id));
$row1 = $stmt1->fetch(PDO::FETCH_ASSOC);
$access_name = $row1['access_name'];
$access_method = $row1['access_method'];
if($access_method == 'linked'){
    $sql2 = 'SELECT sparql_column_name, sparql_column_datatype, sparql_column_description FROM sparql_column sc JOIN sparql s ON sc.sparql_id = s.sparql_id where s.sparql_access_name = ?';
    $stmt2 = $con_local->prepare($sql2);
    $stmt2->execute(array($access_name));
}else{
    $sql2 = 'SELECT * FROM column_info WHERE table_id = ? ORDER BY number';
    $stmt2 = $con_local->prepare($sql2);
    $stmt2->execute(array($t_id));
}


?>

<?php include 'header.php'; ?>

<div class="container">
    <div class="row">
        <div class="col-lg-10 col-lg-offset-1">
            <h1 class="page-header">#<?= $access_name ?></h1>
            <!-- <h1 class="page-header"><?= $sql2 ?><h1> -->
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Column Name</th>
                        <th>Data Type</th>
                        <th>Description</th>
                    </tr>
                </thead>
                <tbody>
                    <?php if($access_method == 'linked'):
                        while ($row2 = $stmt2->fetch(PDO::FETCH_ASSOC)): ?>
                            <tr>
                                <td><?= $row2['sparql_column_name'] ?></td>
                                <td><?= $row2['sparql_column_datatype'] ?></td>
                                <td class="edit"><?= $row2['sparql_column_description'] ?></td>
                            </tr>
                        <?php endwhile;
                    else: ?>
                        <?php while($row2 = $stmt2->fetch(PDO::FETCH_ASSOC)): ?>
                            <tr>
                                <td><?= $row2['name'] ?></td>
                                <td><?= $row2['type'] ?></td>
                                <td class="edit"><?= $row2['description'] ?></td>
                            </tr>
                        <?php endwhile;
                endif; ?>

                </tbody>
            </table>
        </div>
    </div>
</div>

<script>
$('table.editable td.edit').click(function(){
    if(!$(this).hasClass('on')){
        $(this).addClass('on');
        var txt = $(this).text();
        $(this).html('<input type="text" value="'+txt+'" class="form-control" />');
        $(this).keypress(function(e) {
            if (e.which == 13) {
                $('td > input').blur();
            }
        });
        $('td > input').focus().blur(function(){
            update(this);
        });
    }
});

function update(element) {
	var inputVal = $(element).val();
	var column = $(element).parents('tr').children().eq(0).text();
	console.log(inputVal);
	console.log(column);
    if(inputVal===''){
        inputVal = element.defaultValue;
    };

    $.ajax({
		type: "POST",
		url: "column_update.php",
		data: { t_id: <?php echo $t_id ?>, column: column, value: inputVal }
		}).done(function( msg ) {
	});

    $(element).parent().removeClass('on').text(inputVal);
}
</script>

<?php include 'footer.php'; ?>

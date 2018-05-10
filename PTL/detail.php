<?php

//設定ファイル読み込み
require_once('config.php');
require_once('functions.php');

// DBへの接続
$con_local = connectLocalDB();

$t_id = $_GET['t_id'];

$sql1 = 'SELECT table_name FROM dbinfo WHERE id = ?';
$stmt1 = $con_local->prepare($sql1);
$stmt1->execute(array($t_id));

$sql2 = 'SELECT * FROM column_info WHERE table_id = ? ORDER BY number';
$stmt2 = $con_local->prepare($sql2);
$stmt2->execute(array($t_id));
?>

<?php include 'header.php'; ?>

<div class="container">
    <div class="row">
        <div class="col-lg-10 col-lg-offset-1">
            <h1 class="page-header">#<?= $stmt1->fetch(PDO::FETCH_ASSOC)['table_name'] ?></h1>
            <table class="table table-striped">
                <thead>
                    <tr>
                        <th>Column Name</th>
                        <th>Data Type</th>
                        <th>Description</th>
                    </tr>
                </thead>
                <tbody>
                    <?php while($row = $stmt2->fetch(PDO::FETCH_ASSOC)): ?>
                    <tr>
                        <td><?= $row['name'] ?></td>
                        <td><?= $row['type'] ?></td>
                        <td class="edit"><?= $row['description'] ?></td>
                    </tr>
                    <?php endwhile ?>
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

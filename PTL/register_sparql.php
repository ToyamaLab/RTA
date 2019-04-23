<?php
require_once('config.php');
require_once('functions.php');
require_once('sparqllib.php');


if($_SERVER["REQUEST_METHOD"] != "POST") {
    $_SESSION['error'] = 'Please register again.';
    header('Location: ./register.php');
    exit;
}

include 'header.php';

$sparql_endpoint = $_POST['sparql_endpoint'];
$sparql_query = $_POST['sparql_query'];
// print "sparql_endpoint: ".htmlspecialchars($sparql_endpoint);
// print "<br>";
// print "sparql_query: ".htmlspecialchars($sparql_query);


$data = sparql_get($sparql_endpoint, $sparql_query);
?>


<?php
if(!isset($data)):
	print "<p>Error: ".sparql_errno().": ".sparql_error()."</p>";
else:
?>

<div class="container">
    <div class="row">
        <div class="col-lg-12">
            <table class="table table-striped" id="content-table">
                <thead>
                    <tr>
                        <?php foreach($data->fields() as $field): ?>
                        <th><?php echo $field ?></th>
                        <?php endforeach; ?>
                    </tr>
                </thead>
                <tbody>
            		<?php
            		$i = 0; 
            		foreach($data as $rows):
            		?>
            			<tr>
            			<?php  
            			foreach($data->fields() as $field):
            			?>
	                		<td>
	                			<?=$rows[$field]?>
	                		</td>
            			<?php endforeach; ?>
            			</tr>
            		<?php
            			$i++;
            			if($i >= 5){
            				break;
            			}
            		endforeach;
            		?>
                </tbody>
            </table>

			<h1 class="page-header">Registration of Table Information</h1>
			<form id="addInfoForm" action="./register_sparql_column.php" method="post">


			<div class="form-group">
			    <label>Access Name</label>
			     	<input type="text" id="access_name" name="access_name" size="40" class="form-control">
			</div>

			<div class="form-group">
			    <label>Table Description</label>
			     	<input type="text" name="table_description" size="40" class="form-control">
			</div>

			<table class="table">
				<thead>
					<tr>
						<th>Column Name</th>
						<th>Data Type</th>
						<th>Column Description</th>
					</tr>
				</thead>
				<tbody>
				<?php
				$index = 0;
				foreach ($data->fields() as $field):
				?>
				<tr>
					<td><input type="hidden" name="column_name[<?=$index?>]" value="<?=$field?>"><?=$field?></td>
					<td><input type="text" name="data_type[<?=$index?>]" class="form-control"></td>
					<td><input type="text" name="column_description[<?=$index?>]" class="form-control"></td>
				</tr>
				<?php
				$index++;
				endforeach;
				?>
				</tbody>
			</table>

			<div class="form-group">
			   	<input type="submit" value="Register" class="btn btn-primary col-lg-6 col-lg-offset-3">
			</div>
			<input type="hidden" name ="sparql_endpoint" value="<?=$sparql_endpoint?>">
			<input type="hidden" name ="sparql_query" value="<?=$sparql_query?>">
			</form>
			<?php
			endif;
			?>
		</div>
	</div>
</div>

<script>
jQuery(function($){
    $("#content-table").DataTable({
        scrollX: true,
        columns: [
            <?php 
            foreach($data as $row):
            	foreach ($data->$fields as $field):?>
            		{ "data": "<?php echo $row['field']; ?>" },
            <?php endforeach; endforeach;?>
        ]
    });
});


$(function(){
    $(window).on('beforeunload', function() {
        return 'Edit is not completed. Move to another page?';
    });
    $("input[type=submit]").click(function() {
        $(window).off('beforeunload');
    });
});
</script>




<?php include 'footer.php'; ?>

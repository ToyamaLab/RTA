<?php

function connectDB($dbms, $host, $user, $password, $db_name) {
	$dsn = $dbms.':host='.$host.';dbname='.$db_name;
	try {
		return new PDO($dsn, $user, $password);
	} catch (PDOException $e) {
		return -1;
	}
}

function connectLocalDB() {
	try {
		return new PDO(DSN, DB_USER, DB_PASSWORD);
	} catch (PDOException $e) {
		echo $e->getMessage();
		exit;
	}
}
?>

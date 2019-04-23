<?php
header("Content-type: text/plain; charset=UTF-8");

// Ajaxリクエストの場合のみ処理する
if(isset($_SERVER['HTTP_X_REQUESTED_WITH'])
   && strtolower($_SERVER['HTTP_X_REQUESTED_WITH']) == 'xmlhttprequest') {

   if (isset($_POST['query'])) {
      require_once('./rta/rta.php');
      rtaSetConfig("./config.rta");
      $resultQuery = rtaExec($_POST['query']);
      echo $resultQuery;
  } else {
      echo 'The parameter of "request" is not found.';
  }
}
?>

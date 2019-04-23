<?php
  session_start();

  function rtaSetConfig($data){
    $_SESSION['num'] = 0;

    if(func_num_args() == 1){
      $_SESSION["config"] = func_get_arg(0);
    } else {
      switch (func_get_arg(0)) {
        case 'postgresql':
          $_SESSION["driver"] = func_get_arg(0);
          $_SESSION["db"] = func_get_arg(1);
          $_SESSION["host"] = func_get_arg(2);
          $_SESSION["user"] = func_get_arg(3);
          break;

        case 'sqlite':
          $_SESSION["driver"] = func_get_arg(0);
          $_SESSION["host"] = func_get_arg(1);
          $_SESSION["db"] = func_get_arg(2);
          $_SESSION["tmp_db"] = func_get_arg(3);
          break;

        default:
          # code...
          break;
      }
    }
  }

  function rtaExec($query){
    $jarFilePath = "./rta/rta.sh";
    # if($_SESSION["driver"] == "postgresql"){
    #   $rtaArgs = setDriver($_SESSION["driver"]).setDB($_SESSION["db"]).setHost($_SESSION["host"]).setUser($_SESSION["user"]);
    # } else if($_SESSION["driver"] == "sqlite"){
    #   $rtaArgs = setDriver($_SESSION["driver"]).setDB($_SESSION["db"]).setHost($_SESSION["host"]);
    # } else if(isset($_SESSION["config"])){
    #   $rtaArgs = setConfig($_SESSION["config"]);
    # }
  $rtaArgs = setConfig($_SESSION["config"]);

    echo $jarFilePath.$rtaArgs.setQuery($query);
    $result = shell_exec($jarFilePath.$rtaArgs.setQuery($query));
    echo $result;
    if ($result) {
        return $result;
    } else {
        return null;
    }
  }

  function setQuery($query){
    // "を'に変換
    $query = str_replace("\"", "'", $query);
    $query = '"'.$query.'"';
    return " -query ".$query;
  }

  function setDriver($driver){
    return " -driver ".$driver;
  }

  function setDB($db){
    return " -db ".$db;
  }

  function setTmpDB($db){
    return " -t_db ".$db;
  }

  function setHost($host){
    return " -h ".$host;
  }

  function setUser($user){
    return " -u ".$user;
  } 

  function setConfig($config){
    // "/"が含まれている場合
    if(strpos($config, "/") !== false){
      $config = substr($config, strrpos($config, "/") + 1);
    } 
    return " -c ".dirname(dirname(__FILE__))."/".$config;
  }

  function setValue($array){
    return " -ehtmlarg {".implode(",", $array)."}";
  }

  session_destroy();
?>

<?php

use TDengine\Connection;
use TDengine\Exception\TDengineException;

try {
    // instantiate
    $host = 'localhost';
    $port = 6030;
    $username = 'root';
    $password = 'taosdata';
    $dbname = null;
    $connection = new Connection($host, $port, $username, $password, $dbname);

    // connect
    $connection->connect();
} catch (TDengineException $e) {
    // throw exception
    throw $e;
}

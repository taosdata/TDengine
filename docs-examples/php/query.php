<?php

use TDengine\Connection;
use TDengine\Exception\TDengineException;

try {
    // instantiate
    $host = 'localhost';
    $port = 6030;
    $username = 'root';
    $password = 'taosdata';
    $dbname = 'power';
    $connection = new Connection($host, $port, $username, $password, $dbname);

    // connect
    $connection->connect();

    $resource = $connection->query('SELECT ts, current FROM meters LIMIT 2');
    var_dump($resource->fetch());
} catch (TDengineException $e) {
    // throw exception
    throw $e;
}

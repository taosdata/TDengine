<?php

use TDengine\Connection;
use TDengine\Exception\TDengineException;

try {
    // 实例化
    $host = 'localhost';
    $port = 6030;
    $username = 'root';
    $password = 'taosdata';
    $dbname = null;
    $connection = new Connection($host, $port, $username, $password, $dbname);

    // 连接
    $connection->connect();
} catch (TDengineException $e) {
    // 连接失败捕获异常
    throw $e;
}

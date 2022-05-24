<?php

use TDengine\Connection;
use TDengine\Exception\TDengineException;

try {
    // 实例化
    $host = 'localhost';
    $port = 6030;
    $username = 'root';
    $password = 'taosdata';
    $dbname = 'power';
    $connection = new Connection($host, $port, $username, $password, $dbname);

    // 连接
    $connection->connect();

    // 插入
    $connection->query('CREATE DATABASE if not exists power');
    $connection->query('CREATE STABLE if not exists meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)');
    $resource = $connection->query(<<<'SQL'
    INSERT INTO power.d1001 USING power.meters TAGS(Beijing.Chaoyang, 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS(Beijing.Chaoyang, 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS(Beijing.Haidian, 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS(Beijing.Haidian, 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)
    SQL);

    // 影响行数
    var_dump($resource->affectedRows());
} catch (TDengineException $e) {
    // 捕获异常
    throw $e;
}

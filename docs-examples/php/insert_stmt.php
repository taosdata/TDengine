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
    $stmt = $connection->prepare('INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)');

    // 设置表名和标签
    $stmt->setTableNameTags('d1001', [
        // 支持格式同参数绑定
        [TDengine\TSDB_DATA_TYPE_BINARY, 'Beijing.Chaoyang'],
        [TDengine\TSDB_DATA_TYPE_INT, 2],
    ]);

    $stmt->bindParams([
        [TDengine\TSDB_DATA_TYPE_TIMESTAMP, 1648432611249],
        [TDengine\TSDB_DATA_TYPE_FLOAT, 10.3],
        [TDengine\TSDB_DATA_TYPE_INT, 219],
        [TDengine\TSDB_DATA_TYPE_FLOAT, 0.31],
    ]);
    $stmt->bindParams([
        [TDengine\TSDB_DATA_TYPE_TIMESTAMP, 1648432611749],
        [TDengine\TSDB_DATA_TYPE_FLOAT, 12.6],
        [TDengine\TSDB_DATA_TYPE_INT, 218],
        [TDengine\TSDB_DATA_TYPE_FLOAT, 0.33],
    ]);
    $resource = $stmt->execute();

    // 影响行数
    var_dump($resource->affectedRows());
} catch (TDengineException $e) {
    // 捕获异常
    throw $e;
}

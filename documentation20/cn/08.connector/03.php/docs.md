# PHP 连接器

## php-tdengine

这是 [Swoole](https://github.com/swoole/swoole-src) 开发组成员，[imi](https://github.com/imiphp/imi) 框架作者宇润开发的 php TDengine 客户端扩展，依赖 `libtaos`。

源代码: <https://github.com/Yurunsoft/php-tdengine>

### 依赖

- [PHP](https://www.php.net/) >= 7.4

- [Swoole](https://github.com/swoole/swoole-src) >= 4.8 （可选）

- [TDengine客户端](https://www.taosdata.com/cn/getting-started/#%E9%80%9A%E8%BF%87%E5%AE%89%E8%A3%85%E5%8C%85%E5%AE%89%E8%A3%85) >= 2.0

## 特性列表

- 参数绑定
- 支持 Swoole 协程化
- 主流操作系统支持：Windows、Linux、MacOS
- 测试用例覆盖

### 编译安装

**下载代码并解压：**

```shell
curl -L -o php-tdengine.tar.gz https://github.com/Yurunsoft/php-tdengine/archive/refs/tags/v1.0.2.tar.gz \
&& mkdir php-tdengine \
&& tar -xzf php-tdengine.tar.gz -C php-tdengine --strip-components=1
```

> 版本 `v1.0.0` 可替换为任意更新的版本，可在 Release 中查看最新版本。

**非 Swoole 环境：**

```shell
phpize && ./configure && make -j && make install
```

**手动指定 tdengine 目录：**

```shell
phpize && ./configure --with-tdengine-dir=/usr/local/Cellar/tdengine/2.4.0.0 && make -j && make install
```

> `--with-tdengine-dir=` 后跟上 tdengine 目录。
> 适用于默认找不到的情况，或者 MacOS 系统用户。

**Swoole 环境：**

```shell
phpize && ./configure --enable-swoole && make -j && make install
```

**启用扩展：**

方法一：在 `php.ini` 中加入 `extension=tdengine`

方法二：运行带参数 `php -dextension=tdengine test.php`

### PHP 代码编写

> 所有错误都会抛出异常: `TDengine\Exception\TDengineException`

**基本：**

```php
use TDengine\Connection;

// 获取扩展版本号
var_dump(\TDengine\EXTENSION_VERSION);

// 设置客户端选项
\TDengine\setOptions([
    \TDengine\TSDB_OPTION_LOCALE => 'en_US.UTF-8', // 区域
    \TDengine\TSDB_OPTION_CHARSET => 'UTF-8', // 字符集
    \TDengine\TSDB_OPTION_TIMEZONE => 'Asia/Shanghai', // 时区
    \TDengine\TSDB_OPTION_CONFIGDIR => '/etc/taos', // 配置目录
    \TDengine\TSDB_OPTION_SHELL_ACTIVITY_TIMER => 3, // shell 活动定时器
]);

// 获取客户端版本信息
var_dump(\TDengine\CLIENT_VERSION);
var_dump(\TDengine\getClientInfo());

// 以下值都是默认值，不改可以不传
$host = '127.0.0.1';
$port = 6030;
$user = 'root';
$pass = 'taosdata';
$db = null;

// 实例化
$connection = new Connection($host, $port, $user, $pass, $db);
// 连接
$connection->connect();
// 获取连接参数
$connection->getHost();
$connection->getPort();
$connection->getUser();
$connection->getPass();
$connection->getDb();
// 获取服务端信息
$connection->getServerInfo();
// 选择默认数据库
$connection->selectDb('db1');
// 关闭连接
$connection->close();
```

**查询：**

```php
// 查询
$resource = $connection->query($sql); // 支持查询和插入
// 获取结果集时间戳字段的精度，0 代表毫秒，1 代表微秒，2 代表纳秒
$resource->getResultPrecision();
// 获取所有数据
$resource->fetch();
// 获取一行数据
$resource->fetchRow();
// 获取字段数组
$resource->fetchFields();
// 获取列数
$resource->getFieldCount();
// 获取影响行数
$resource->affectedRows();
// 获取 SQL 语句
$resource->getSql();
// 获取连接对象
$resource->getConnection();
// 关闭资源（一般不需要手动关闭，变量销毁时会自动释放）
$resource->close();
```

**参数绑定：**

```php
// 查询
$stmt = $connection->prepare($sql); // 支持查询和插入，参数用?占位
// 设置表名和标签
$stmt->setTableNameTags('表名', [
    // 支持格式同参数绑定
    [TDengine\TSDB_DATA_TYPE_INT, 36],
]);
// 绑定参数方法1
$stmt->bindParams(
    // [字段类型, 值]
    [TDengine\TSDB_DATA_TYPE_TIMESTAMP, $time1],
    [TDengine\TSDB_DATA_TYPE_INT, 36],
    [TDengine\TSDB_DATA_TYPE_FLOAT, 44.0],
);
// 绑定参数方法2
$stmt->bindParams([
    // ['type' => 字段类型, 'value' => 值]
    ['type' => TDengine\TSDB_DATA_TYPE_TIMESTAMP, 'value' => $time2],
    ['type' => TDengine\TSDB_DATA_TYPE_INT, 'value' => 36],
    ['type' => TDengine\TSDB_DATA_TYPE_FLOAT, 'value' => 44.0],
]);
// 执行 SQL，返回 Resource，使用方法同 query() 返回值
$resource = $stmt->execute();
// 获取 SQL 语句
$stmt->getSql();
// 获取连接对象
$stmt->getConnection();
// 关闭（一般不需要手动关闭，变量销毁时会自动释放）
$stmt->close();
```

**字段类型：**

| 参数名称 | 说明 |
| ------------ | ------------ 
| `TDengine\TSDB_DATA_TYPE_NULL` | null |
| `TDengine\TSDB_DATA_TYPE_BOOL` | bool |
| `TDengine\TSDB_DATA_TYPE_TINYINT` | tinyint |
| `TDengine\TSDB_DATA_TYPE_SMALLINT` | smallint |
| `TDengine\TSDB_DATA_TYPE_INT` | int |
| `TDengine\TSDB_DATA_TYPE_BIGINT` | bigint |
| `TDengine\TSDB_DATA_TYPE_FLOAT` | float |
| `TDengine\TSDB_DATA_TYPE_DOUBLE` | double |
| `TDengine\TSDB_DATA_TYPE_BINARY` | binary |
| `TDengine\TSDB_DATA_TYPE_TIMESTAMP` | timestamp |
| `TDengine\TSDB_DATA_TYPE_NCHAR` | nchar |
| `TDengine\TSDB_DATA_TYPE_UTINYINT` | utinyint |
| `TDengine\TSDB_DATA_TYPE_USMALLINT` | usmallint |
| `TDengine\TSDB_DATA_TYPE_UINT` | uint |
| `TDengine\TSDB_DATA_TYPE_UBIGINT` | ubigint |

### 在框架中使用

- imi 框架请看：[imi-tdengine](https://github.com/imiphp/imi-tdengine)

## tdengine-restful-connector

封装了 TDengine 的 RESTful 接口，可以使用 PHP 轻松地操作 TDengine 的数据插入和查询了。

此项目支持在 PHP >= 7.0 的项目中使用。

支持在 ThinkPHP、Laravel、Swoole、imi 等项目中使用

在 Swoole 环境中支持协程化，不会阻塞！

Github：<https://github.com/Yurunsoft/tdengine-restful-connector>

### 安装

`composer require yurunsoft/tdengine-restful-connector`

### 使用

**使用连接管理器：**

```php
// 增加名称为 test 的连接配置
\Yurun\TDEngine\TDEngineManager::setClientConfig('test', new \Yurun\TDEngine\ClientConfig([
    // 'host'            => '127.0.0.1',
    // 'hostName'        => '',
    // 'port'            => 6041,
    // 'user'            => 'root',
    // 'password'        => 'taosdata',
    // 'db'              => 'database'
    // 'ssl'             => false,
    // 'timestampFormat' => \Yurun\TDEngine\Constants\TimeStampFormat::LOCAL_STRING,
    // 'keepAlive'       => true,
]));
// 设置默认数据库为test
\Yurun\TDEngine\TDEngineManager::setDefaultClientName('test');
// 获取客户端对象（\Yurun\TDEngine\Client）
$client = \Yurun\TDEngine\TDEngineManager::getClient();
```

**直接 new 客户端：**

```php
$client = new \Yurun\TDEngine\Client(new \Yurun\TDEngine\ClientConfig([
    // 'host'            => '127.0.0.1',
    // 'hostName'        => '',
    // 'port'            => 6041,
    // 'user'            => 'root',
    // 'password'        => 'taosdata',
    // 'db'              => 'database'
    // 'ssl'             => false,
    // 'timestampFormat' => \Yurun\TDEngine\Constants\TimeStampFormat::LOCAL_STRING,
    // 'keepAlive'       => true,
]));

// 通过 sql 方法执行 sql 语句
var_dump($client->sql('create database if not exists db_test'));
var_dump($client->sql('show databases'));
var_dump($client->sql('create table if not exists db_test.tb (ts timestamp, temperature int, humidity float)'));
var_dump($client->sql(sprintf('insert into db_test.tb values(%s,%s,%s)', time() * 1000, mt_rand(), mt_rand() / mt_rand())));

$result = $client->sql('select * from db_test.tb');

$result->getResponse(); // 获取接口原始返回数据

// 获取列数据
foreach ($result->getColumns() as $column)
{
    $column->getName(); // 列名
    $column->getType(); // 列类型值
    $column->getTypeName(); // 列类型名称
    $column->getLength(); // 类型长度
}

// 获取数据
foreach ($result->getData() as $row)
{
    echo $row['列名']; // 经过处理，可以直接使用列名获取指定列数据
}

$result->getStatus(); // 告知操作结果是成功还是失败；同接口返回格式

$result->getHead(); // 表的定义，如果不返回结果集，则仅有一列“affected_rows”。（从 2.0.17 版本开始，建议不要依赖 head 返回值来判断数据列类型，而推荐使用 column_meta。在未来版本中，有可能会从返回值中去掉 head 这一项。）；同接口返回格式

$result->getRow(); // 表明总共多少行数据；同接口返回格式
```

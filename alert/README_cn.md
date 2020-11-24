# Alert

报警监测程序，从 [TDEngine](https://www.taosdata.com/) 读取数据后，根据预定义的规则计算和生成报警，并将它们推送到 [AlertManager](https://github.com/prometheus/alertmanager) 或其它下游应用。

## 安装

### 使用编译好的二进制文件

您可以从 [涛思数据](https://www.taosdata.com/cn/getting-started/) 官网下载最新的安装包。下载完成后，使用以下命令解压：

```
$ tar -xzf tdengine-alert-$version-$OS-$ARCH.tar.gz 
```

如果您之前没有安装过 TDengine 的服务端或客户端，您需要使用下面的命令安装 TDengine 的动态库：

```
$ ./install_driver.sh
```

### 从源码安装

从源码安装需要在您用于编译的计算机上提前安装好 TDEngine 的服务端或客户端，如果您还没有安装，可以参考 TDEngine 的文档。

报警监测程序使用 [Go语言](https://golang.org) 开发，请安装最新版的 Go 语言编译环境。

```
$ mkdir taosdata
$ cd taosdata
$ git clone https://github.com/taosdata/tdengine.git
$ cd tdengine/alert/cmd/alert
$ go build
```

如果由于部分包无法下载导致 `go build` 失败，请根据 [goproxy.io](https://goproxy.io) 上的说明配置好 `GOPROXY` 再重新执行 `go build`。

## 配置

报警监测程序的配置文件采用标准`json`格式，下面是默认的文件内容，请根据实际情况修改。

```json
{
  "port": 8100,
  "database": "file:alert.db",
  "tdengine": "root:taosdata@/tcp(127.0.0.1:0)/",
  "log": {
    "level": "production",
    "path": "alert.log"
  },
  "receivers": {
    "alertManager": "http://127.0.0.1:9093/api/v1/alerts",
    "console": true
  }
}
```

其中：

* **port**：报警监测程序支持使用 `restful API` 对规则进行管理，这个参数用于配置 `http` 服务的侦听端口。
* **database**：报警监测程序将规则保存到了一个 `sqlite` 数据库中，这个参数用于指定数据库文件的路径（不需要提前创建这个文件，如果它不存在，程序会自动创建它）。
* **tdengine**：`TDEngine` 的连接字符串（这个字符串的详细格式说明请见 GO 连接器的文档），一般来说，数据库名应该在报警规则的 `sql` 语句中指定，所以这个字符串中 **不** 应包含数据库名。
* **log > level**：日志的记录级别，可选 `production` 或 `debug`。
* **log > path**：日志文件的路径。
* **receivers > alertManager**：报警监测程序会将报警推送到 `AlertManager`，在这里指定 `AlertManager` 的接收地址。
* **receivers > console**：是否输出到控制台 (stdout)。

准备好配置文件后，可使用下面的命令启动报警监测程序（ `alert.cfg` 是配置文件的路径）：

```
$ ./alert -cfg alert.cfg
```

## 编写报警规则

从技术角度，可以将报警描述为：从 `TDEngine` 中查询最近一段时间、符合一定过滤条件的数据，并基于这些数据根据定义好的计算方法得出一个结果，当结果符合某个条件且持续一定时间后，触发报警。

根据上面的描述，可以很容易的知道报警规则中需要包含的大部分信息。 以下是一个完整的报警规则，采用标准 `json` 格式：

```json
{
  "name": "rule1",
  "sql": "select sum(col1) as sumCol1 from test.meters where ts > now - 1h group by areaid",
  "expr": "sumCol1 > 10",
  "for": "10m",
  "period": "1m",
  "labels": {
    "ruleName": "rule1"
  },
  "annotations": {
    "summary": "sum of rule {{$labels.ruleName}} of area {{$values.areaid}} is {{$values.sumCol1}}"
  }
}
```

其中：

* **name**：用于为规则指定一个唯一的名字。
* **sql**：从 `TDEngine` 中查询数据时使用的 `sql` 语句，查询结果中的列将被后续计算使用，所以，如果使用了聚合函数，请为这一列指定一个别名。
* **expr**：一个计算结果为布尔型的表达式，支持算数运算、逻辑运算，并且内置了部分函数，也可以引用查询结果中的列。 当表达式计算结果为 `true` 时，进入报警状态。
* **for**：当表达式计算结果为 `true` 的连续时长超过这个选项时，触发报警，否则报警处于“待定”状态。默认为0，表示一旦计算结果为 `true`，立即触发报警。
* **period**：规则的检查周期，默认1分钟。
* **labels**：人为指定的标签列表，标签可以在生成报警信息引用。如果 `sql` 中包含 `group by` 子句，则所有用于分组的字段会被自动加入这个标签列表中。
* **annotations**：用于定义报警信息，使用 [go template](https://golang.org/pkg/text/template) 语法，其中，可以通过 `$labels.<label name>` 引用标签，也可以通过 `$values.<column name>` 引用查询结果中的列。

### 运算符

以下是 `expr` 字段中支持的运算符，您可以使用 `()` 改变运算的优先级。

<table>
<thead>
<tr> <td>运算符</td><td>单目/双目</td><td>优先级</td><td>作用</td> </tr>
</thead>
<tbody>
<tr> <td>~</td><td>单目</td><td>6</td><td>按位取反</td> </tr>
<tr> <td>!</td><td>单目</td><td>6</td><td>逻辑非</td> </tr>
<tr> <td>+</td><td>单目</td><td>6</td><td>正号</td> </tr>
<tr> <td>-</td><td>单目</td><td>6</td><td>负号</td> </tr>
<tr> <td>*</td><td>双目</td><td>5</td><td>乘法</td> </tr>
<tr> <td>/</td><td>双目</td><td>5</td><td>除法</td> </tr>
<tr> <td>%</td><td>双目</td><td>5</td><td>取模（余数）</td> </tr>
<tr> <td><<</td><td>双目</td><td>5</td><td>按位左移</td> </tr>
<tr> <td>>></td><td>双目</td><td>5</td><td>按位右移</td> </tr>
<tr> <td>&</td><td>双目</td><td>5</td><td>按位与</td> </tr>
<tr> <td>+</td><td>双目</td><td>4</td><td>加法</td> </tr>
<tr> <td>-</td><td>双目</td><td>4</td><td>减法</td> </tr>
<tr> <td>|</td><td>双目</td><td>4</td><td>按位或</td> </tr>
<tr> <td>^</td><td>双目</td><td>4</td><td>按位异或</td> </tr>
<tr> <td>==</td><td>双目</td><td>3</td><td>等于</td> </tr>
<tr> <td>!=</td><td>双目</td><td>3</td><td>不等于</td> </tr>
<tr> <td><</td><td>双目</td><td>3</td><td>小于</td> </tr>
<tr> <td><=</td><td>双目</td><td>3</td><td>小于或等于</td> </tr>
<tr> <td>></td><td>双目</td><td>3</td><td>大于</td> </tr>
<tr> <td>>=</td><td>双目</td><td>3</td><td>大于或等于</td> </tr>
<tr> <td>&&</td><td>双目</td><td>2</td><td>逻辑与</td> </tr>
<tr> <td>||</td><td>双目</td><td>1</td><td>逻辑或</td> </tr>
</tbody>
</table>

### 内置函数

目前支持以下内置函数，可以在报警规则的 `expr` 字段中使用这些函数：

* **min**：取多个值中的最小值，例如 `min(1, 2, 3)` 返回 `1`。
* **max**：取多个值中的最大值，例如 `max(1, 2, 3)` 返回 `3`。
* **sum**：求和，例如 `sum(1, 2, 3)` 返回 `6`。
* **avg**：求算术平均值，例如 `avg(1, 2, 3)` 返回 `2`。
* **sqrt**：计算平方根，例如 `sqrt(9)` 返回 `3`。
* **ceil**：上取整，例如 `ceil(9.1)` 返回 `10`。
* **floor**：下取整，例如 `floor(9.9)` 返回 `9`。
* **round**：四舍五入，例如 `round(9.9)` 返回 `10`， `round(9.1)` 返回 `9`。
* **log**：计算自然对数，例如 `log(10)` 返回 `2.302585`。
* **log10**：计算以10为底的对数，例如 `log10(10)` 返回 `1`。
* **abs**：计算绝对值，例如 `abs(-1)` 返回 `1`。
* **if**：如果第一个参数为 `true`，返回第二个参数，否则返回第三个参数，例如 `if(true, 10, 100)` 返回 `10`， `if(false, 10, 100)` 返回 `100`。

## 规则管理

* 添加或修改

    * API地址：http://\<server\>:\<port\>/api/update-rule
    * Method：POST
    * Body：规则定义
    * 示例：curl -d '@rule.json' http://localhost:8100/api/update-rule

* 删除

    * API地址：http://\<server\>:\<port\>/api/delete-rule?name=\<rule name\>
    * Method：DELETE
    * 示例：curl -X DELETE http://localhost:8100/api/delete-rule?name=rule1

* 挂起或恢复

    * API地址：http://\<server\>:\<port\>/api/enable-rule?name=\<rule name\>&enable=[true | false]
    * Method：POST
    * 示例：curl -X POST http://localhost:8100/api/enable-rule?name=rule1&enable=true

* 获取列表

    * API地址：http://\<server\>:\<port\>/api/list-rule
    * Method：GET
    * 示例：curl http://localhost:8100/api/list-rule

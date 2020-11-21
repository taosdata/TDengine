# Alert

The Alert application reads data from [TDEngine](https://www.taosdata.com/), calculating according to predefined rules to generate alerts, and pushes alerts to downstream applications like [AlertManager](https://github.com/prometheus/alertmanager).

## Install

### From Binary

Precompiled binaries is available at [taosdata website](https://www.taosdata.com/en/getting-started/), please download and unpack it by below shell command.

```
$ tar -xzf tdengine-alert-$version-$OS-$ARCH.tar.gz 
```

If you have no TDengine server or client installed, please execute below command to install the required driver library:

```
$ ./install_driver.sh
```

### From Source Code

Two prerequisites are required to install from source.

1. TDEngine server or client must be installed.
2. Latest [Go](https://golang.org) language must be installed.

When these two prerequisites are ready, please follow steps below to build the application:

```
$ mkdir taosdata
$ cd taosdata
$ git clone https://github.com/taosdata/tdengine.git
$ cd tdengine/alert/cmd/alert
$ go build
```

If `go build` fails because some of the dependency packages cannot be downloaded, please follow steps in [goproxy.io](https://goproxy.io) to configure `GOPROXY` and try `go build` again.

## Configure

The configuration file format of Alert application is standard `json`, below is its default content, please revise according to actual scenario.

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

The use of each configuration item is:

* **port**: This is the `http` service port which enables other application to manage rules by `restful API`.
* **database**: rules are stored in a `sqlite` database, this is the path of the database file (if the file does not exist, the alert application creates it automatically).
* **tdengine**: connection string of `TDEngine` server (please refer the documentation of GO connector for the detailed format of this string), note the database name should be put in the `sql` field of a rule in most cases, thus it should NOT be included in the string.
* **log > level**: log level, could be `production` or `debug`.
* **log > path**: log output file path.
* **receivers > alertManager**: the alert application pushes alerts to `AlertManager` at this URL.
* **receivers > console**: print out alerts to console (stdout) or not.

When the configruation file is ready, the alert application can be started with below command (`alert.cfg` is the path of the configuration file):

```
$ ./alert -cfg alert.cfg
```

## Prepare an alert rule

From technical aspect, an alert could be defined as: query and filter recent data from `TDEngine`, and calculating out a boolean value from these data according to a formula, and trigger an alert if the boolean value last for a certain duration.

This is a rule example in `json` format:

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

The fields of the rule is explained below:

* **name**: the name of the rule, must be unique.
* **sql**: this is the `sql` statement used to query data from `TDEngine`, columns of the query result are used in later processing, so please give the column an alias if aggregation functions are used.
* **expr**: an expression whose result is a boolean value, arithmatic and logical calculations can be included in the expression, and builtin functions (see below) are also supported. Alerts are only triggered when the expression evaluates to `true`.
* **for**: this item is a duration which default value is zero second. when `expr` evaluates to `true` and last at least this duration, an alert is triggered.
* **period**: the interval for the alert application to check the rule, default is 1 minute.
* **labels**: a label list, labels are used to generate alert information. note if the `sql` statement includes a `group by` clause, the `group by` columns are inserted into this list automatically.
* **annotations**: the template of alert information which is in [go template](https://golang.org/pkg/text/template) syntax, labels can be referenced by `$labels.<label name>` and columns of the query result can be referenced by `$values.<column name>`.

### Operators

Operators which can be used in the `expr` field of a rule are list below, `()` can be to change precedence if default does not meet requirement.

<table>
<thead>
<tr> <td>Operator</td><td>Unary/Binary</td><td>Precedence</td><td>Effect</td> </tr>
</thead>
<tbody>
<tr> <td>~</td><td>Unary</td><td>6</td><td>Bitwise Not</td> </tr>
<tr> <td>!</td><td>Unary</td><td>6</td><td>Logical Not</td> </tr>
<tr> <td>+</td><td>Unary</td><td>6</td><td>Positive Sign</td> </tr>
<tr> <td>-</td><td>Unary</td><td>6</td><td>Negative Sign</td> </tr>
<tr> <td>*</td><td>Binary</td><td>5</td><td>Multiplication</td> </tr>
<tr> <td>/</td><td>Binary</td><td>5</td><td>Division</td> </tr>
<tr> <td>%</td><td>Binary</td><td>5</td><td>Modulus</td> </tr>
<tr> <td><<</td><td>Binary</td><td>5</td><td>Bitwise Left Shift</td> </tr>
<tr> <td>>></td><td>Binary</td><td>5</td><td>Bitwise Right Shift</td> </tr>
<tr> <td>&</td><td>Binary</td><td>5</td><td>Bitwise And</td> </tr>
<tr> <td>+</td><td>Binary</td><td>4</td><td>Addition</td> </tr>
<tr> <td>-</td><td>Binary</td><td>4</td><td>Subtraction</td> </tr>
<tr> <td>|</td><td>Binary</td><td>4</td><td>Bitwise Or</td> </tr>
<tr> <td>^</td><td>Binary</td><td>4</td><td>Bitwise Xor</td> </tr>
<tr> <td>==</td><td>Binary</td><td>3</td><td>Equal</td> </tr>
<tr> <td>!=</td><td>Binary</td><td>3</td><td>Not Equal</td> </tr>
<tr> <td><</td><td>Binary</td><td>3</td><td>Less Than</td> </tr>
<tr> <td><=</td><td>Binary</td><td>3</td><td>Less Than or Equal</td> </tr>
<tr> <td>></td><td>Binary</td><td>3</td><td>Great Than</td> </tr>
<tr> <td>>=</td><td>Binary</td><td>3</td><td>Great Than or Equal</td> </tr>
<tr> <td>&&</td><td>Binary</td><td>2</td><td>Logical And</td> </tr>
<tr> <td>||</td><td>Binary</td><td>1</td><td>Logical Or</td> </tr>
</tbody>
</table>

### Built-in Functions

Built-in function can be used in the `expr` field of a rule.

* **min**: returns the minimum one of its arguments, for example: `min(1, 2, 3)` returns `1`.
* **max**: returns the maximum one of its arguments, for example: `max(1, 2, 3)` returns `3`.
* **sum**: returns the sum of its arguments, for example: `sum(1, 2, 3)` returns `6`.
* **avg**: returns the average of its arguments, for example: `avg(1, 2, 3)` returns `2`.
* **sqrt**: returns the square root of its argument, for example: `sqrt(9)` returns `3`.
* **ceil**: returns the minimum integer which greater or equal to its argument, for example: `ceil(9.1)` returns `10`.
* **floor**: returns the maximum integer which lesser or equal to its argument, for example: `floor(9.9)` returns `9`.
* **round**: round its argument to nearest integer, for examples: `round(9.9)` returns `10` and `round(9.1)` returns `9`.
* **log**: returns the natural logarithm of its argument, for example: `log(10)` returns `2.302585`. 
* **log10**: returns base 10 logarithm of its argument, for example: `log10(10)` return `1`.
* **abs**: returns the absolute value of its argument, for example: `abs(-1)` returns `1`.
* **if**: if the first argument is `true` returns its second argument, and returns its third argument otherwise, for examples: `if(true, 10, 100)` returns `10` and `if(false, 10, 100)` returns `100`.

## Rule Management

* Add / Update

    * API address: http://\<server\>:\<port\>/api/update-rule
    * Method: POST
    * Body: the rule
    * Example：curl -d '@rule.json' http://localhost:8100/api/update-rule

* Delete

    * API address: http://\<server\>:\<port\>/api/delete-rule?name=\<rule name\>
    * Method：DELETE
    * Example：curl -X DELETE http://localhost:8100/api/delete-rule?name=rule1

* Enable / Disable

    * API address: http://\<server\>:\<port\>/api/enable-rule?name=\<rule name\>&enable=[true | false]
    * Method POST
    * Example：curl -X POST http://localhost:8100/api/enable-rule?name=rule1&enable=true

* Retrieve rule list

    * API address: http://\<server\>:\<port\>/api/list-rule
    * Method: GET
    * Example：curl http://localhost:8100/api/list-rule

# STRESS

Stress test tool for TDengine. It run a set of test cases randomly and show statistics.

## COMMAND LINE

``` bash
$ ./stress [-h=<localhost>] [-P=<0>] [-d=<test>] [-u=<root>] [-p=<taosdata>] [-c=<4>] [-f=<true>] [-l=<logPath>] [path_or_sql]
```

* **-h**: host name or IP address of TDengine server (default: localhost).
* **-P**: port number of TDengine server (default: 0).
* **-u**: user name (default: root).
* **-p**: password (default: taosdata).
* **-c**: concurrency, number of concurrent goroutines for query (default: 4).
* **-f**: fetch data or not (default: true).
* **-l**: log file path (default: no log).
* **path_or_sql**: a SQL statement or path of a JSON file which contains the test cases (default: cases.json).

## TEST CASE FILE

```json
[{
    "weight": 1,
    "sql": "select * from meters where ts>=now+%dm and ts<=now-%dm and c1=%v and c2=%d and c3='%s' and tbname='%s'",
    "args": [{
        "type": "range",
        "min": 30,
        "max": 60
    }, {
        "type": "bool"
    }, {
        "type": "int",
        "min": -10,
        "max": 20
    }, {
        "type": "string",
        "min": 0,
        "max": 10,
    }, {
        "type": "list",
        "list": [
            "table1",
            "table2",
            "table3",
            "table4"
        ]
    }]
}]
```

The test case file is a standard JSON file which contains an array of test cases. For test cases, field `sql` is mandatory, and it can optionally include a `weight` field and an `args` field which is an array of arguments.

`sql` is a SQL statement, it can include zero or more arguments (placeholders).

`weight` defines the possibility of the case being selected, the greater value the higher possibility. It must be an non-negative integer and the default value is zero, but, if all cases have a zero weight, all the weights are regarded as 1. 

Placeholders of `sql` are replaced by arguments in `args` at runtime. There are 5 types of arguments currently:

* **bool**: generate a `boolean` value randomly.
* **int**: generate an `integer` between [`min`, `max`] randomly, the default value of `min` is 0 and `max` is 100.
* **range**: generate two `integer`s between [`min`, `max`] randomly, the first is less than the second, the default value of `min` is 0 and `max` is 100.
* **string**: generate a `string` with length between [`min`, `max`] randomly, the default value of `min` is 0 and `max` is 100.
* **list**: select an item from `list` randomly.

## OUTPUT

```
 00:00:08 | TOTAL REQ | TOTAL TIME(us) | TOTAL AVG(us) | REQUEST |  TIME(us)  |  AVERAGE(us)  |
    TOTAL |      3027 |       26183890 |       8650.11 |     287 |    3060935 |      10665.28 |
  SUCCESS |      3027 |       26183890 |       8650.11 |     287 |    3060935 |      10665.28 |
     FAIL |         0 |              0 |          0.00 |       0 |          0 |          0.00 |
```

* **Col 2**: total number of request since test start.
* **Col 3**: total time of all request since test start.
* **Col 4**: average time of all request since test start.
* **Col 5**: number of request in last second.
* **Col 6**: time of all request in last second.
* **Col 7**: average time of all request in last second.

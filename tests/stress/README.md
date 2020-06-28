# STRESS

``` bash
$ ./stress [-server=<localhost>] [-db=<test>] [-concurrent=<1>] [-fetch=<false>] [scriptFile]
```

## SCRIPT FILE

```json
[{
    "sql": "select * from meters where id = %d and a >= %d and a <= %d and tbname='%s'",
    "args": [{
        "type": "int",
        "min": -10,
        "max": 20
    }, {
        "type": "range",
        "min": 30,
        "max": 60
    }, {
        "type": "string",
        "min": 0,
        "max": 10,
        "list": [
            "table1",
            "table2",
            "table3",
            "table4"
        ]
    }]
}]
```
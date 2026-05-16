# Last 查询性能优化测试报告

## 1. 背景 

[LAST 查询从 3.3.4.7 至 3.3.8.8 性能所有下降 ](https://project.feishu.cn/taosdata_td/feature/detail/6643522153)

## 2. 测试脚本

### 2.1 写入脚本 insert.json

100 W 张子表，每张表 2016 条数据
```json
{
    "filetype":"insert",
    "cfgdir":"/etc/taos",
    "host":"127.0.0.1",
    "port":6030,
    "user":"root",
    "password":"taosdata",
    "connection_pool_size":8,
    "thread_count":32,
    "create_table_thread_count":10,
    "result_file":"./log/insert_result.txt",
    "confirm_parameter_prompt":"no",
    "insert_interval":0,
    "num_of_records_per_req":1000,
    "thread_bind_vgroup":"yes",
    "databases":[
        {
            "dbinfo":{
                "name":"test",
                "drop":"yes",
                "replica":1,
                "wal_retention_period":1,
                "wal_retention_size":1,
                "vgroups":32,
                "cachemodel": "'none'",
                "cachesize":100,
                "wal_level":1,
                "buffer":256,
                "duration":"14400m",
                "stt_trigger":2
            },
            "super_tables":[
                {
                    "name":"meters",
                    "child_table_exists":"no",
                    "childtable_count":1000000,
                    "childtable_prefix":"d",
                    "escape_character":"yes",
                    "auto_create_table":"no",
                    "batch_create_tbl_num":5,
                    "data_source":"rand",
                    "insert_mode":"stmt",
                    "non_stop_mode":"no",
                    "line_protocol":"line",
                    "insert_rows":2016,
                    "interlace_rows":1,
                    "insert_interval":0,
                    "timestamp_step":300000,
                    "start_timestamp":"2022-10-01 00:00:00.000",
                    "sample_format":"csv",
                    "sample_file": "./test_cases/baseline_scenarios/meters.csv",
                    "columns":[
                        { "type": "float",      "name": "current",  "max": 1, "min": 0 },
                        { "type": "int",        "name": "voltage",  "max": 100, "min": 0 },
                        { "type": "float",      "name": "phase",    "max": 100, "min": 0 }
                    ],
                    "tags":[
                        {
                            "type":"binary",
                            "name":"location",
                            "max":64,
                            "min":1,
                            "values":[
                                "San Francisco",
                                "Los Angles",
                                "San Diego",
                                "San Jose",
                                "Palo Alto",
                                "Campbell",
                                "Mountain View",
                                "Sunnyvale",
                                "Santa Clara",
                                "Cupertino"
                            ]
                        },
                        {
                            "name":"groupId",
                            "type":"int",
                            "max":100000,
                            "min":1
                        }
                    ]
                }
            ]
        }
    ]
}

```

### 2.2 查询 Q1.json

```json
{
        "filetype": "query",
        "cfgdir": "/etc/taos",
        "host": "127.0.0.1",
        "port": 6030,
        "user": "root",
        "password": "taosdata",
        "confirm_parameter_prompt": "no",
        "continue_if_fail": "yes",
        "databases": "test",
        "query_times": 1000,
        "query_mode": "taosc",
        "specified_table_query": {
                "threads": 1,
                "sqls": [
                        {
                                "sql": "select last_row(current) from test.d1;"
                        },
                        {
                                "sql": "select last(*) from test.d1 partition by tbname slimit 10;"
                        }
                ]
        }
}

```

## 3. 场景1： 使用 3.3.4.7 benchmark 产生数据测试对比

### 3.3.4.7 分支结果

```sql
complete query with 1 threads and 1000 query delay avg:         0.007293s min:  0.007082s max:  0.009478s p90:  0.007470s p95:  0.007523s p99:  0.007670s SQL command: select last_row(current) from test.d1;
complete query with 1 threads and 1000 query delay avg:         0.007738s min:  0.007552s max:  0.008943s p90:  0.007839s p95:  0.007874s p99:  0.008007s SQL command: select last_row(current) from test.d2;
complete query with 1 threads and 1000 query delay avg:         0.006976s min:  0.006820s max:  0.007520s p90:  0.007068s p95:  0.007099s p99:  0.007295s SQL command: select last_row(current) from test.d10;
complete query with 1 threads and 1000 query delay avg:         0.007024s min:  0.006816s max:  0.009287s p90:  0.007163s p95:  0.007288s p99:  0.007440s SQL command: select last_row(current) from test.d11;
complete query with 1 threads and 1000 query delay avg:         0.004433s min:  0.004258s max:  0.005102s p90:  0.004513s p95:  0.004542s p99:  0.004618s SQL command: select last_row(current) from test.d100;
complete query with 1 threads and 1000 query delay avg:         0.007229s min:  0.007024s max:  0.008450s p90:  0.007357s p95:  0.007512s p99:  0.007633s SQL command: select last_row(current) from test.d1000;
complete query with 1 threads and 1000 query delay avg:         0.004018s min:  0.003876s max:  0.004479s p90:  0.004091s p95:  0.004112s p99:  0.004168s SQL command: select last_row(current) from test.d10000;
complete query with 1 threads and 1000 query delay avg:         0.005899s min:  0.005708s max:  0.006493s p90:  0.006019s p95:  0.006061s p99:  0.006147s SQL command: select last_row(current) from test.d20000;
complete query with 1 threads and 1000 query delay avg:         0.004293s min:  0.004161s max:  0.005028s p90:  0.004362s p95:  0.004399s p99:  0.004496s SQL command: select last_row(current) from test.d30000;
complete query with 1 threads and 1000 query delay avg:         0.007817s min:  0.007607s max:  0.008494s p90:  0.007954s p95:  0.008097s p99:  0.008179s SQL command: select last_row(current) from test.d100000;
complete query with 1 threads and 1000 query delay avg:         0.007787s min:  0.007591s max:  0.008448s p90:  0.007944s p95:  0.008065s p99:  0.008130s SQL command: select last_row(current) from test.d300000;
complete query with 1 threads and 1000 query delay avg:         0.007820s min:  0.007627s max:  0.008428s p90:  0.007920s p95:  0.007951s p99:  0.008023s SQL command: select last_row(current) from test.d500000;
```

**最终多表平均值： 0.07832700000 s /12 = 0.006527250 s**

### 3.0 分支结果(commit cc11e8bd3fdf14798900e03b7a6212be49b08d09)

```python
avg: 0.009162s min: 0.008590s max: 0.052513s p90: 0.009555s p95: 0.009945s p99: 0.013151s 
avg: 0.009694s min: 0.009035s max: 0.126825s p90: 0.009885s p95: 0.010077s p99: 0.011240s 
avg: 0.008849s min: 0.008312s max: 0.016334s p90: 0.009246s p95: 0.009403s p99: 0.010840s 
avg: 0.008812s min: 0.008333s max: 0.012459s p90: 0.009219s p95: 0.009430s p99: 0.010630s 
avg: 0.005511s min: 0.005096s max: 0.013387s p90: 0.005826s p95: 0.005947s p99: 0.007010s 
avg: 0.009097s min: 0.008523s max: 0.044912s p90: 0.009449s p95: 0.009948s p99: 0.011937s 
avg: 0.004963s min: 0.004682s max: 0.009391s p90: 0.005253s p95: 0.005363s p99: 0.006227s 
avg: 0.007300s min: 0.006854s max: 0.011210s p90: 0.007717s p95: 0.007829s p99: 0.008516s 
avg: 0.005195s min: 0.004898s max: 0.007699s p90: 0.005553s p95: 0.005690s p99: 0.006400s 
avg: 0.009376s min: 0.009009s max: 0.032244s p90: 0.009819s p95: 0.010106s p99: 0.010804s 
avg: 0.009317s min: 0.009012s max: 0.011947s p90: 0.009502s p95: 0.010124s p99: 0.010748s 
avg: 0.009415s min: 0.009079s max: 0.038468s p90: 0.009657s p95: 0.010177s p99: 0.011424s 
```

**最终多表平均值：  0.096691/12 = 0.008057583333333333**

### 3.1 优化后结果

```python
avg: 0.007270 s min: 0.006809s max: 0.010271s p90: 0.007668s p95: 0.007913s p99: 0.009382s 
avg: 0.007526 s min: 0.007321s max: 0.010084s p90: 0.007601s p95: 0.007640s p99: 0.009261s 
avg: 0.006793 s min: 0.006597s max: 0.009238s p90: 0.006824s p95: 0.006928s p99: 0.008834s 
avg: 0.006781 s min: 0.006565s max: 0.009155s p90: 0.006828s p95: 0.006880s p99: 0.008433s 
avg: 0.004386 s min: 0.004181s max: 0.042189s p90: 0.004392s p95: 0.004414s p99: 0.005392s 
avg: 0.007045 s min: 0.006818s max: 0.057416s p90: 0.007075s p95: 0.007103s p99: 0.008153s 
avg: 0.003985 s min: 0.003786s max: 0.043152s p90: 0.003987s p95: 0.004013s p99: 0.005039s 
avg: 0.005811 s min: 0.005612s max: 0.039698s p90: 0.005830s p95: 0.005866s p99: 0.006997s 
avg: 0.004255 s min: 0.004078s max: 0.045768s p90: 0.004253s p95: 0.004287s p99: 0.005398s 
avg: 0.007621 s min: 0.007348s max: 0.086494s p90: 0.007615s p95: 0.007696s p99: 0.009165s
avg: 0.007536 s min: 0.007303s max: 0.009857s p90: 0.007564s p95: 0.007618s p99: 0.009581s
avg: 0.007598 s min: 0.007360s max: 0.023277s p90: 0.007649s p95: 0.007690s p99: 0.009259s

```

**最终多表平均值：  0.076607 / 12 = 0.006383916666666666**；
附之前另外一次测试：
3.3.4.7 ： 0.07977300000000001/12 = 0.006647750000000001
3.0 优化前：0.09670899999999999/12=0.008059083333333333
3.0 优化后：0.07743/12 = 0.0064525
**优化提升结果：**
**第一次测试：(0.008059083333333333-0.0064525)/0.008059083333333333 = 19.935%**
** (0.0064525 < 0.006647750) **
**第二次测试：(0.008057583333333333-0.006383916666666666) / 0.008057583333333333 = 20.77%**
**（0.006383916666666666  < 0.006527250）**
优化后好像都稍微快了一丢丢

### 3.2 调整 stt_trigger 为 1， 进行compact 数据重整后测试

优化前平均耗时：0.016983/12=0.00141525
```yaml
[01/12 09:33:01.326400] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 1 spend 4.838342s QPS: 206.682 query delay avg: 0.004836s min: 0.004590s max: 0.011109s p90: 0.004852s p95: 0.005255s p99: 0.007815s SQL command: select last_row(current) from test.d1; 
[01/12 09:33:06.165357] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 2 spend 4.786394s QPS: 208.926 query delay avg: 0.004785s min: 0.004595s max: 0.011825s p90: 0.004843s p95: 0.004877s p99: 0.005540s SQL command: select last_row(current) from test.d2; 
[01/12 09:33:10.952373] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 3 spend 4.819328s QPS: 207.498 query delay avg: 0.004818s min: 0.004662s max: 0.007638s p90: 0.004886s p95: 0.004914s p99: 0.005511s SQL command: select last_row(current) from test.d10; 
[01/12 09:33:15.772285] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 4 spend 4.819041s QPS: 207.510 query delay avg: 0.004818s min: 0.004637s max: 0.007762s p90: 0.004892s p95: 0.004930s p99: 0.005514s SQL command: select last_row(current) from test.d11; 
[01/12 09:33:20.591896] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 5 spend 4.838290s QPS: 206.685 query delay avg: 0.004837s min: 0.004673s max: 0.007876s p90: 0.004903s p95: 0.004950s p99: 0.005535s SQL command: select last_row(current) from test.d100; 
[01/12 09:33:25.430794] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 6 spend 4.731122s QPS: 211.366 query delay avg: 0.004730s min: 0.004563s max: 0.007510s p90: 0.004786s p95: 0.004826s p99: 0.005456s SQL command: select last_row(current) from test.d1000; 
[01/12 09:33:30.162481] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 7 spend 4.857878s QPS: 205.851 query delay avg: 0.004857s min: 0.004693s max: 0.006498s p90: 0.004916s p95: 0.004950s p99: 0.005576s SQL command: select last_row(current) from test.d10000; 
[01/12 09:33:35.020965] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 8 spend 4.723413s QPS: 211.711 query delay avg: 0.004722s min: 0.004547s max: 0.007598s p90: 0.004782s p95: 0.004815s p99: 0.005411s SQL command: select last_row(current) from test.d20000; 
[01/12 09:33:39.745002] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 9 spend 4.842986s QPS: 206.484 query delay avg: 0.004842s min: 0.004663s max: 0.007854s p90: 0.004912s p95: 0.004936s p99: 0.005561s SQL command: select last_row(current) from test.d30000; 
[01/12 09:33:44.588587] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 10 spend 4.432025s QPS: 225.630 query delay avg: 0.004431s min: 0.004244s max: 0.007325s p90: 0.004484s p95: 0.004518s p99: 0.005111s SQL command: select last_row(current) from test.d100000; 
[01/12 09:33:49.021180] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 11 spend 4.462688s QPS: 224.080 query delay avg: 0.004462s min: 0.004284s max: 0.007157s p90: 0.004532s p95: 0.004585s p99: 0.005207s SQL command: select last_row(current) from test.d300000; 
[01/12 09:33:53.484434] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 12 spend 4.836477s QPS: 206.762 query delay avg: 0.004835s min: 0.004638s max: 0.007546s p90: 0.004889s p95: 0.004934s p99: 0.005910s SQL command: select last_row(current) from test.d500000; 
[01/12 09:33:58.321550] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
```

优化后平均耗时：0.017248/12 = 0.0014373333333333332
```sql
[01/14 10:49:22.768504] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 1 spend 1.476594s QPS: 677.234 query delay avg: 0.001474s min: 0.001238s max: 0.112858s p90: 0.001419s p95: 0.001477s p99: 0.002182s SQL command: select last_row(current) from test.d1;
[01/14 10:49:24.245599] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 2 spend 1.388563s QPS: 720.169 query delay avg: 0.001387s min: 0.001260s max: 0.037321s p90: 0.001396s p95: 0.001412s p99: 0.001456s SQL command: select last_row(current) from test.d2;
[01/14 10:49:25.634719] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 3 spend 1.414486s QPS: 706.971 query delay avg: 0.001413s min: 0.001275s max: 0.047071s p90: 0.001414s p95: 0.001431s p99: 0.001460s SQL command: select last_row(current) from test.d10;
[01/14 10:49:27.049746] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 4 spend 1.355409s QPS: 737.785 query delay avg: 0.001354s min: 0.001265s max: 0.002610s p90: 0.001398s p95: 0.001416s p99: 0.001448s SQL command: select last_row(current) from test.d11;
[01/14 10:49:28.405679] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 5 spend 1.457092s QPS: 686.298 query delay avg: 0.001456s min: 0.001267s max: 0.096222s p90: 0.001404s p95: 0.001431s p99: 0.001679s SQL command: select last_row(current) from test.d100;
[01/14 10:49:29.863294] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 6 spend 1.431653s QPS: 698.493 query delay avg: 0.001431s min: 0.001302s max: 0.049718s p90: 0.001423s p95: 0.001434s p99: 0.001462s SQL command: select last_row(current) from test.d1000;
[01/14 10:49:31.295502] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 7 spend 1.518627s QPS: 658.490 query delay avg: 0.001517s min: 0.001302s max: 0.097602s p90: 0.001467s p95: 0.001481s p99: 0.001518s SQL command: select last_row(current) from test.d10000;
[01/14 10:49:32.814662] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 8 spend 1.440996s QPS: 693.964 query delay avg: 0.001440s min: 0.001287s max: 0.044109s p90: 0.001439s p95: 0.001471s p99: 0.001753s SQL command: select last_row(current) from test.d20000;
[01/14 10:49:34.256190] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 9 spend 1.532284s QPS: 652.621 query delay avg: 0.001531s min: 0.001338s max: 0.108550s p90: 0.001461s p95: 0.001472s p99: 0.001508s SQL command: select last_row(current) from test.d30000;
[01/14 10:49:35.789027] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 10 spend 1.437570s QPS: 695.618 query delay avg: 0.001436s min: 0.001298s max: 0.051524s p90: 0.001432s p95: 0.001448s p99: 0.001487s SQL command: select last_row(current) from test.d100000;
[01/14 10:49:37.227127] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 11 spend 1.387606s QPS: 720.666 query delay avg: 0.001387s min: 0.001266s max: 0.032353s p90: 0.001398s p95: 0.001411s p99: 0.001439s SQL command: select last_row(current) from test.d300000;
[01/14 10:49:38.615251] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 12 spend 1.423424s QPS: 702.531 query delay avg: 0.001422s min: 0.001283s max: 0.029052s p90: 0.001441s p95: 0.001463s p99: 0.001754s SQL command: select last_row(current) from test.d500000;
[01/14 10:49:40.039227] SUCC: host:127.0.0.1 port:0 dbname:test connect successfully.
complete query with 1 threads and 1000 sql 13 spend 1.797446s QPS: 556.345 query delay avg: 0.001796s min: 0.001651s max: 0.049533s p90: 0.001796s p95: 0.001819s p99: 0.001967s SQL command: select last(*) from test.d11109 partition by tbname slimit 10;

```

**结果：重整数据后，stt 文件读取耗时大幅度降低，该场景耗时只有重整前的  0.00141525/0.006527250=21.6%**
**此时优化的路径占比变得很小，优化效果基本看不出来。符合预期。也证明了之前的耗时大部分确实都是由于 stt 文件读取造成，优化点合理。**

## 4. 场景2：使用3.0 版本taosbenchmark 产生的数据对比

优化前表平均耗时：0.069482/12 = 0.0057901666666666666
优化后表平均耗时：0.056972999999999996/12 = 0.004747749999999999
**提升约：(0.0057901666666666666-0.004747749999999999)/0.0057901666666666666 = 18.0 %**

## 5. 结论

1. 主要耗时集中在 stt 文件读取上，通过优化 stt 文件读取链路，及局部优化内存池实现，对比优化前提升约 20% 
2. 该场景和 3.3.4.7 性能达到一致，甚至略有提升（1%-2%）
3. stt_triger 和 Compact 对stt 文件的读取速度影响巨大，实际使用中需要注意

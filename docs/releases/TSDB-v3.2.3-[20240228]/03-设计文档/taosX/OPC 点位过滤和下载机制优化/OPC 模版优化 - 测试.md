# OPC 模版优化 - 测试

## 1. OPC-UA，“数据点位模版”，不配置 transform

点“数据点位模版”下载csv模版，不做任何修改，上传csv文件，执行任务
```rust
序号,数据点位id,"是否启用(可选,1 - 启动, 0 - 停用。配置为0,将删除数据点位对应的子表)",超级表名,子表名,采集值列名,采集值转换规则(可选),"采集值类型(可选,默认根据实际类型自动填充,可选值有int, double, float, string)",数据质量列名,OPC原始时间列名(默认作为时间戳主键),"TD 服务端接收时间列名(将本列剪切到 ts_col 之前,将会使用本列作为时间戳主键)", ts_col 的时间戳转换规则, received_ts_col 的时间戳转换规则," 标签列(不需要可删除,需要多个,可以在右侧添加新列,可指定列名和类型）"
0,point_id,enabled,stable,tbname,value_col,value_transform,type,quality_col,ts_col,received_ts_col,ts_transform,received_ts_transform,tag::VARCHAR(200)::name
1,ns=3;i=1001,1,opc_{type},t_{ns}_{id},val,,,quality,ts,rts,,,Constant
2,ns=3;i=1002,1,opc_{type},t_{ns}_{id},val,,,quality,ts,rts,,,Counter
3,ns=3;i=1003,1,opc_{type},t_{ns}_{id},val,,,quality,ts,rts,,,Random
4,ns=3;i=1004,1,opc_{type},t_{ns}_{id},val,,,quality,ts,rts,,,Sawtooth
5,ns=3;i=1005,1,opc_{type},t_{ns}_{id},val,,,quality,ts,rts,,,Sinusoid
6,ns=3;i=1006,1,opc_{type},t_{ns}_{id},val,,,quality,ts,rts,,,Square
7,ns=3;i=1007,1,opc_{type},t_{ns}_{id},val,,,quality,ts,rts,,,Triangle
```

结果：

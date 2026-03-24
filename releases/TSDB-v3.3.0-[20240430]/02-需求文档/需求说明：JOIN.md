# 需求说明：JOIN

## 1. 引言

### 1.1 术语与缩写名词

不对术语进行特别描述。

### 1.2 相关文档资料

| 文档日期 | 说明 | 链接 | 编写人 |
| --- | --- | --- | --- |
| 2023-11-07 | 需求报告 | [LEFT (SEMI) JOIN需求](https://taosdata.feishu.cn/wiki/VXqiwsuIxiTWSgki00tctn4enuf) | 肖波 |
| 2023-01-07 | 需求报告 | [WINDOW JOIN需求](https://taosdata.feishu.cn/wiki/D9m2wQSxlimi0EkcIKScT4xwnve) | 肖波 |
| 2023-11-11 | 初步设计 | [JOIN 操作分析](https://taosdata.feishu.cn/docx/V8HCdaf25ofWYFxnDMZc1g1dnGg) | Jeff |
| 2023-11-16 | 用户手册 | [Join 功能](https://taosdata.feishu.cn/wiki/NQqNwJirriwpmpkaDbrc4sb6ncg) | 潘魏 |
| 2024-03-04 | FS | [Join 功能](https://taosdata.feishu.cn/wiki/NZIJwC2Iyi6o1DkdCNtcdsVanRb) | 潘魏 |

### 1.3 优先级要求

预期在四月底的 3.2.4.0 版本正式发布

### 1.4 版本要求

社区版

## 2. 需求目标

关联查询基础功能在 3.2.4.0 版本之间就具备，但在金融、工业场景中对关联查询提出了更高要求，近期开发的复合主键和 TIMETRUNCATE 函数，让关联查询的复杂度变得更高。
在 2023 年 11 月开始，JOIN 高级功能的设计、开发工作已经开展，本文仅对需求进行补充说明，把重点放在用户场景分析上，重点描述现有设计文档中不能覆盖的查询功能。

## 3. 功能需求

上海宽睿将是第一个使用 JOIN 功能的用户，我们已经与该公司研发部门建立了密切的联系。该用户会随时在使用中提出需求，也可以忍受少量故障。为了在金融场景中打开局面，除 JOIN 之外，一些工作量的较小的查询类需求也将记录在本文档中。

### 3.1 数据模型

#### 3.1.1 **逐笔委托表：quot_order**

| 列名 | 中文名称 | MySQL 数据类型 | TD 数据类型 | 备注 |
| --- | --- | --- | --- | --- |
| secu_cd | 证券代码 | varchar(20) | varchar(20) | 标签 |
| cmpl_cd | 复合码 | varchar(50) | varchar(50) | 标签 |
| trd_mkt | 交易市场 | varchar(10) | varchar(10) | 标签 |
| secu_type | 证券类型 | varchar(20) | varchar(20) | 标签 |
| ~~trd_day~~ | ~~交易日~~ | ~~date()~~ |  | ~~删除~~ |
| quot_time | 行情时间 | bigint(20) | timestamp |  |
| entr_ordr_no | 委托订单号 | bigint(20) | bigint |  |
| entr_prc | 委托价格 | decimal(12,3) | double |  |
| entr_qty | 委托量 | decimal(22,0) | bigint |  |
| ordr_clas | 订单类别 | varchar(20) | varchar(20) | 可沟通变成 int 类型 |
| entr_ordn | 委托序号 | bigint(20) | bigint |  |
| channel_cd | 频道代码 | bigint(20) | bigint |  |
| busi_sequ_no | 业务序列号 | bigint(20) | bigint |  |
| deal_dir | 买卖方向 | varchar(100) | varchar(100) | 可沟通变成 int 类型 |
| maty | 期限 | int(11) | int |  |
| maty_type | 期限类型 | varchar(20) | varchar(20) | 可沟通变成 int 类型 |
| snd_time | 发送时间 | bigint(20) | timestamp |  |
| pbsh_time | 发布时间 | datetime(6) | timestamp |  |

建表语句如下
```sql {wrap}
create table quot_order (
    quot_time timestamp,
    entr_ordr_no bigint,
    entr_prc double,
    entr_qty bigint,
    ordr_clas varchar(20),
    entr_ordn bigint,
    channel_cd bigint,
    busi_sequ_no bigint,
    deal_dir varchar(100),
    maty int,
    maty_type varchar(20),
    snd_time timestamp,
    pbsh_time timestamp
)
tags (
    secu_cd varchar(20),
    cmpl_cd varchar(50),
    trd_mkt varchar(10),
    secu_type varchar(20)
);
```

#### 3.1.2 **逐笔成交表：quot_tick**

| 列名 | 中文名称 | MySQL 数据类型 | TDengine 数据类型 | 备注 |
| --- | --- | --- | --- | --- |
| secu_cd | 证券代码 | varchar(20) | varchar(20) | 标签 |
| cmpl_cd | 复合码 | varchar(50) | varchar(50) | 标签 |
| trd_mkt | 交易市场 | varchar(10) | varchar(10) | 标签 |
| secu_type | 证券类型 | varchar(20) | varchar(20) | 标签 |
| ~~trd_day~~ | ~~交易日~~ | ~~date()~~ |  | ~~删除~~ |
| quot_time | 行情时间 | bigint(20) | timestamp |  |
| mtch_prc | 成交价格 | decimal(12,3) | double |  |
| trvo | 成交量 | decimal(22,0) | bigint |  |
| mtch_amt | 成交金额 | decimal(22,3) | double |  |
| buyr_ordr_no | 买方订单号 | bigint(20) | bigint |  |
| sler_ordr_no | 卖方订单号 | bigint(20) | bigint |  |
| mtch_clas | 成交类别 | varchar(20) | varchar(20) | 可沟通变成 int 类型 |
| trd_ordn | 成交序号 | bigint(20) | bigint |  |
| channel_cd | 频道代码 | bigint(20) | bigint |  |
| busi_sequ_no | 业务序列号 | bigint(20) | bigint |  |
| sell_buy_flag | 内外盘标志 | varchar(4) | varchar(4) | 可沟通变成 int 类型 |
| snd_time | 发送时间 | bigint(20) | timestamp |  |
| pbsh_time | 发布时间 | datetime(6) | timestamp |  |

建表语句如下
```sql {wrap}
create table quot_tick (
    quot_time timestamp,
    mtch_prc double,
    trvo bigint,
    mtch_amt double,
    buyr_ordr_no bigint,
    sler_ordr_no bigint,
    mtch_clas varchar(20),
    trd_ordn bigint,
    channel_cd bigint,
    busi_sequ_no bigint,
    sell_buy_flag varchar(4),
    snd_time timestamp,
    pbsh_time timestamp
)
tags (
    secu_cd varchar(20),
    cmpl_cd varchar(50),
    trd_mkt varchar(10),
    secu_type varchar(20)
);
```

#### 3.1.3 行情快照**表：quot_lv2**

| 列名 | 中文名称 | MySQL 数据类型 | TD 数据类型 | 备注 |
| --- | --- | --- | --- | --- |
| secu_cd | 证券代码 | varchar(20) | varchar(20) | 标签 |
| cmpl_cd | 复合码 | varchar(50) | varchar(50) | 标签 |
| trd_mkt | 交易市场 | varchar(10) | varchar(10) | 标签 |
| secu_type | 证券类型 | varchar(20) | varchar(20) | 标签 |
| ~~trd_day~~ | ~~交易日~~ | ~~date()~~ |  | ~~删除~~ |
| quot_time | 行情时间 | bigint(20) | timestamp |  |
| snd_time | 发送时间 | bigint(20) | timestamp |  |
| yest_clqn_prc | 昨收价 | decimal(12,3) | double |  |
| opqn_prc | 开盘价 | decimal(12,3) | double |  |
| high_prc | 最高价 | decimal(12,3) | double |  |
| low_prc | 最低价 | decimal(12,3) | double |  |
| last_prc | 最新价 | decimal(12,3) | double |  |
| clqn_prc | 收盘价 | decimal(12,3) | double |  |
| aprc | 均价 | decimal(12,3) | double |  |
| trvo | 成交量 | decimal(22,0) | bigint |  |
| mtch_amt | 成交金额 | decimal(22,5) | double |  |
| mtch_cnt | 成交笔数 | decimal(12,0) | bigint |  |
| pursbuy_ten_prc | 申买十价 | varchar(500) | varchar(500) |  |
| pursbuy_ten_qty | 申买十量 | varchar(500) | varchar(500) |  |
| purssell_ten_prc | 申卖十价 | varchar(500) | varchar(500) |  |
| purssell_ten_qty | 申卖十量 | varchar(500) | varchar(500) |  |
| buy_ten_tot_entr_cnt | 买十总委托笔数 | varchar(1000) | varchar(1000) |  |
| buy_one_prc_entr_queue | 买一价委托队列 | varchar(1000) | varchar(1000) |  |
| sell_ten_tot_entr_cnt | 卖十总委托笔数 | varchar(1000) | varchar(1000) |  |
| sell_one_prc_entr_queue | 卖一价委托队列 | varchar(1000) | varchar(1000) |  |
| entr_buy_tot_qty | 委托买入总量 | decimal(22,0) | bigint |  |
| entr_sell_tot_qty | 委托卖出总量 | decimal(22,0) | bigint |  |
| wght_avg_buy_prc | 加权平均委买价格 | decimal(12,3) | double |  |
| wght_avg_sell_prc | 加权平均委卖价格 | decimal(12,3) | double |  |
| chg1 | 升跌1 | decimal(12,3) | double |  |
| chg2 | 升跌2 | decimal(12,3) | double |  |
| vohp | 持仓量 | decimal(22,0) | bigint |  |
| pera1 | 市盈率1 | decimal(26,6) | double |  |
| pera2 | 市盈率2 | decimal(26,6) | double |  |
| limu_prc | 涨停板价位 | decimal(12,3) | double |  |
| limd_prc | 跌停板价位 | decimal(12,3) | double |  |
| real_phs | 实时阶段 | varchar(20) | varchar(20) | 可沟通变成 int 类型 |
| buy_one_entr_cnt | 买一揭示委托笔数 | decimal(22,0) | bigint |  |
| sell_one_entr_cnt | 卖一揭示委托笔数 | decimal(22,0) | bigint |  |
| buy_tot_cnt | 买入总笔数 | decimal(22,0) | bigint |  |
| sell_tot_cnt | 卖出总笔数 | decimal(22,0) | bigint |  |
| buy_entr_mtch_max_dura | 买入委托成交最大等待时间 | decimal(22,0) | bigint |  |
| sell_entr_mtch_max_dura | 卖出委托成交最大等待时间 | decimal(22,0) | bigint |  |
| buyr_entr_prc_num | 买方委托价位数 | decimal(22,0) | bigint |  |
| sler_entr_prc_num | 卖方委托价位数 | decimal(22,0) | bigint |  |
| buy_whdw_cnt | 买入撤单笔数 | decimal(22,0) | bigint |  |
| buy_whdw_num | 买入撤单数量 | decimal(22,0) | bigint |  |
| buy_whdw_amt | 买入撤单金额 | decimal(12,3) | double |  |
| sell_whdw_cnt | 卖出撤单笔数 | decimal(22,0) | bigint |  |
| sell_whdw_num | 卖出撤单数量 | decimal(22,0) | bigint |  |
| sell_whdw_amt | 卖出撤单金额 | decimal(12,3) | double |  |
| etf_purs_cnt | ETF申购笔数 | decimal(22,0) | bigint |  |
| etf_purs_num | ETF申购数量 | decimal(22,0) | bigint |  |
| etf_purs_amt | ETF申购金额 | decimal(12,3) | double |  |
| etf_redp_cnt | ETF赎回笔数 | decimal(22,0) | bigint |  |
| etf_redp_num | ETF赎回数量 | decimal(22,0) | bigint |  |
| etf_redp_amt | ETF赎回金额 | decimal(12,3) | double |  |
| pbsh_time | 发布时间 | datetime(6) | timestamp |  |

建表语句如下
```sql {wrap}
create table quot_lv2 (
    quot_time timestamp, 
    snd_time timestamp, 
    yest_clqn_prc double,
    opqn_prc double,
    high_prc double,
    low_prc double,
    last_prc double,
    clqn_prc double,
    aprc double,
    trvo bigint, 
    mtch_amt double,
    mtch_cnt bigint,
    entr_buy_tot_qty bigint,
    entr_sell_tot_qty bigint,
    wght_avg_buy_prc double,
    wght_avg_sell_prc double,
    chg1 double,
    chg2 double,
    vohp bigint,
    pera1 double,
    pera2 double,
    limu_prc double,
    limd_prc double,
    real_phs varchar(20),
    buy_one_entr_cnt bigint,
    sell_one_entr_cnt bigint,
    buy_tot_cnt bigint,
    sell_tot_cnt bigint,
    buy_entr_mtch_max_dura bigint,
    sell_entr_mtch_max_dura bigint,
    buyr_entr_prc_num bigint,
    sler_entr_prc_num bigint,
    buy_whdw_cnt bigint,
    buy_whdw_num bigint,
    buy_whdw_amt double,
    sell_whdw_cnt bigint,
    sell_whdw_num bigint,
    sell_whdw_amt double,
    etf_purs_cnt bigint,
    etf_purs_num bigint,
    etf_purs_amt double,
    etf_redp_cnt bigint,
    etf_redp_num bigint,
    etf_redp_amt double,
    pbsh_time timestamp
)
tags (
    secu_cd varchar(20),
    cmpl_cd varchar(50),
    trd_mkt varchar(10),
    secu_type varchar(20)
);
```

#### 3.1.4 样例数据

- 股票 X、Y
  - Order 表
    - 2024-02-26：一条记录
    - 2024-02-27 ：四条记录
      - 两个买入
      - 两个卖出
  - Tick 表
    - 2024-02-26：一条记录
    - 2024-02-27 ：四条记录
      - 一个买入，与 Order 表对应
      - 一个卖出，与 Order 表对应
      - 一个买入撤回
      - 一个卖出撤回
- 快照
  - 不模拟任何数据
```sql
-- Order 
create table x_order using quot_order tags('x', 'x', 'SZ', 'SZ');
create table y_order using quot_order tags('y', 'y', 'SZ', 'SZ');

-- tick
create table x_tick using quot_tick tags('x', 'x', 'SZ', 'SZ');
create table y_tick using quot_tick tags('y', 'y', 'SZ', 'SZ');

-- lv2
create table x_lv2 using quot_lv2 tags('x', 'x', 'SZ', 'SZ');
create table y_lv2 using quot_lv2 tags('y', 'y', 'SZ', 'SZ');

-- test data
insert into x_order(quot_time, entr_ordr_no, deal_dir, entr_qty, channel_cd) values
    ('2024-02-26 09:00:00.000', 1001, 1, 1000, 40)
    ('2024-02-27 09:00:00.000', 1002, 1, 2000, 20)
    ('2024-02-27 09:01:00.000', 1003, 1, 300,  40)
    ('2024-02-27 09:02:00.000', 1004, 2, 4000, 40)
    ('2024-02-27 09:03:00.000', 1005, 2, 5000, 40);

insert into y_order(quot_time, entr_ordr_no, deal_dir, entr_qty, channel_cd) values
    ('2024-02-26 10:00:00.000', 2001, 1, 1001, 50)
    ('2024-02-27 10:00:00.000', 2002, 1, 2001, 30)
    ('2024-02-27 10:01:00.000', 2003, 1, 301,  50)
    ('2024-02-27 10:02:00.000', 2004, 2, 4001, 50)
    ('2024-02-27 10:03:00.000', 2005, 2, 5001, 50);

insert into x_tick(quot_time, buyr_ordr_no, sler_ordr_no, mtch_clas) values
    ('2024-02-26 09:00:01.000', 1001, NULL, 1)
    ('2024-02-27 09:00:02.000', 1002, NULL, 1)
    ('2024-02-27 09:01:03.000', NULL, 1003, 1)
    ('2024-02-27 09:02:04.000', 1002, NULL, 4)
    ('2024-02-27 09:03:05.000', NULL, 1003, 4);

insert into y_tick(quot_time, buyr_ordr_no, sler_ordr_no, mtch_clas) values
    ('2024-02-26 10:00:01.000', 2001, NULL, 1)
    ('2024-02-27 10:00:02.000', 2002, NULL, 1)
    ('2024-02-27 10:01:03.000', NULL, 2003, 1)
    ('2024-02-27 10:02:04.000', 2002, NULL, 4)
    ('2024-02-27 10:03:05.000', NULL, 2003, 4);
```

### 3.2 投影查询和聚合查询

#### 3.2.1 需求

投影查询，带标签列、普通列筛选
聚合查询：通常会使用到 count、sum 等常用聚合函数
```sql {wrap}
select trd_day... from quot_order  
where trd_day >= toDate(#{startDate})  and trd_day <= toDate(#{endDate}) 
and secu_type = #{secuType} 
AND trd_day = #{trdDay} 
AND cmpl_cd = #{cmplCd} 
AND CAST(channel_cd as CHAR) NOT LIKE '40%' 
group by trd_day 
```

TDengine SQL
```sql
-- 投影查询
select quot_time, entr_ordr_no, deal_dir, channel_cd from quot_order
  where quot_time >= to_timestamp('20240226', 'YYYYMMDD') and quot_time <= to_timestamp('20240228', 'YYYYMMDD')
  and secu_type = 'SZ'
  and cmpl_cd = 'x'
  and cast(channel_cd as binary(20)) like '40%';
  
        quot_time        | entr_ordr_no | deal_dir |  channel_cd   |
=====================================================================
 2024-02-26 09:00:00.000 |         1001 | 1        |            40 |
 2024-02-27 09:01:00.000 |         1003 | 1        |            40 |
 2024-02-27 09:02:00.000 |         1004 | 2        |            40 |
 2024-02-27 09:03:00.000 |         1005 | 2        |            40 |
Query OK, 4 row(s) in set (0.015084s)
    
-- 聚合查询    
select cmpl_cd, _wstart, count(*) from quot_order
  where quot_time >= to_timestamp('20240226', 'YYYYMMDD') and quot_time <= to_timestamp('20240228', 'YYYYMMDD')
  and secu_type = 'SZ'
  and cmpl_cd = 'x'
  and cast(channel_cd as binary(20)) like '40%'
  partition by cmpl_cd
  interval(1d);
  
  cmpl_cd   |         _wstart         |       count(*)        |
===============================================================
 x          | 2024-02-26 00:00:00.000 |                     1 |
 x          | 2024-02-27 00:00:00.000 |                     3 |
Query OK, 2 row(s) in set (0.009453s)
```

#### 3.2.2 小结

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R101 | 函数 CAST(expr AS type_name) 的 type_name 为 binary 类型时，不需要指定类型长度 ```sql -- 如下 SQL 报错 select quot_time, entr_ordr_no, deal_dir from quot_order where secu_type = 'SZ' and cmpl_cd = 'x' and cast(channel_cd as binary) like '40%'; DB error: syntax error near ") like '40%' ;" (0.000211s) -- 需要修改成 binary(20) select quot_time, entr_ordr_no, deal_dir from quot_order where secu_type = 'SZ' and cmpl_cd = 'x' and cast(channel_cd as binary(20)) like '40%'; quot_time | entr_ordr_no | deal_dir | ====================================================== 2024-02-26 09:00:00.000 | 1001 | 1 | 2024-02-27 09:01:00.000 | 1003 | 1 | 2024-02-27 09:02:00.000 | 1004 | 2 | 2024-02-27 09:03:00.000 | 1005 | 2 | Query OK, 4 row(s) in set (0.018506s) ``` | 低优先级 |
| R102 | TBNAME 关键字，在为子表设置别名且调用聚合函数时，查询出错 ```sql -- 如下 SQL 报错 select A.tbname, count(A.quot_time) from quot_order A where A.quot_time >= to_timestamp('2024-02-26', 'YYYY-MM-DD') and A.quot_time <= to_timestamp('2024-02-27', 'YYYY-MM-DD') and A.secu_type = 'SZ' and A.cmpl_cd = 'x' and cast(A.channel_cd as binary(20)) like '40%' group by A.tbname; tbname | count(a.quot_time) | =========================================== | 1 | Query OK, 1 row(s) in set (0.004854s) -- 将 A.tbname 修改为 tbname 后仍然出错 select tbname, count(A.quot_time) from quot_order A where A.quot_time >= to_timestamp('2024-02-26', 'YYYY-MM-DD') and A.quot_time <= to_timestamp('2024-02-27', 'YYYY-MM-DD') and A.secu_type = 'SZ' and A.cmpl_cd = 'x' and cast(A.channel_cd as binary(20)) like '40%' group by A.tbname; DB error: Not a GROUP BY expression (0.000642s) -- 将 group by 之后的别名去掉之后，才成功 select tbname, count(A.quot_time) from quot_order A where A.quot_time >= to_timestamp('2024-02-26', 'YYYY-MM-DD') and A.quot_time <= to_timestamp('2024-02-27', 'YYYY-MM-DD') and A.secu_type = 'SZ' and A.cmpl_cd = 'x' and cast(A.channel_cd as binary(20)) like '40%' group by tbname; tbname | count(a.quot_time) | =========================================== x_order | 1 | Query OK, 1 row(s) in set (0.004761s) ``` | 低优先级 |
| R103 | 在 partition by 语法中，如果指定了 tbname ，应该也能查询出该表的其他 tag ```sql {wrap} -- 正确 select tbname, count(quot_time) from quot_order partition by tbname interval(1d); tbname | count(a.quot_time) | ===================================== x_order | 1 | x_order | 4 | y_order | 1 | y_order | 4 | Query OK, 4 row(s) in set (0.003312s) -- cmpl_cd 是标签，通过聚合函数查询，正确 select tbname, first(cmpl_cd), count(quot_time) from quot_order partition by tbname interval(1d); tbname | first(cmpl_cd) | count(a.quot_time) | ===================================================== x_order | x | 1 | x_order | x | 4 | y_order | y | 1 | y_order | y | 4 | Query OK, 4 row(s) in set (0.003867s) -- cmpl_cd 是标签，增加一个 partiton 列，正确 select tbname, cmpl_cd, count(quot_time) from quot_order partition by tbname, cmpl_cd interval(1d); tbname | cmpl_cd | count(quot_time) | ===================================================== y_order | y | 1 | y_order | y | 4 | x_order | x | 1 | x_order | x | 4 | Query OK, 4 row(s) in set (0.003867s) -- cmpl_cd 是标签，当按照 tbname 分组时，其行为应该类似于 first(tbname) select tbname, cmpl_cd, count(quot_time) from quot_order partition by tbname interval(1d); DB error: Invalid usage of expr: cmpl_cd (0.000495s) ``` | 低优先级 |

### 3.3 关联查询

#### 3.3.1 需求

Join 发生在不同主表之间（quot_order、quot_tick），同主表的子表或者不同主表的子表之间一般不会使用 join 函数；Join函数的连接条件列（where 或 on），可能会包含主表的标签列或者普通列。

##### 3.3.1.1 查询一

批量查询逐笔成交里面的每笔单子对应的逐笔委托数据
场景一：从 quot_tick 逐笔成交里，遍历指定时间段的成交记录，查询每笔成交记录对应的 quot_order 中逐笔委托的记录
场景二：从 quot_order 逐笔委托里，遍历指定时间段的委托记录，查询每笔委托记录对应的 quot_tick 中逐笔成交的记录
```sql {wrap}
select * from  quot_tick  A
LEFT JOIN quot_order B 
ON A.cmpl_cd=B.cmpl_cd and A.trd_day=B.trd_day and B.entr_ordr_no=A.buyr_ordr_no AND B.deal_dir='1'
LEFT JOIN quot_tick C 
ON A.cmpl_cd=B.cmpl_cd and A.trd_day=B.trd_day and B.entr_ordr_no=A.sler_ordr_no AND B.deal_dir='2'
```

TDengine SQL
```sql
-- 场景一：一层  LEFT JOIN，1->1
select A.quot_time, A.buyr_ordr_no, A.quot_time, B.entr_ordr_no, B.deal_dir 
  from quot_tick A
  left join quot_order B 
  on 
    A.cmpl_cd = B.cmpl_cd 
    and timetruncate(A.quot_time, 1d) = timetruncate(B.quot_time, 1d)
    and A.buyr_ordr_no = B.entr_ordr_no
    and B.deal_dir = '1'
;

DB error: Planner internal error (0.003136s)
  
-- 场景二：一层 ASOF JOIN, 1->n
select A.quot_time, A.entr_ordr_no, A.deal_dir, B.buyr_ordr_no, B.quot_time
  from quot_order A
  left asof join quot_tick B 
  on 
    A.cmpl_cd = B.cmpl_cd 
    and timetruncate(A.quot_time, 1d) = timetruncate(B.quot_time, 1d)
    and A.entr_ordr_no = B.buyr_ordr_no
  where
    A.deal_dir = '1'
;

DB error: Planner internal error (0.001596s)
  
-- 场景一和场景二的结合：两层 JOIN，LEFT JOIN + LEFT ASOF JOIN
select A.quot_time, A.buyr_ordr_no, A.quot_time, B.entr_ordr_no, B.deal_dir, C.quot_time, C.buyr_ordr_no
  from quot_tick A
  left join quot_order B 
  on 
    A.cmpl_cd = B.cmpl_cd 
    and timetruncate(A.quot_time, 1d) = timetruncate(B.quot_time, 1d)
    and A.buyr_ordr_no = B.entr_ordr_no
  where
    B.deal_dir = '1'
  left asof join quot_tick C
  on 
    B.cmpl_cd = C.cmpl_cd 
    and timetruncate(B.quot_time, 1d) = timetruncate(C.quot_time, 1d)
    and B.entr_ordr_no = C.buyr_ordr_no
  where
    B.deal_dir = '1'
;

DB error: syntax error near 
```

##### 3.3.1.2 查询二

查询指定证券代码、某一天委托量大于 10000 的委托单，对应的成交数据
```sql {wrap}
select * from  quot_tick 
where trd_day='20231027' 
and cmpl_cd in ('','')  
and buyr_ordr_no in （select entr_ordr_no from quot_order where  trd_day='20231027' and cmpl_cd in ('','')  and entr_qty>'10000' ）
```

TDengine SQL
```sql
select A.quot_time, A.buyr_ordr_no, A.sler_ordr_no from quot_tick A
  where timetruncate(A.quot_time, 1d) = to_timestamp('20240226', 'YYYYMMDD')
  and A.cmpl_cd in ('x', 'y')
  and A.buyr_ordr_no in (
    select B.entr_ordr_no from quot_order B
      where timetruncate(B.quot_time, 1d) = to_timestamp('20240226', 'YYYYMMDD') 
      and B.cmpl_cd in ('x', 'y') 
      and B.entr_qty > 10000
  );
  
  DB error: syntax error near...
  
  -- 可用如下 SQL 替代
select A.quot_time, A.buyr_ordr_no, A.sler_ordr_no from quot_tick A 
  left semi join quot_order B on 
    A.buyr_ordr_no = B.entr_ordr_no 
    and timetruncate(B.quot_time, 1d) = to_timestamp('20240226', 'YYYYMMDD')  
  where
    B.cmpl_cd in ('x', 'y')
    and A.cmpl_cd in ('x', 'y')
    and B.entr_qty > 10000;
    
 -- 或者反向 asof
 select A.quot_time, A.buyr_ordr_no, A.sler_ordr_no from quot_order B 
  left asof join quot_tick A on 
    A.buyr_ordr_no = B.entr_ordr_no 
    and timetruncate(B.quot_time, 1d) = to_timestamp('20240226', 'YYYYMMDD')  
  where
    B.cmpl_cd in ('x', 'y')
    and A.cmpl_cd in ('x', 'y')
    and B.entr_qty > 10000;  
```

##### 3.3.1.3 查询三

委托表（quot_order）联合成交表(quot_tick) 查询生成成交的委托单 (符合条件的 quot_order 全部列) 信息
```sql {wrap}
select o.* from quot_order o,quot_lvl2 l where t_date(o.quot_time'%Y%m%d')= t_date(l.quot_time,'%Y%m%d') and o.busi_sequ_no=l.busi_sequ_no and o.cmpl_cd = l.cmpl_cd and o.secu_type=l.secu_type
```

TDengine SQL
```sql {wrap}
select A.* from quot_order A, quot_lv2 B
  where timetruncate(A.quot_time, 1d)= timetruncate(B.quot_time, 1d) 
  and A.cmpl_cd = B.cmpl_cd 
  and A.secu_type = B.secu_type
 
DB error: Planner internal error (0.001792s)
```

#### 3.3.2 分析

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R104 | 常规 LEFT JOIN ```sql select A.quot_time, A.buyr_ordr_no, A.quot_time, B.entr_ordr_no, B.deal_dir from quot_tick A left join quot_order B on A.cmpl_cd = B.cmpl_cd and timetruncate(A.quot_time, 1d) = timetruncate(B.quot_time, 1d) and A.buyr_ordr_no = B.entr_ordr_no where B.deal_dir = '1' ; DB error: Planner internal error (0.005958s) ``` | 高优先级 |
| R105 | ASOF JOIN 需要条件 ```sql select A.quot_time, A.entr_ordr_no, A.deal_dir, B.buyr_ordr_no, B.quot_time from quot_order A left asof join quot_tick B on A.cmpl_cd = B.cmpl_cd and timetruncate(A.quot_time, 1d) = timetruncate(B.quot_time, 1d) and A.entr_ordr_no = B.buyr_ordr_no where A.deal_dir = '1' ; DB error: Planner internal error (0.001596s) ``` | 高优先级 |
| R106 | 两层 JOIN ```sql select A.quot_time, A.buyr_ordr_no, A.quot_time, B.entr_ordr_no, B.deal_dir, C.quot_time, C.buyr_ordr_no from quot_tick A left join quot_order B on A.cmpl_cd = B.cmpl_cd and timetruncate(A.quot_time, 1d) = timetruncate(B.quot_time, 1d) and A.buyr_ordr_no = B.entr_ordr_no where B.deal_dir = '1' left asof join quot_tick C on B.cmpl_cd = C.cmpl_cd and timetruncate(B.quot_time, 1d) = timetruncate(C.quot_time, 1d) and B.entr_ordr_no = C.buyr_ordr_no ; DB error: syntax error near ``` | 中优先级 用户暂时说可以绕开 |
| R107 | 嵌套查询出现错误 ```sql -- 内层查询 select B.entr_ordr_no from quot_order B where timetruncate(B.quot_time, 1d) = to_timestamp('20240226', 'YYYYMMDD') and B.cmpl_cd in ('x', 'y') and B.entr_qty > 10000; entr_ordr_no | ======================== 1001 | 2001 | Query OK, 2 row(s) in set (0.004298s) -- 外层查询 select A.quot_time, A.buyr_ordr_no, A.sler_ordr_no from quot_tick A where timetruncate(A.quot_time, 1d) = to_timestamp('20240226', 'YYYYMMDD') and A.cmpl_cd in ('x', 'y') quot_time | buyr_ordr_no | sler_ordr_no | =========================================================== 2024-02-26 09:00:01.000 | 1001 | 0 | 2024-02-26 10:00:01.000 | 2001 | 0 | Query OK, 2 row(s) in set (0.003872s) -- 查询出错 select A.quot_time, A.buyr_ordr_no, A.sler_ordr_no from quot_tick A where timetruncate(A.quot_time, 1d) = to_timestamp('20240226', 'YYYYMMDD') and A.cmpl_cd in ('x', 'y') and A.buyr_ordr_no in ( select B.entr_ordr_no from quot_order B where timetruncate(B.quot_time, 1d) = to_timestamp('20240226', 'YYYYMMDD') and B.cmpl_cd in ('x', 'y') and B.entr_qty > 10000 ); DB error: syntax error near "select b.entr_ordr_no from quot_order b where timetruncate(b.quot_time, 1d) = to_timestamp('20240226', 'YYYYMMDD') and b.cmpl_cd in ('x', 'y') and b.entr_qty > 10000 );" (0.000421s) ``` | 低优先级 可以用 join 替代 |

### 3.4 合并查询

#### 3.4.1 需求

将 quot_tick 逐笔成交里面撤单数据补充到委托里
```sql {wrap}
select trd_day,cmpl_cd,busi_sequ_no,trvo,MAX(buyr_ordr_no,sler_ordr_no),mtch_clas 
from quot_tick 
where trd_mkt='SZ' 
and mtch_clas='4'
UNION 
select trd_day,cmpl_cd,busi_sequ_no,entr_qty,entr_ordr_no,deal_dir 
from quot_order
where trd_mkt='SZ'
```

TDengine SQL
```sql {wrap}
-- 不去重复
select A.quot_time, A.cmpl_cd, A.buyr_ordr_no, A.mtch_clas
      from quot_tick A
      where  A.trd_mkt = 'SZ' 
      and A.mtch_clas = 4
      and A.buyr_ordr_no is not null
  UNION ALL 
  select B.quot_time, B.cmpl_cd, B.sler_ordr_no, B.mtch_clas
      from quot_tick B
      where B.trd_mkt = 'SZ' 
      and B.mtch_clas = 4
      and B.sler_ordr_no is not null
  UNION ALL
  select C.quot_time, C.cmpl_cd, C.entr_ordr_no, C.deal_dir 
      from quot_order C
      where C.trd_mkt = 'SZ'
```

#### 3.4.2 小结

| 需求编号 | **需求描述** | 研发确认 |
| --- | --- | --- |
| R108 | Max 语法支持多列 ```sql select quot_time, cmpl_cd, MAX(buyr_ordr_no, sler_ordr_no), mtch_clas from quot_tick where trd_mkt = 'SZ' and mtch_clas = 4 and sler_ordr_no is not null; DB error: Invalid number of parameters : max (0.000567s) ``` | 中优先级 |

## 4. 性能需求

在金融场景中，关联条件基于 timetruncate 1d 之后的时间进行，因此测试重点覆盖此状态下的性能。

## 5. 其他需求

无

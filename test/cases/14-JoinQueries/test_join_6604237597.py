from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoin:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join(self):
        """Join with tbname

        1. Create 1 database and 2 super tables with different schemas
        2. Create child tables from super tables with different tag values
        3. Insert data into child tables
        4. Join on timestamps and tags
        5. Check the result of join correctly

        Catalog:
            - Query:Join

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-23 Dapan added for 6604237597

        """

        tdSql.execute(f"drop database if exists sta1;")
        tdSql.execute(f"create database sta1 vgroups 4 duration 100d stt_trigger 1 minrows 10;")
        tdSql.execute(f"create stable sta1.pb_day (ts TIMESTAMP, ymd INT COMPOSITE KEY, insertdae TIMESTAMP, fzl0 INT, fzlless10 INT, fzl10 INT, fzl20 INT, fzl30 INT, fzl40 INT, fzl50 INT, fzl60 INT, fzl70 INT, fzl80 INT, fzl85 INT, fzl90 INT, fzl95 INT, fzl100 INT, fzl110 INT, fzl120 INT, fzl130 INT, fzl140 INT, fzl150 INT, fzlover200 INT, max_lr DOUBLE, max_ts TIMESTAMP, sum_lr DOUBLE, cnt INT) tags (name varchar(32), typedesc varchar(200), bureau nchar(16), pbname nchar(50), rated float);")
        tdSql.execute(f"create stable sta1.unbalance_day (ts TIMESTAMP, ymd INT COMPOSITE KEY, insertdate TIMESTAMP,  maxvalue DOUBLE, minvalue DOUBLE, rate DOUBLE) tags (gis_code varchar(32));")
        tdSql.execute(f"use sta1;")
        tdSql.execute(f"insert into pb01 using pb_day tags(1, NULL, NULL, NULL, NULL) values('2025-10-01 00:00:00.000', 20251001, '2025-12-01 11:08:17.677', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 31, NULL, 41, 11, 101, 271, 391, 581, NULL, NULL, NULL, NULL, 1.2988121646, '2025-10-01 07:30:00.000', 111.19548916048, 91);")
        tdSql.execute(f"insert into pb02 using pb_day tags(2, NULL, NULL, NULL, NULL) values('2025-10-02 00:00:00.000', 20251002, '2025-12-02 11:08:17.677', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 32, NULL, 42, 12, 102, 272, 392, 582, NULL, NULL, NULL, NULL, 2.2988121646, '2025-10-02 07:30:00.000', 112.19548916048, 92);")
        tdSql.execute(f"insert into pb03 using pb_day tags(3, NULL, NULL, NULL, NULL) values('2025-10-03 00:00:00.000', 20251003, '2025-12-03 11:08:17.677', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 33, NULL, 43, 13, 103, 273, 393, 583, NULL, NULL, NULL, NULL, 3.2988121646, '2025-10-03 07:30:00.000', 113.19548916048, 93);")
        tdSql.execute(f"insert into un01 using unbalance_day tags(1) values('2025-10-01 00:00:00.000', 20251001, '2025-12-01 15:43:14.298', 71.989155, 41.018367, 1.2209);")
        tdSql.execute(f"insert into un02 using unbalance_day tags(2) values('2025-10-02 00:00:00.000', 20251002, '2025-12-02 15:43:14.298', 72.989155, 42.018367, 2.2209);")
        tdSql.execute(f"insert into un03 using unbalance_day tags(3) values('2025-10-03 00:00:00.000', 20251003, '2025-12-03 15:43:14.298', 73.989155, 43.018367, 3.2209);")
        
        tdSql.query(f"""select gis_code, bureau, pbname, rated, typedesc, 
        count(*) as day_count, sum(total_dur) as sum_total_dur, 
        sum(total_dur) / count(*) as avg_dur, 
        max(maxvalue) as maxvalue, 
        cols(max(max_lr), max_ts as max_ts), 
        max(max_lr) as peak_loadrate, 
        min(minvalue) as minvalue, 
        max(rate) as max_rate
        from(
        select /*+ hash_join() */ a.gis_code, a.bureau, a.pbname, a.rated, a.typedesc, b.ts, a.total_dur, a.max_lr, a.max_ts, 
        b.maxvalue, b.minvalue, b.rate 
        from
        (select _wstart as ts, name as gis_code, 
        bureau, pbname, rated, typedesc, 
        sum(fzl100) as total_dur, max(max_lr) as max_lr, cols(max(max_lr), ts as max_ts) 
        from pb_day 
        where _rowts between '2025-1-1' and '2025-11-1' 
        and fzl100 > 15  
        partition by tbname interval(1d)
        ) as a, 
        (select ts, gis_code, 
        maxvalue, minvalue, rate 
        from unbalance_day 
        where _rowts between '2025-1-1' and '2025-11-1'
        ) as b 
        where a.gis_code = b.gis_code and a.ts = b.ts
        ) 
        partition by gis_code, bureau, pbname, rated, typedesc;""")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 271)
        tdSql.checkData(0, 7, 271)
        tdSql.checkData(0, 8, 71.989155)
        tdSql.checkData(0, 9, '2025-10-01 00:00:00.000')
        tdSql.checkData(0, 10, 1.2988121646)
        tdSql.checkData(0, 11, 41.018367)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 5, 1)
        tdSql.checkData(1, 6, 272)
        tdSql.checkData(1, 7, 272)
        tdSql.checkData(1, 8, 72.989155)
        tdSql.checkData(1, 9, '2025-10-02 00:00:00.000')
        tdSql.checkData(1, 10, 2.2988121646)
        tdSql.checkData(1, 11, 42.018367)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 5, 1)
        tdSql.checkData(2, 6, 273)
        tdSql.checkData(2, 7, 273)
        tdSql.checkData(2, 8, 73.989155)
        tdSql.checkData(2, 9, '2025-10-03 00:00:00.000')
        tdSql.checkData(2, 10, 3.2988121646)
        tdSql.checkData(2, 11, 43.018367)

        tdSql.query(f"""select gis_code, bureau, pbname, rated, typedesc, 
        count(*) as day_count, sum(total_dur) as sum_total_dur, 
        sum(total_dur) / count(*) as avg_dur, 
        max(maxvalue) as maxvalue, 
        cols(max(max_lr), max_ts as max_ts), 
        max(max_lr) as peak_loadrate, 
        min(minvalue) as minvalue, 
        max(rate) as max_rate
        from(
        select /*+ hash_join() */ a.gis_code, a.bureau, a.pbname, a.rated, a.typedesc, b.ts, a.total_dur, a.max_lr, a.max_ts, 
        b.maxvalue, b.minvalue, b.rate 
        from
        (select ts, gis_code, 
        maxvalue, minvalue, rate 
        from unbalance_day 
        where _rowts between '2025-1-1' and '2025-11-1'
        ) as b, 
        (select _wstart as ts, name as gis_code, 
        bureau, pbname, rated, typedesc, 
        sum(fzl100) as total_dur, max(max_lr) as max_lr, cols(max(max_lr), ts as max_ts) 
        from pb_day 
        where _rowts between '2025-1-1' and '2025-11-1' 
        and fzl100 > 15  
        partition by tbname interval(1d)
        ) as a 
        where a.gis_code = b.gis_code and a.ts = b.ts
        ) 
        partition by gis_code, bureau, pbname, rated, typedesc;""")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 271)
        tdSql.checkData(0, 7, 271)
        tdSql.checkData(0, 8, 71.989155)
        tdSql.checkData(0, 9, '2025-10-01 00:00:00.000')
        tdSql.checkData(0, 10, 1.2988121646)
        tdSql.checkData(0, 11, 41.018367)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 5, 1)
        tdSql.checkData(1, 6, 272)
        tdSql.checkData(1, 7, 272)
        tdSql.checkData(1, 8, 72.989155)
        tdSql.checkData(1, 9, '2025-10-02 00:00:00.000')
        tdSql.checkData(1, 10, 2.2988121646)
        tdSql.checkData(1, 11, 42.018367)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 5, 1)
        tdSql.checkData(2, 6, 273)
        tdSql.checkData(2, 7, 273)
        tdSql.checkData(2, 8, 73.989155)
        tdSql.checkData(2, 9, '2025-10-03 00:00:00.000')
        tdSql.checkData(2, 10, 3.2988121646)
        tdSql.checkData(2, 11, 43.018367)

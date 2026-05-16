from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoin:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join(self):
        """Join with tbname

        1. Create 1 database and 2 super tables with different schemas
        2. Create child tables from super tables with different tag values
        3. Insert data into child tables with same timestamps
        4. Join left table is child query from first super table and where condition 
        5. Join right table is child query from second super table and where condition  
        6. Join on timestamps and tbname tag
        7. Check the result of join correctly

        Catalog:
            - Query:Join

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-1 Dapan added for TS-7170

        """

        tdSql.execute(f"create database db1 vgroups 10;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable stba (attr_time TIMESTAMP,WTUR_maintained TINYINT UNSIGNED) tags(attr_id NCHAR(256));")
        tdSql.execute(f"create stable stbb (attr_time TIMESTAMP,WTUR_active_P FLOAT) tags(attr_id NCHAR(256),wind_farm_id NCHAR(256));")
        tdSql.execute(f"create table tba9 using stba tags('CN_12_N0001_W0001_WTUR00009');")
        tdSql.execute(f"create table tba10 using stba tags('CN_12_N0001_W0001_WTUR00010');")
        tdSql.execute(f"create table tbb9 using stbb tags('CN_12_N0001_W0001_WTUR00009', '0000000000000000000000000000000009');")
        tdSql.execute(f"insert into tba9 (attr_time, WTUR_maintained) values('2025-08-28 00:00:00.000', 0);  ")
        tdSql.execute(f"insert into tba10 (attr_time, WTUR_maintained) values('2025-08-28 00:00:00.000', 0);  ")
        tdSql.execute(f"insert into tbb9 (attr_time, WTUR_active_P) values('2025-08-28 00:00:00.000', 1);")
        tdSql.execute(f"insert into tbb9 (attr_time, WTUR_active_P) values('2025-08-28 00:00:05.000', 2);")
        tdSql.execute(f"insert into tbb9 (attr_time, WTUR_active_P) values('2025-08-28 00:01:05.000', 3);")
        
        tdSql.query(f"select d.*,a.wtur_active_p from (select attr_time,attr_id,wtur_maintained from db1.stba  where tbname like 'tba%' and attr_time>='2025-08-28 00:00:00' and attr_time<='2025-08-28 00:10:00') d  join (select attr_time,attr_id,wtur_active_p from db1.stbb  where tbname like 'tbb%' and attr_time>='2025-08-28 00:00:00' and attr_time<='2025-08-28 00:10:00') a  on timetruncate(d.attr_time, 1m)=timetruncate(a.attr_time, 1m) and d.attr_id=a.attr_id;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 3, 2)

import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream
from datetime import datetime
from datetime import date


class Test_Nevados:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_nevados(self):
        """Nevados

        Refer: https://taosdata.feishu.cn/wiki/XaqbweV96iZVRnkgHLJcx2ZCnQf

        Catalog:
            - Streams:UseCases

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-16 Simon Guan Created

        """

        tdStream.createSnode()
        self.prepare()
        
        # self.windspeeds_hourly()                    # 1
        # self.windspeeds_daily()                     # 3
        self.kpi_db_test()                          # 2
        # self.kpi_trackers_test()                    # 4
        # self.off_target_trackers()                  # 5
        # self.snowdepths_daily()                     # 6
        # self.kpi_zones_test()                       # 7
        # self.kpi_sites_test()                       # 8
        # self.trackers_motor_current_state_window()  # 9
        # self.snowdepths_hourly()                    # 10

    def prepare(self):
        db = "dev"
        stb = "windspeeds"
        precision = "ms"
        start = "2025-06-01 00:00:00"
        interval = 150
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000

        # start = (
        #     datetime.now()
        #     .replace(hour=0, minute=0, second=0, microsecond=0)
        #     .strftime("%Y-%m-%d %H:%M:%S")
        # )
        dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")

        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsInterval = interval * prec
        tdLog.info(f"start={start} tsStart={tsStart}")

        tdLog.info(f"create database {db}")
        tdSql.prepare(dbname=db)

        ##### windspeeds:
        tdLog.info(f"create super table: f{stb}")
        tdSql.execute(
            f"create table {stb}("
            "    _ts TIMESTAMP,"
            "    speed DOUBLE,"
            "    direction DOUBLE"
            ") tags("
            "    id NCHAR(16),"
            "    site NCHAR(16),"
            "    tracker NCHAR(16),"
            "    zone NCHAR(16)"
            ")"
        )

        totalTables = tbBatch * tbPerBatch
        tdLog.info(f"create total {totalTables} child tables")
        for batch in range(tbBatch):
            sql = "create table "
            for tb in range(tbPerBatch):
                table = batch * tbPerBatch + tb
                id = f"id_{table}"
                site = f"site_{table}"
                tracker = f"tracker_{table}"
                zone = f"zone_{table}"
                sql += f"{db}.t{table} using {db}.{stb} tags('{id}', '{site}', '{tracker}', '{zone}')"
            tdSql.execute(sql)

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write {totalRows} rows per table")
        for table in range(totalTables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.t{table} values "
                for row in range(rowsPerBatch):
                    if row >= 100 and row < 400:
                        continue
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    speed = rows
                    direction = rows * 2.0 if rows % 100 < 60 else rows * 0.5
                    sql += f"({ts}, {speed}, {direction}) "
                tdSql.execute(sql)           

    def windspeeds_hourly(self): 
        tdLog.info("windspeeds_hourly")
        # create stream windspeeds_hourly fill_history 1 into windspeeds_hourly as select _wend as window_hourly, site, id, max(speed) as windspeed_hourly_maximum from windspeeds where _ts >= '2025-05-07' partition by site, id interval(1h);
        tdSql.execute(
            "create stream `windspeeds_hourly`"
            "  interval(1h) sliding(1h)"
            "  from windspeeds"
            "  partition by site, id"
            "  stream_options(fill_history('2025-06-01 00:00:00') | pre_filter(_ts >= '2025-05-07'))"
            "  into `windspeeds_hourly` OUTPUT_SUBTABLE(CONCAT('windspeeds_hourly_', cast(site as varchar), cast(id as varchar)))" 
            "  as select _twstart window_start, _twend as window_hourly, max(speed) as windspeed_hourly_maximum from %%trows"
        )
        tdStream.checkStreamStatus()

        tdSql.checkTableSchema(
            dbname="dev",
            tbname="windspeeds_hourly",
            schema=[
                ["window_start", "TIMESTAMP", 8, ""],
                ["window_hourly", "TIMESTAMP", 8, ""],
                ["windspeed_hourly_maximum", "DOUBLE", 8, ""],
                ["site", "NCHAR", 16, "TAG"],
                ["id", "NCHAR", 16, "TAG"],
            ],
        )

        tdSql.checkResultsByFunc(
            "select count(*) from information_schema.ins_tables where db_name='dev' and stable_name='windspeeds_hourly';",
            lambda: tdSql.compareData(0, 0, 10),
        )

        sql = "select window_start, window_hourly, site, id, windspeed_hourly_maximum from dev.windspeeds_hourly where id='id_1';"
        exp_sql = "select _wstart, _wend, site, id, max(speed) from t1 interval(1h);"
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)

        sql = "select count(*) from dev.windspeeds_hourly;"
        exp_sql = "select count(*) from (select _wstart, _wend, site, id, max(speed) from windspeeds partition by tbname interval(1h));"
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)

    def windspeeds_daily(self): 
        tdLog.info("windspeeds_daily")
        # create stream windspeeds_daily fill_history 1 into windspeeds_daily as select _wend as window_daily, site, id, max(windspeed_hourly_maximum) as windspeed_daily_maximum from windspeeds_hourly partition by site, id interval(1d, 5h);
        tdSql.execute(
            "create stream `windspeeds_daily`"
            "  interval(1d, 5h) sliding(1d)"
            "  from windspeeds_hourly"
            "  partition by site, id"
            "  stream_options(fill_history('2025-06-01 00:00:00'))"
            "  into `windspeeds_daily` OUTPUT_SUBTABLE(CONCAT('windspeeds_daily_', cast(site as varchar), cast(id as varchar)))" 
            "  tags("
            "    group_id bigint as _tgrpid"
            "  )"
            "  as select _twstart window_start, _twend as window_hourly, max(windspeed_hourly_maximum) as windspeed_daily_maximum, %%1 as site, %%2 as id from %%trows"
        )
        tdStream.checkStreamStatus()

        tdSql.checkTableSchema(
            dbname="dev",
            tbname="windspeeds_daily",
            schema=[
                ["window_start", "TIMESTAMP", 8, ""],
                ["window_hourly", "TIMESTAMP", 8, ""],
                ["windspeed_daily_maximum", "DOUBLE", 8, ""],
                ["site", "NCHAR", 16, ""],
                ["id", "NCHAR", 16, ""],
                ["group_id", "BIGINT", 8, "TAG"],
            ],
        )

        tdSql.checkResultsByFunc(
            "select count(*) from information_schema.ins_tables where db_name='dev' and stable_name='windspeeds_daily';",
            lambda: tdSql.compareData(0, 0, 10),
        )

        sql = "select window_start, window_hourly, windspeed_daily_maximum from dev.windspeeds_daily where id='id_1';"
        exp_sql = "select _wstart, _wend, max(windspeed_hourly_maximum) from windspeeds_hourly where id='id_1' interval(1d, 5h);"
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)

    def prepare_trackers_tables(self):
        db = "dev"
        stb = "trackers"
        
        ##### trackers stable:
        tdLog.info(f"create super table: trackers")
        tdSql.execute(
            f"create table {db}.{stb}("                    
            "_ts                        TIMESTAMP,"  
            "reg_system_status14        BOOL,"       
            "reg_move_enable14          BOOL,"       
            "reg_move_enable02          BOOL,"       
            "reg_pack7_mv               DOUBLE,"     
            "reg_temp_status05          BOOL,"       
            "reg_system_status02        BOOL,"       
            "reg_temp_status13          BOOL,"       
            "reg_battery_status07       BOOL,"       
            "reg_temp_status08          BOOL,"       
            "reg_system_status15        BOOL,"       
            "reg_motor_ma               DOUBLE,"     
            "reg_temp_status15          BOOL,"       
            "reg_pack5_mv               DOUBLE,"     
            "reg_system_status13        BOOL,"       
            "reg_battery_status02       BOOL,"       
            "reg_temp_status04          BOOL,"       
            "reg_move_enable08          BOOL,"       
            "reg_move_pitch             DOUBLE,"     
            "reg_system_status03        BOOL,"       
            "reg_battery_status12       BOOL,"       
            "reg_system_status04        BOOL,"       
            "reg_temp_status03          BOOL,"       
            "reg_battery_status01       BOOL,"       
            "reg_pack4_mv               DOUBLE,"     
            "reg_move_enable09          BOOL,"       
            "reg_temp_status00          BOOL,"       
            "reg_move_enable10          BOOL,"       
            "reg_panel_mv               DOUBLE,"     
            "reg_move_enable13          BOOL,"       
            "reg_temp_status02          BOOL,"       
            "reg_system_status00        BOOL,"       
            "reg_system_status07        BOOL,"       
            "reg_roll                   DOUBLE,"     
            "reg_battery_mv             DOUBLE,"     
            "reg_temp_status12          BOOL,"       
            "reg_battery_status10       BOOL,"       
            "reg_battery_status15       BOOL,"       
            "reg_temp_status07          BOOL,"       
            "reg_pack1_mv               DOUBLE,"    
            "reg_system_status09        BOOL,"       
            "reg_battery_status06       BOOL,"       
            "reg_move_enable00          BOOL,"       
            "reg_system_status12        BOOL,"       
            "reg_temp_therm2            DOUBLE,"     
            "reg_temp_status10          BOOL,"       
            "reg_motor_temp             DOUBLE,"     
            "reg_pack3_mv               DOUBLE,"     
            "reg_battery_negative_peak  DOUBLE,"     
            "reg_move_enable04          BOOL,"       
            "xbee_signal                DOUBLE,"     
            "reg_temp_status06          BOOL,"       
            "reg_battery_status09       BOOL,"       
            "reg_pack6_mv               DOUBLE,"     
            "reg_temp_status11          BOOL,"       
            "reg_move_enable01          BOOL,"       
            "reg_battery_status08       BOOL,"       
            "reg_move_enable05          BOOL,"       
            "reg_system_status10        BOOL,"       
            "reg_pack2_mv               DOUBLE,"     
            "reg_move_enable15          BOOL,"       
            "reg_firmware_rev           DOUBLE,"     
            "reg_battery_status13       BOOL,"       
            "reg_temp_therm1            DOUBLE,"     
            "reg_move_enable11          BOOL,"       
            "reg_temp_status14          BOOL,"       
            "reg_system_status06        BOOL,"       
            "reg_pitch                  DOUBLE,"     
            "reg_move_enable03          BOOL,"       
            "reg_battery_status14       BOOL,"       
            "reg_system_status08        BOOL,"       
            "reg_battery_status05       BOOL,"       
            "reg_battery_status04       BOOL,"       
            "reg_battery_status03       BOOL,"       
            "reg_battery_status00       BOOL,"       
            "reg_battery_positive_peak  DOUBLE,"     
            "reg_system_status05        BOOL,"       
            "reg_battery_status11       BOOL,"       
            "reg_system_status01        BOOL,"       
            "reg_battery_mA             DOUBLE,"     
            "is_online                  BOOL,"       
            "mode                       VARCHAR(32),"
            "reg_pack8_mv               DOUBLE,"     
            "reg_move_enable06          BOOL,"       
            "reg_temp_status09          BOOL,"       
            "reg_move_enable07          BOOL,"       
            "reg_temp_status01          BOOL,"       
            "reg_move_enable12          BOOL,"       
            "reg_system_status11        BOOL,"       
            "reg_battery_rested_mV      DOUBLE,"     
            "reg_motor_last_move_avg_mA DOUBLE,"     
            "reg_battery_discharge_net  DOUBLE,"     
            "reg_panel_last_charge_mV   DOUBLE,"     
            "reg_serial_number          VARCHAR(4),"
            "reg_motor_last_move_peak_mA DOUBLE,"     
            "reg_panel_last_charge_mA   DOUBLE,"     
            "reg_day_seconds            DOUBLE,"     
            "reg_motor_last_move_min_mV DOUBLE,"     
            "reg_motor_last_move_start_pitch DOUBLE,"     
            "reg_motor_last_move_count DOUBLE)"
            "tags (site NCHAR(8),tracker NCHAR(16),zone NCHAR(16))"
        )
        
        # create sub tables of trackers 
        precision = "ms"
        start = "2025-01-01 00:00:00"
        interval = 150  # s
        # interval = 180  # s
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000
        sub_prefix = "trk_"
        
        dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")

        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else: # ms
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsInterval = interval * prec
        tdLog.info(f"start={start} tsStart={tsStart}") 

        totalTables = tbBatch * tbPerBatch
        tdLog.info(f"create total {totalTables} child tables for trackers")
        for batch in range(tbBatch):
            sql = "create table "
            for tb in range(tbPerBatch):
                table = batch * tbPerBatch + tb
                id = f"id_{table}"
                site = f"site_{table}"
                tracker = f"tracker_{table}"
                zone = f"zone_{table}"
                sql += f"{db}.{sub_prefix}{table} using {db}.{stb} tags('{site}', '{tracker}', '{zone}')"
            tdSql.execute(sql)

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write {totalRows} rows per table")
        for table in range(totalTables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.{sub_prefix}{table} (_ts, reg_system_status14, reg_pitch, reg_move_pitch, reg_temp_therm2, reg_motor_last_move_peak_mA) values "
                for row in range(rowsPerBatch):
                    if row >= 100 and row < 400:
                        continue
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    reg_system_status14 = rows / 2
                    reg_pitch = self.rand_int(1,5)
                    reg_move_pitch = self.rand_int(1,5)
                    reg_temp_therm2 = self.rand_int(-20,70)
                    reg_motor_last_move_peak_mA = self.rand_int(0,1) # bool
                    sql += f"({ts}, {reg_system_status14}, {reg_pitch}, {reg_move_pitch}, {reg_temp_therm2}, {reg_motor_last_move_peak_mA}) "
                tdSql.execute(sql)   

    def insert_real_data_to_trackers(self):
        db = "dev"
        stb = "trackers"
        
        # create sub tables of trackers 
        precision = "ms"
        start = "2025-02-01 00:00:00"
        interval = 150  # s
        # interval = 180  # s
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000
        sub_prefix = "trk_"
        
        dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")

        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else: # ms
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsInterval = interval * prec
        tdLog.info(f"start={start} tsStart={tsStart}") 

        totalTables = tbBatch * tbPerBatch
        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write {totalRows} rows per table")
        for table in range(totalTables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.{sub_prefix}{table} (_ts, reg_system_status14, reg_pitch, reg_move_pitch, reg_temp_therm2, reg_motor_last_move_peak_mA) values "
                for row in range(rowsPerBatch):
                    if row >= 100 and row < 400:
                        continue
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    reg_system_status14 = rows / 2
                    reg_pitch = self.rand_int(1,5)
                    reg_move_pitch = self.rand_int(1,5)
                    reg_temp_therm2 = self.rand_int(-20,70)
                    reg_motor_last_move_peak_mA = self.rand_int(0,1) # bool
                    sql += f"({ts}, {reg_system_status14}, {reg_pitch}, {reg_move_pitch}, {reg_temp_therm2}, {reg_motor_last_move_peak_mA}) "
                tdSql.execute(sql)   

    def kpi_db_test(self):
        db = 'dev'
        sub_prefix = "trk_"
        self.prepare_trackers_tables()
        # tags (site NCHAR(8),tracker NCHAR(16),zone NCHAR(4))
        # create stream if not exists kpi_db_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_db_test 
        # as select _wend as window_end, case when last(_ts) is not null then 1 else 0 end as db_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' interval(1h) sliding(1h);
        tdSql.execute(
            "create stream `kpi_db_test`"
            "  interval(1h) sliding(1h)"
            "  from trackers partition by tbname"
            "  stream_options(fill_history('2025-01-01 00:00:00.000') | watermark(10m) | ignore_disorder | force_output)"
            "  into `kpi_db_test` OUTPUT_SUBTABLE(CONCAT('kpi_db_test_', tbname))"
            "  as select _twend as window_end, case when last(_ts) is not null then 1 else 0 end as db_online, count(*) from %%trows"
        )
        
        self.insert_real_data_to_trackers()
        
        time.sleep(20)
        tdStream.checkStreamStatus()
    
        tdSql.checkResultsByFunc(
            sql=f'select * from information_schema.ins_tables where db_name="{db}" and table_name="kpi_db_test_{sub_prefix}0"',
            func=lambda: tdSql.getRows() >= 1
        )

        sql = f"select * from dev.kpi_db_test_{sub_prefix}0 limit 2000;"
        exp_sql = f"select we, case when lastts is not null then 1 else 0 end as db_online, case when cnt is not null then cnt else 0 end as cnt from (select _wend we, last(_ts) lastts, count(*) cnt from trackers where tbname = '{sub_prefix}0' and _ts >= '2025-01-01 00:00:00.000' and _ts < '2025-02-02 17:00:00.000' interval(1h) fill(null)) limit 2000;"
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)
        

    def kpi_trackers_test(self):
        # create stream if not exists kpi_trackers_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_trackers_test as select _wend as window_end, site, zone, tracker, case when ((min(abs(reg_pitch - reg_move_pitch)) <= 2) or (min(reg_temp_therm2) < -10) or (max(reg_temp_therm2) > 60) or (last(reg_system_status14) = true)) then 1 else 0 end as tracker_on_target, case when last(reg_pitch) is not null then 1 else 0 end as tracker_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by tbname interval(1h) sliding(1h);
        tdSql.execute(
            "create stream `kpi_trackers_test`"
            "  interval(1h) sliding(1h)"
            "  from windspeeds partition by tbname, site, zone, tracker"
            "  stream_options(fill_history('2025-06-01 00:00:00') | watermark(10m) | ignore_disorder | pre_filter(_ts >= '2024-10-04T00:00:00.000Z'))"
            "  into `kpi_trackers_test`"
            "  as select _twstart, _twend as window_end, %%2 as site, %%3 as zone, %%4 as tracker, "
            "   case when ((min(abs(speed - direction)) <= 2) or (min(speed) < -10) or (max(direction) > 60) or (last(speed) = true)) then 1 else 0 end as tracker_on_target, "
            "   case when last(speed) is not null then 1 else 0 end as tracker_online"
            "   from %%trows"
        )

    def off_target_trackers(self):
        # create stream off_target_trackers ignore expired 0 ignore update 0 into off_target_trackers as select _wend as _ts, site, tracker, last(reg_pitch) as off_target_pitch, last(mode) as mode from trackers where _ts >= '2024-04-23' and _ts < now() + 1h and abs(reg_pitch-reg_move_pitch) > 2 partition by site, tracker interval(15m) sliding(5m);
        tdSql.execute(
            "create stream `off_target_trackers`"
            "  interval(15m) sliding(5m)"
            "  from windspeeds partition by site, tracker"
            "  stream_options(pre_filter(_ts >= '2024-04-23' and _ts < now() + 1h and abs(reg_pitch-reg_move_pitch) > 2))"
            "  into `off_target_trackers`"
            "as select _twstart, _twend as window_end, %%2 as site, %%3 as tracker, last(reg_pitch) as off_target_pitch, last(mode) as mode"
            "   from %%trows"
        )

    def kpi_zones_test(self):
        # create stream if not exists kpi_zones_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_zones_test as select _wend as window_end, site, zone, case when last(_ts) is not null then 1 else 0 end as zone_online from trackers where _ts >= '2024-10-04T10:00:00.000Z' partition by site, zone interval(1h) sliding(1h);
        tdSql.execute("")

    def kpi_sites_test(self):
        # create stream if not exists kpi_sites_test trigger window_close watermark 10m fill_history 1 ignore update 1 into  kpi_sites_test as select _wend as window_end, site, case when last(_ts) is not null then 1 else 0 end as site_online from  trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by site interval(1h) sliding(1h);
        tdSql.execute("")

    def trackers_motor_current_state_window(self):
        # create stream trackers_motor_current_state_window into  trackers_motor_current_state_window as select _ts, site, tracker, max(`reg_motor_last_move_peak_mA` / 1000) as max_motor_current from  trackers where _ts >= '2024-09-22' and _ts < now() + 1h and `reg_motor_last_move_peak_mA` > 0 partition by tbname/*, site, tracker */ state_window(cast(reg_motor_last_move_count as int));
        tdSql.execute("")

    def snowdepths_hourly(self):
        # create stream snowdepths_hourly fill_history 1 into  snowdepths_hourly as select _wend as window_hourly, site, id, max(depth) as snowdepth_hourly_maximum from  snowdepths where _ts >= '2024-01-01' partition by site, id interval(1h);
        tdSql.execute(
            "create stream `snowdepths_hourly`"
            "  interval(1h) sliding(1h)"
            "  from windspeeds partition by site, id"
            "  stream_options(fill_history('2025-06-01 00:00:00') | pre_filter(_ts >= '2024-01-01'))"
            "  into `snowdepths_hourly`"
            "as select _twstart, _twend as window_hourly, %%2 as site, %%3 as id, max(depth) as snowdepth_hourly_maximum"
            "   from %%trows"
        )

    def snowdepths_daily(self):
        # create stream snowdepths_daily fill_history 1 into snowdepths_daily as select _wend as window_daily, site, id, max(snowdepth_hourly_maximum) as snowdepth_daily_maximum from snowdepths_hourly partition by site, id interval(1d, 5h);
        tdSql.execute(
            "create stream `snowdepths_daily`"
            "  interval((1d, 5h) "
            "  from snowdepths_hourly partition by site, id"
            "  stream_options(fill_history('2025-06-01 00:00:00'))"
            "  into `snowdepths_daily`"
            "as select _twstart, _twend as window_daily, %%2 as site, %%3 as id, max(snowdepth_hourly_maximum) as snowdepth_daily_maximum"
            "   from %%trows"
        )
        
    def rand_int(self, min_val=1, max_val=10):
        if min_val >= max_val:
            tdLog.exit(f"input val error")
        return random.randint(min_val, max_val)
 

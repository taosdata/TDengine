import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream
from datetime import datetime
from datetime import date
import threading

class Test_Nevados:
    state_val_index = 0

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_usecase_nevados(self):
        """IDMP nevados

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
        tdSql.execute(f"alter all dnodes 'debugflag 131';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 135';")
        
        self.db             = "dev"
        self.precision      = "ms"
        self.windspeeds_stb = "windspeeds"
        self.trackers_stb   = "trackers"
        self.snowdepths_stb = "snowdepths"
        self.history_start_time = "2025-01-01 00:00:00"
        self.real_start_time    = "2025-02-01 00:00:00"
        
        tdLog.info(f"create database {self.db}")
        tdSql.prepare(dbname=self.db, drop=True, vgroups=2)
        
        tdLog.info(f"==== start run windspeeds stable stream")
        # self.prepare_windspeeds(self.db, self.precision, self.windspeeds_stb, self.history_start_time)        
        # self.windspeeds_hourly(self.db, self.precision, self.real_start_time)                    # 1
        # self.windspeeds_daily()                     # 3
        tdLog.info(f"==== end run windspeeds stable stream")
        
        # tdLog.info(f"==== start run trackers stable stream")
        self.trackers_stable_stream_cases(self.db, self.trackers_stb, self.precision, self.trackers_stb, self.history_start_time, self.real_start_time)
        # tdLog.info(f"==== end run trackers stable stream")
        
        # self.prepare_snowdepths(self.db, self.precision, self.snowdepths_stb, self.history_start_time) 
        # self.snowdepths_daily()                     # 6
        # self.snowdepths_hourly()                    # 10        
        
    def trackers_stable_stream_cases(self, db, stb, precision, trackers_stb, history_start_time, real_start_time):        
        self.prepare_trackers(db, precision, trackers_stb, history_start_time) 
        
        # self.kpi_db_test(db, stb, precision, real_start_time)                          # 2
        # self.kpi_trackers_test(db, stb, precision, real_start_time)                    # 4
        # self.off_target_trackers(db, stb, precision, real_start_time)                  # 5
        # self.kpi_zones_test(db, stb, precision, real_start_time)                       # 7
        # self.kpi_sites_test(db, stb, precision, real_start_time)                       # 8
        self.trackers_motor_current_state_window(db, stb, precision, real_start_time)    # 9
        

    def prepare_windspeeds(self, db, precision, stb, history_start_time):
        start = history_start_time
        interval = 150
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000
        
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
        
        ##### windspeeds:
        tdLog.info(f"create super table: f{stb}")
        tdSql.execute(
            f"create table {db}.{stb}("
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

    def windspeeds_real_data(self, db, precision, real_start_time):
        start = real_start_time
        interval = 150
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000
        
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
        totalTables = tbBatch * tbPerBatch
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

    def windspeeds_hourly(self, db, precision, real_start_time): 
        tdLog.info("windspeeds_hourly")
        # create stream windspeeds_hourly fill_history 1 into windspeeds_hourly as select _wend as window_hourly, site, id, max(speed) as windspeed_hourly_maximum from windspeeds where _ts >= '2025-05-07' partition by site, id interval(1h);
        tdSql.execute(
            "create stream `windspeeds_hourly`"
            "  interval(1h) sliding(1h)"
            "  from windspeeds"
            "  partition by site, id"
            "  stream_options(fill_history('2025-01-01 00:00:00') | pre_filter(_ts >= '2025-02-01') | max_delay(3s))"
            "  into `windspeeds_hourly` OUTPUT_SUBTABLE(CONCAT('windspeeds_hourly_', cast(site as varchar), cast(id as varchar)))" 
            "  as select _twstart window_start, _twend as window_hourly, max(speed) as windspeed_hourly_maximum from %%trows"
        )
        tdStream.checkStreamStatus()        
        
        self.windspeeds_real_data(db, precision, real_start_time)

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
            "  stream_options(fill_history('2025-01-01 00:00:00'))"
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

    def prepare_trackers(self, db, precision, stb, history_start_time):
        
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
            "reg_motor_last_move_count DOUBLE,"
            "insert_now_time timestamp)"
            "tags (site NCHAR(8),tracker NCHAR(16),zone NCHAR(16))"
        )
        
        # create sub tables of trackers 
        start = history_start_time
        interval = 150  # s
        # interval = 180  # s
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000
        sub_prefix = "trk"
        
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
                sql = f"insert into {db}.{sub_prefix}{table} (_ts, reg_system_status14, reg_pitch, reg_move_pitch, reg_temp_therm2, reg_motor_last_move_peak_mA, reg_motor_last_move_count, insert_now_time) values "
                for row in range(rowsPerBatch):
                    if row >= 100 and row < 400:
                        continue
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    reg_system_status14 = rows / 2
                    reg_pitch = self.rand_int(1,5)
                    reg_move_pitch = self.rand_int(1,5)
                    reg_temp_therm2 = self.rand_int(-20,70)
                    reg_motor_last_move_peak_mA = self.rand_int(0,1000) # bool
                    reg_motor_last_move_count = self.rand_state_val() 
                    sql += f"({ts}, {reg_system_status14}, {reg_pitch}, {reg_move_pitch}, {reg_temp_therm2}, {reg_motor_last_move_peak_mA}, {reg_motor_last_move_count}, now) "
                tdSql.execute(sql)   

    def trackers_real_data(self, db, stb, precision, real_start_time):
        delete_sql = f"delete from {stb} where _ts >= '{real_start_time}'"
        tdSql.execute(delete_sql)
        
        start = real_start_time
        interval = 150  # s
        # interval = 180  # s
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000
        sub_prefix = "trk"
        
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
                sql = f"insert into {db}.{sub_prefix}{table} (_ts, reg_system_status14, reg_pitch, reg_move_pitch, reg_temp_therm2, reg_motor_last_move_peak_mA, reg_motor_last_move_count, insert_now_time) values "
                for row in range(rowsPerBatch):
                    if row >= 100 and row < 400:
                        continue
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    reg_system_status14 = rows / 2
                    reg_pitch = self.rand_int(1,5)
                    reg_move_pitch = self.rand_int(1,5)
                    reg_temp_therm2 = self.rand_int(-20,70)
                    reg_motor_last_move_peak_mA = self.rand_int(0,1000) # bool
                    reg_motor_last_move_count = self.rand_state_val()
                    sql += f"({ts}, {reg_system_status14}, {reg_pitch}, {reg_move_pitch}, {reg_temp_therm2}, {reg_motor_last_move_peak_mA}, {reg_motor_last_move_count}, now) "
                tdSql.execute(sql)   

    def trackers_real_data_interlace_mode(self, db, stb, precision, real_start_time):
        delete_sql = f"delete from {stb} where _ts >= '{real_start_time}'"
        tdSql.execute(delete_sql)
        
        start = real_start_time
        interval = 150  # s
        # interval = 180  # s
        tbBatch = 1
        tbPerBatch = 10
        rowBatch = 1
        rowsPerBatch = 1000
        sub_prefix = "trk"
        
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

        # totalTables = tbBatch * tbPerBatch
        # totalRows = rowsPerBatch * rowBatch
        
        totalRows = 700
        totalTables = 10
        ts = tsStart
        tdLog.info(f"write {totalRows} rows per table by interlace mode")
        
        for row in range(totalRows):   
            reg_motor_last_move_count = self.rand_state_val()         
            sql = f"insert into "
            for table in range(totalTables):
                sql += f"{db}.{sub_prefix}{table} (_ts, reg_system_status14, reg_pitch, reg_move_pitch, reg_temp_therm2, reg_motor_last_move_peak_mA, reg_motor_last_move_count, insert_now_time) values "   
                
                reg_system_status14 = row / 2
                reg_pitch = self.rand_int(1,5)
                reg_move_pitch = self.rand_int(1,5)
                reg_temp_therm2 = self.rand_int(-20,70)
                reg_motor_last_move_peak_mA = self.rand_int(0,1000) # bool
                # reg_motor_last_move_count = self.rand_state_val()
                # tdLog.info(f"ts: {ts}, reg_motor_last_move_count: {reg_motor_last_move_count}")
                sql += f"({ts}, {reg_system_status14}, {reg_pitch}, {reg_move_pitch}, {reg_temp_therm2}, {reg_motor_last_move_peak_mA}, {reg_motor_last_move_count}, now) "
                        
            ts += tsInterval
            tdSql.execute(sql) 


    def kpi_db_test(self, db, stb, precision, real_start_time):
        sub_prefix = "trk"
        # tags (site NCHAR(8),tracker NCHAR(16),zone NCHAR(4))
        # create stream if not exists kpi_db_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_db_test 
        # as select _wend as window_end, case when last(_ts) is not null then 1 else 0 end as db_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' interval(1h) sliding(1h);
        
        tdLog.info(f"create stream kpi_db_test")
        tdSql.execute(
            "create stream `kpi_db_test`"
            "  interval(1h) sliding(1h)"
            "  from trackers partition by tbname"
            "  stream_options(fill_history('2025-01-01 00:00:00.000') | watermark(10m) | ignore_disorder | force_output)"
            "  into `kpi_db_test` OUTPUT_SUBTABLE(CONCAT('kpi_db_test_', tbname))"
            "  as select _twend as window_end, case when last(_ts) is not null then 1 else 0 end as db_online, count(*) from %%trows"
        )
        
        self.trackers_real_data(db, stb, precision, real_start_time)

        tdStream.checkStreamStatus()
        
        loop_cnt = 0
        for loop_cnt in range(60):
            tdSql.query(f'select * from information_schema.ins_tables where db_name="{db}" and table_name="kpi_db_test_{sub_prefix}0"')
            result_rows = tdSql.getRows()
            if result_rows == 1:
                tdLog.info(f"wait {loop_cnt} loop for stream result table")
                break
            time.sleep(2)
        if loop_cnt == 60:
            tdLog.exit(f"kpi_db_test stream not result table")

        sql = f"select * from dev.kpi_db_test_{sub_prefix}0;"
        exp_sql = (f"select we, case when lastts is not null then 1 else 0 end as db_online," 
                   f" case when cnt is not null then cnt else 0 end as cnt" 
                   f" from (select _wend we, last(_ts) lastts, count(*) cnt" 
                           f" from trackers where tbname = '{sub_prefix}0' and _ts >= '2025-01-01 00:00:00.000' and _ts < '2025-02-02 17:00:00.000' interval(1h) fill(null));")
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)
        tdLog.info(f"check stream kpi_db_test result end")

    def kpi_trackers_test(self, db, stb, precision, real_start_time):
        sub_prefix = "trk"
        # create stream if not exists kpi_trackers_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_trackers_test 
        # as select _wend as window_end, site, zone, tracker, 
        #           case when ((min(abs(reg_pitch - reg_move_pitch)) <= 2) 
        #                   or (min(reg_temp_therm2) < -10) or (max(reg_temp_therm2) > 60) or (last(reg_system_status14) = true)) 
        #                then 1 else 0 end as tracker_on_target, 
        #           case when last(reg_pitch) is not null then 1 else 0 end as tracker_online 
        #    from trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by tbname interval(1h) sliding(1h);
        
        tdLog.info(f"create stream kpi_trackers_test")
        tdSql.execute(
            "create stream `kpi_trackers_test`"
            "  interval(1h) sliding(1h)"
            "  from trackers partition by tbname, site, zone, tracker"
            "  stream_options(fill_history('2025-01-01 00:00:00') | watermark(10m) | ignore_disorder)"
            "  into `kpi_trackers_test` OUTPUT_SUBTABLE(CONCAT('kpi_trackers_test_', tbname))"
            "  as select _twend as we, %%2 as out_site, %%3 as out_zone, %%4 as out_tracker, "
            "   case when ((min(abs(reg_pitch - reg_move_pitch)) <= 2) or (min(reg_temp_therm2) < -10) or (max(reg_temp_therm2) > 60) or (last(reg_system_status14) = true)) then 1 else 0 end as tracker_on_target,"
            "   case when last(reg_pitch) is not null then 1 else 0 end as tracker_online"
            "   from %%trows"
        )
        
        self.trackers_real_data(db, stb, precision, real_start_time)
        
        tdStream.checkStreamStatus()
        
        loop_cnt = 0
        for loop_cnt in range(60):
            tdSql.query(f'select * from information_schema.ins_tables where db_name="{db}" and table_name="kpi_trackers_test_{sub_prefix}0"')
            result_rows = tdSql.getRows()
            if result_rows == 1:
                tdLog.info(f"wait {loop_cnt} loop for stream result table")
                break
            time.sleep(2)
            
        if loop_cnt == 60:
            tdLog.exit(f"kpi_trackers_test stream not result table")

        sql = f"select * from dev.kpi_trackers_test_{sub_prefix}0;"
        exp_sql = (f"select _wend, site, zone, tracker," 
                  f" case when ((min(abs(reg_pitch - reg_move_pitch)) <= 2)" 
                  f" or (min(reg_temp_therm2) < -10)"
                  f" or (max(reg_temp_therm2) > 60)"
                  f" or (last(reg_system_status14) = true)) then 1 else 0 end as tracker_on_target,"
                  f" case when last(reg_pitch) is not null then 1 else 0 end as tracker_online"
                  f" from trackers where tbname = '{sub_prefix}0' and _ts >= '2025-01-01 00:00:00.000' and _ts < '2025-02-02 17:00:00.000'"
                  f" partition by tbname,site,zone,tracker interval(1h);")
        tdLog.info(f"exp_sql: {exp_sql}")          
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)        
        tdLog.info(f"check stream kpi_trackers_test result end")

    def off_target_trackers(self, db, stb, precision, real_start_time):
        sub_prefix = 'trk'
        # create stream off_target_trackers 
        #               ignore expired 0 ignore update 0 
        # into off_target_trackers 
        # as select _wend as _ts, site, tracker, last(reg_pitch) as off_target_pitch, last(mode) as mode 
        #           from trackers where _ts >= '2024-04-23' and _ts < now() + 1h 
        #                           and abs(reg_pitch-reg_move_pitch) > 2 
        #                           partition by site, tracker interval(15m) sliding(5m);
        tdLog.info(f"create stream off_target_trackers")
        tdSql.execute(
            "create stream `off_target_trackers`"
            " interval(15m) sliding(5m)"
            " from trackers partition by tbname, site, tracker"
            " stream_options(pre_filter(_ts >= '2024-04-23' and _ts < now() + 1h and abs(reg_pitch-reg_move_pitch) > 2))"
            " into `off_target_trackers` OUTPUT_SUBTABLE(CONCAT('off_target_trackers_', tbname))"
            " as select _twend as window_end, %%2 as out_site, %%3 as out_tracker, last(reg_pitch) as off_target_pitch, last(mode) as mode"
            " from %%trows"
        )
        
        self.trackers_real_data(db, stb, precision, real_start_time)
        
        tdStream.checkStreamStatus()
        
        loop_cnt = 0
        for loop_cnt in range(60):
            tdSql.query(f'select * from information_schema.ins_tables where db_name="{db}" and table_name="off_target_trackers_{sub_prefix}0"')
            result_rows = tdSql.getRows()
            if result_rows == 1:
                tdLog.info(f"wait {loop_cnt} loop for stream result table")
                break
            time.sleep(2)
            
        if loop_cnt == 60:
            tdLog.exit(f"off_target_trackers stream not result table")

        sql = f"select * from dev.off_target_trackers_{sub_prefix}0;"
        exp_sql = (f"select _wend as window_end, site, tracker," 
                  f" last(reg_pitch) as off_target_pitch, last(mode) as mode" 
                  f" from trackers where tbname = '{sub_prefix}0' and _ts >= '2025-01-01 00:00:00.000' and _ts < '2025-02-02 17:00:00.000'"
                  f" partition by site,tracker interval(15m) sliding(5m);")
        tdLog.info(f"exp_sql: {exp_sql}")          
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)        
        tdLog.info(f"check stream off_target_trackers result end") 

    def kpi_zones_test(self, db, stb, precision, real_start_time):
        sub_prefix = "trk"
        # create stream if not exists kpi_zones_test 
        #               trigger window_close watermark 10m fill_history 1 ignore update 1 
        # into kpi_zones_test 
        # as select 
        #           _wend as window_end, 
        #           site, zone, 
        #           case when last(_ts) is not null then 1 else 0 end as zone_online 
        # from trackers where _ts >= '2024-10-04T10:00:00.000Z' partition by site, zone interval(1h) sliding(1h);         
        tdLog.info(f"create stream kpi_zones_test")
        tdSql.execute(
            "create stream `kpi_zones_test`"
            "  interval(1h) sliding(1h)"
            "  from trackers partition by tbname, site, zone"
            "  stream_options(fill_history('2025-01-01 00:00:00') | watermark(10m) | ignore_disorder)"
            "  into `kpi_zones_test` OUTPUT_SUBTABLE(CONCAT('kpi_zones_test_', tbname))"
            "  as select _twend as we, %%2 as out_site, %%3 as out_zone,"
            "  case when last(_ts) is not null then 1 else 0 end as zone_online"
            "  from %%trows"
        )
        
        self.trackers_real_data(db, stb, precision, real_start_time)
        
        tdStream.checkStreamStatus()
        
        loop_cnt = 0
        for loop_cnt in range(60):
            tdSql.query(f'select * from information_schema.ins_tables where db_name="{db}" and table_name="kpi_zones_test_{sub_prefix}0"')
            result_rows = tdSql.getRows()
            if result_rows == 1:
                tdLog.info(f"wait {loop_cnt} loop for stream result table")
                break
            time.sleep(2)
            
        if loop_cnt == 60:
            tdLog.exit(f"kpi_zones_test stream not result table")

        sql = f"select * from dev.kpi_zones_test_{sub_prefix}0;"
        exp_sql = (f"select _wend, site, zone," 
                  f" case when last(_ts) is not null then 1 else 0 end as zone_online" 
                  f" from trackers where tbname = '{sub_prefix}0' and _ts >= '2025-01-01 00:00:00.000' and _ts < '2025-02-02 17:00:00.000'"
                  f" partition by tbname,site,zone interval(1h);")
        tdLog.info(f"exp_sql: {exp_sql}")          
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)        
        tdLog.info(f"check stream kpi_zones_test result end")

    def kpi_sites_test(self, db, stb, precision, real_start_time):
        # create stream if not exists kpi_sites_test 
        #        trigger window_close watermark 10m fill_history 1 ignore update 1 
        # into kpi_sites_test 
        # as select _wend as window_end, site, 
        #           case when last(_ts) is not null then 1 else 0 end as site_online 
        # from  trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by site interval(1h) sliding(1h);
       
        sub_prefix = "trk"      
        tdLog.info(f"create stream kpi_sites_test")
        tdSql.execute(
            "create stream `kpi_sites_test`"
            "  interval(1h) sliding(1h)"
            "  from trackers partition by tbname, site"
            "  stream_options(fill_history('2025-01-01 00:00:00') | watermark(10m) | ignore_disorder)"
            "  into `kpi_sites_test` OUTPUT_SUBTABLE(CONCAT('kpi_sites_test_', tbname))"
            "  as select _twend as we, %%2 as out_site,"
            "  case when last(_ts) is not null then 1 else 0 end as site_online"
            "  from %%trows"
        )
        
        self.trackers_real_data(db, stb, precision, real_start_time)
        
        tdStream.checkStreamStatus()
        
        loop_cnt = 0
        for loop_cnt in range(60):
            tdSql.query(f'select * from information_schema.ins_tables where db_name="{db}" and table_name="kpi_sites_test_{sub_prefix}0"')
            result_rows = tdSql.getRows()
            if result_rows == 1:
                tdLog.info(f"wait {loop_cnt} loop for stream result table")
                break
            time.sleep(2)
            
        if loop_cnt == 60:
            tdLog.exit(f"kpi_sites_test stream not result table")

        sql = f"select * from dev.kpi_sites_test_{sub_prefix}0;"
        exp_sql = (f"select _wend, site," 
                  f" case when last(_ts) is not null then 1 else 0 end as site_online"
                  f" from trackers where tbname = '{sub_prefix}0' and _ts >= '2025-01-01 00:00:00.000' and _ts < '2025-02-02 17:00:00.000'"
                  f" partition by tbname,site interval(1h);")
        tdLog.info(f"exp_sql: {exp_sql}")          
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)        
        tdLog.info(f"check stream kpi_sites_test result end")

    def trackers_motor_current_state_window(self, db, stb, precision, real_start_time):
        # create stream trackers_motor_current_state_window 
        # into  trackers_motor_current_state_window 
        # as select _ts, site, tracker, 
        #           max(`reg_motor_last_move_peak_mA` / 1000) as max_motor_current 
        # from  trackers where _ts >= '2024-09-22' and _ts < now() + 1h 
        #       and `reg_motor_last_move_peak_mA` > 0 partition by tbname, site, tracker state_window(cast(reg_motor_last_move_count as int));       
        sub_prefix = "trk"      
        tdLog.info(f"create stream trackers_motor_current_state_window")
        tdSql.execute(
            f"create stream `trackers_motor_current_state_window`"
            f"  state_window(cast(`reg_motor_last_move_count` as int))"
            f"  from trackers partition by tbname, site, tracker"
            f"  stream_options(pre_filter(_ts >= '2025-01-01' and _ts < now() + 1h and reg_motor_last_move_peak_mA > 0))"
            f"  into `trackers_state_window` OUTPUT_SUBTABLE(CONCAT('trackers_state_window_', tbname))"
            f"  as select _ts, %%2 as out_site, %%3 as out_tracker,"
            f"  max(reg_motor_last_move_peak_mA / 1000) as max_motor_current"
            f"  from %%trows;"
        )
        
        tdStream.checkStreamStatus()
        tdSql.query(f"select * from information_schema.ins_streams where stream_name = 'trackers_motor_current_state_window';")
        tdLog.info(f"stream_name: {tdSql.getData(0,0)}")
        tdLog.info(f"status: {tdSql.getData(0,5)}")
        tdLog.info(f"message: {tdSql.getData(0,8)}")
        
        self.trackers_real_data_interlace_mode(db, stb, precision, real_start_time)
        
        tdStream.checkStreamStatus()
        
        loop_cnt = 0
        for loop_cnt in range(60):
            tdSql.query(f'select * from information_schema.ins_tables where db_name="{db}" and table_name="trackers_state_window_{sub_prefix}0"')
            result_rows = tdSql.getRows()
            if result_rows == 1:
                tdLog.info(f"stream result table after {loop_cnt} loop times ")
                loop_cnt = None
                break
            time.sleep(2)
            tdLog.info(f"waiting {loop_cnt} loop for stream result table")        
        
        tdLog.info(f"last wait {loop_cnt} loop for stream result table")    
        if loop_cnt != None:
            tdLog.exit(f"trackers_motor_current_state_window stream not result table")

        sql = f"select * from dev.trackers_state_window_{sub_prefix}0;"
        exp_sql = (f"select _ts, site, tracker," 
                  f" max(reg_motor_last_move_peak_mA / 1000) as max_motor_current"
                  f" from trackers where tbname = '{sub_prefix}0' and _ts >= '{real_start_time}' and _ts < now() + 1h and reg_motor_last_move_peak_mA > 0"
                  f" partition by tbname,site,tracker state_window(cast(reg_motor_last_move_count as int));")
        tdLog.info(f"exp_sql: {exp_sql}")          
        tdSql.checkResultsBySql(sql=sql, exp_sql=exp_sql)        
        tdLog.info(f"check stream trackers_motor_current_state_window result end")

    def snowdepths_hourly(self, db, stb, precision, real_start_time):
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

    def snowdepths_daily(self, db, stb, precision, real_start_time):
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
        
    def rand_state_val(self):
        state_val_list = [1,2,2,3,3,3,4,4,4,4,5,5,5,5,5,6,6,6,6,6,6,7,7,7,7,7,7,7,8,8,8,8,8,8,8,8,9,9,9,9,9,9,9,9,9]        
        
        # tdLog.info(f"Test_Nevados.state_val_index: {Test_Nevados.state_val_index}")
        ret_val = state_val_list[Test_Nevados.state_val_index]
        Test_Nevados.state_val_index += 1
        Test_Nevados.state_val_index %= len(state_val_list)
        return ret_val
 

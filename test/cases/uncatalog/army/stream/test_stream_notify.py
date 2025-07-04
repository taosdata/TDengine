###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql
import signal
import subprocess

class StreamNotifyServer:
    def __init__(self):
        self.log_file = ""
        self.sub_process = None

    def __del__(self):
        self.stop()

    def test_stream_notify(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx
        History:
            - xxx
            - xxx
        """
        tdLog.info(f"Start notify server: python3 {etool.curFile(__file__, 'stream_notify_server.py')} -p {port} -d {log_file}")
        self.sub_process = subprocess.Popen(['python3', etool.curFile(__file__, 'stream_notify_server.py'), '-p', str(port), '-d', log_file])
        self.log_file = log_file

        if self.sub_process is not None:
            self.sub_process.send_signal(signal.SIGINT)
            try:
                self.sub_process.wait(60)
            except subprocess.TimeoutExpired:
                self.sub_process.kill()

class TestStreamNotifySinglePass():
    def __init__(self, num_addr_per_stream, trigger_mode, notify_event, disorder):
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.num_addr_per_stream = num_addr_per_stream
        self.trigger_mode = trigger_mode
        self.notify_event = notify_event
        self.disorder = disorder
        self.streams = []
        self.id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    @staticmethod
    def is_port_in_use(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("127.0.0.1", port)) == 0

    def gen_streams(self):
        self.streams = [
            {
                "stream_name": "s_time_par",
                "dest_table": "dst_time_par",
                "window_clause": "interval(5s)",
                "partitioned": True,
            },
            {
                "stream_name": "s_time",
                "dest_table": "dst_time",
                "window_clause": "interval(5s)",
                "partitioned": False,
            },
            {
                "stream_name": "s_state_par",
                "dest_table": "dst_state_par",
                "window_clause": "state_window(c6)",
                "partitioned": True,
            },
            {
                "stream_name": "s_session_par",
                "dest_table": "dst_session_par",
                "window_clause": "session(ts, 50a)",
                "partitioned": True,
            },
            {
                "stream_name": "s_session",
                "dest_table": "dst_session",
                "window_clause": "session(ts, 50a)",
                "partitioned": False,
            },
            {
                "stream_name": "s_event_par",
                "dest_table": "dst_event_par",
                "window_clause": "event_window start with c6 = true end with c6 = false",
                "partitioned": True,
            },
            {
                "stream_name": "s_count_par",
                "dest_table": "dst_count_par",
                "window_clause": "count_window(10)",
                "partitioned": True,
            },
        ]
        # set port to random number between 10000 and 20000
        port = random.randint(10000, 20000)
        for stream in self.streams:
            stream["notify_address"] = ""
            stream["notify_server"] = []
            if self.trigger_mode == "FORCE_WINDOW_CLOSE":
                if stream["stream_name"] == "s_time" or stream["stream_name"] == "s_session":
                    continue
            elif "MAX_DELAY" in self.trigger_mode or "AT_ONCE" in self.trigger_mode or self.disorder:
                if stream["stream_name"] == "s_session_par" or stream["stream_name"] == "s_state_par":
                    continue
            for i in range(self.num_addr_per_stream):
                # Find an available port
                while TestStreamNotifySinglePass.is_port_in_use(port):
                    port += 1
                # Start the stream notify server and add the address to the stream
                log_file = f"{self.current_dir}/{self.id}_{stream['stream_name']}_{i}.log"
                if os.path.exists(log_file):
                    os.remove(log_file)
                server = StreamNotifyServer()
                server.run(port, log_file)
                stream["notify_address"] += f"'ws://127.0.0.1:{port}',"
                stream["notify_server"].append(server)
                port += 1
            stream["notify_address"] = stream["notify_address"][:-1]

    def create_streams(self):
        tdLog.info("==========step1:create table")
        tdSql.execute("drop database if exists test;")
        tdSql.execute("create database test keep 3650;")
        tdSql.execute("use test;")
        tdSql.execute(
            f"""create stable if not exists test.st
            (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10), c9 tinyint unsigned, c10 smallint unsigned, c11 int unsigned, c12 bigint unsigned)
            tags(groupid int);
            """
        )
        for stream in self.streams:
            if len(stream["notify_server"]) == 0:
                continue
            stream_option = f"TRIGGER {self.trigger_mode}"
            if self.trigger_mode != "FORCE_WINDOW_CLOSE":
                stream_option += " IGNORE UPDATE 0"
                if not stream["stream_name"].startswith("s_count"):
                    stream_option += " IGNORE EXPIRED 0"
            if stream["stream_name"].startswith("s_count"):
                stream_option += " WATERMARK 1a"
            stream_sql = f"""create stream {stream["stream_name"]} {stream_option} into {stream["dest_table"]} as
                        select _wstart, _wend, min(c0), max(c1), min(c2), max(c3), min(c4), max(c5), first(c6), first(c7), last(c8), min(c9), max(c10), min(c11), max(c12)
                        from test.st {stream["partitioned"] and "partition by tbname" or ""}
                        {stream["window_clause"]}
                        notify ({stream["notify_address"]}) on ({self.notify_event});
                        """
            tdSql.execute(stream_sql, show=True)
        # Wait for the stream tasks to be ready
        for i in range(50):
            tdLog.info(f"i={i} wait for stream tasks ready ...")
            time.sleep(1)
            rows = tdSql.query("select * from information_schema.ins_stream_tasks where status <> 'ready';")
            if rows == 0:
                break

    def insert_data(self):
        tdLog.info("insert stream notify test data.")
        # taosBenchmark run
        json_file = self.disorder and "stream_notify_disorder.json" or "stream_notify.json"
        json = etool.curFile(__file__,  json_file)
        etool.benchMark(json=json)

    def wait_all_streams_done(self):
        while True:
            tdLog.info("wait for all streams done ...")
            time.sleep(10)
            rows = tdSql.query("select stream_name, level, notify_event_stat from information_schema.ins_stream_tasks where notify_event_stat is not null;")
            num_pushed = 0
            num_sent = 0
            for i in range(rows):
                tdLog.printNoPrefix(f"{tdSql.getData(i, 0)}, {tdSql.getData(i, 1)}, {tdSql.getData(i, 2)}")
                notify_event_stat = tdSql.getData(i, 2)
                match = re.search(r"Push (\d+)x, (\d+) elems", notify_event_stat)
                if match:
                    num_pushed += int(match.group(2))
                match = re.search(r"Send (\d+)x, (\d+) elems", notify_event_stat)
                if match:
                    num_sent += int(match.group(2))
            if num_pushed == num_sent:
                break
        tdLog.info("wait for all notify servers stop ...")
        for stream in self.streams:
            for server in stream["notify_server"]:
                server.stop()

    def parse(self, log_file, out_file, stream_name):
        message_ids = set()
        events_map = {}
        has_open = "window_open" in self.notify_event
        has_close = "window_close" in self.notify_event
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                data = json.loads(line)

                # Check if the data has the required fields: messageId, timestamp, streams
                if "messageId" not in data:
                    print(f"Error: Missing 'messageId' in data {data}")
                    return False
                if "timestamp" not in data:
                    print(f"Error: Missing 'timestamp' in data {data}")
                    return False
                if "streams" not in data:
                    print(f"Error: Missing 'streams' in data {data}")
                    return False

                # Check if the message id is duplicated
                if message_ids.__contains__(data["messageId"]):
                    print(f"Error: Duplicate message id {data['messageId']}")
                    return False
                message_ids.add(data["messageId"])

                # Check if the streams is correct
                for stream in data["streams"]:
                    # Check if the stream has the required fields: streamName, events
                    if "streamName" not in stream:
                        print(f"Error: Missing 'streamName' in stream {stream}")
                        return False
                    if "events" not in stream:
                        print(f"Error: Missing 'events' in stream {stream}")
                        return False

                    # Check if the stream name is correct
                    if stream["streamName"] != stream_name:
                        print(f"Error: Incorrect stream name {stream['streamName']}")
                        return False

                    # Check if the events are correct
                    for event in stream["events"]:
                        # Check if the event has the required fields: tableName, eventType, eventTime, windowId, windowType
                        if "tableName" not in event:
                            print(f"Error: Missing 'tableName' in event {event}")
                            return False
                        if "eventType" not in event:
                            print(f"Error: Missing 'eventType' in event {event}")
                            return False
                        if "eventTime" not in event:
                            print(f"Error: Missing 'eventTime' in event {event}")
                            return False
                        if "windowId" not in event:
                            print(f"Error: Missing 'windowId' in event {event}")
                            return False
                        if "windowType" not in event:
                            print(f"Error: Missing 'windowType' in event {event}")
                            return False
                        if event["eventType"] not in [
                            "WINDOW_OPEN",
                            "WINDOW_CLOSE",
                            "WINDOW_INVALIDATION",
                        ]:
                            print(f"Error: Invalid event type {event['eventType']}")
                            return False
                        if event["windowType"] not in [
                            "Time",
                            "State",
                            "Session",
                            "Event",
                            "Count",
                        ]:
                            print(f"Error: Invalid window type {event['windowType']}")
                            return False

                        if event["eventType"] == "WINDOW_INVALIDATION":
                            if not has_close:
                                print(f"Error: WINDOW_INVALIDATION event is not allowed")
                                return False
                            # WINDOW_INVALIDATION must have fields: windowStart, windowEnd
                            if "windowStart" not in event:
                                print(f"Error: Missing 'windowStart' in event {event}")
                                return False
                            if "windowEnd" not in event:
                                print(f"Error: Missing 'windowEnd' in event {event}")
                                return False
                            events_map.pop(
                                (event["tableName"], event["windowId"]), None
                            )
                            continue

                        # Get the event from the event map; if it doesn't exist, create a new one
                        e = events_map.get((event["tableName"], event["windowId"]))
                        if e is None:
                            events_map[(event["tableName"], event["windowId"])] = {
                                "opened": False,
                                "closed": False,
                                "wstart": 0,
                                "wend": 0,
                            }
                            e = events_map.get((event["tableName"], event["windowId"]))

                        if event["eventType"] == "WINDOW_OPEN":
                            if not has_open:
                                print(f"Error: WINDOW_OPEN event is not allowed")
                                return False
                            # WINDOW_OPEN for all windows must have field: windowStart
                            if "windowStart" not in event:
                                print(f"Error: Missing 'windowStart' in event {event}")
                                return False
                            if event["windowType"] == "State":
                                # WINDOW_OPEN for State window must also have fields: prevState, curState
                                if "prevState" not in event:
                                    print(
                                        f"Error: Missing 'prevState' in event {event}"
                                    )
                                    return False
                                if "curState" not in event:
                                    print(f"Error: Missing 'curState' in event {event}")
                                    return False
                            elif event["windowType"] == "Event":
                                # WINDOW_OPEN for Event window must also have fields: triggerCondition
                                if "triggerCondition" not in event:
                                    print(
                                        f"Error: Missing 'triggerCondition' in event {event}"
                                    )
                                    return False
                            e["opened"] = True
                            e["wstart"] = event["windowStart"]
                        elif event["eventType"] == "WINDOW_CLOSE":
                            if not has_close:
                                print(f"Error: WINDOW_CLOSE event is not allowed")
                                return False
                            # WINDOW_CLOSE for all windows must have fields: windowStart, windowEnd, result
                            if "windowStart" not in event:
                                print(f"Error: Missing 'windowStart' in event {event}")
                                return False
                            if "windowEnd" not in event:
                                print(f"Error: Missing 'windowEnd' in event {event}")
                                return False
                            if "result" not in event:
                                print(f"Error: Missing 'result' in event {event}")
                                return False
                            if event["windowType"] == "State":
                                # WINDOW_CLOSE for State window must also have fields: curState, nextState
                                if "curState" not in event:
                                    print(f"Error: Missing 'curState' in event {event}")
                                    return False
                                if "nextState" not in event:
                                    print(
                                        f"Error: Missing 'nextState' in event {event}"
                                    )
                                    return False
                            elif event["windowType"] == "Event":
                                # WINDOW_CLOSE for Event window must also have fields: triggerCondition
                                if "triggerCondition" not in event:
                                    print(
                                        f"Error: Missing 'triggerCondition' in event {event}"
                                    )
                                    return False
                            e["closed"] = True
                            e["wstart"] = event["windowStart"]
                            e["wend"] = event["windowEnd"]

        # Collect all the windows that closed
        windows_map = {}
        for k, v in events_map.items():
            if not v["closed"]:
                continue
            e = windows_map.get(k[0])
            if e is None:
                windows_map[k[0]] = []
                e = windows_map.get(k[0])
            e.append((v["wstart"], v["wend"]))

        # Sort the windows by start time
        for k, v in windows_map.items():
            v.sort(key=lambda x: x[0])

        # Write all collected window info to the specified output file in sorted order as csv format
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("wstart,wend,tbname\n")
            for k, v in sorted(windows_map.items()):
                for w in v:
                    f.write(f"{w[0]},{w[1]},\"{k}\"\n")
        return True

    def check_notify_result(self):
        all_right = True
        for stream in self.streams:
            if len(stream["notify_server"]) == 0 or not "window_close" in self.notify_event:
                continue
            query_file = f"{self.current_dir}/{self.id}_{stream['stream_name']}.csv"
            query_sql = f"select cast(_wstart as bigint) as wstart, cast(_wend as bigint) as wend, tbname from test.{stream['dest_table']} order by tbname, wstart >> {query_file};"
            tdLog.info("query_sql: " + query_sql)
            os.system(f"taos -c {tdCom.getClientCfgPath()} -s '{query_sql}'")
            for i in range(self.num_addr_per_stream):
                server = stream["notify_server"][i]
                parse_file = f"{self.current_dir}/{self.id}_{stream['stream_name']}_{i}.csv"
                if os.path.exists(parse_file):
                    os.remove(parse_file)
                if not self.parse(f"{self.current_dir}/{self.id}_{stream['stream_name']}_{i}.log", parse_file, stream["stream_name"]):
                    tdLog.exit(f"Error: {stream['stream_name']}_{i} parse notify result failed")
                # Compare the result using diff command
                diff_file = f"{self.current_dir}/{self.id}_{stream['stream_name']}_{i}.diff"
                if os.path.exists(diff_file):
                    os.remove(diff_file)
                os.system(f"diff --strip-trailing-cr {query_file} {parse_file} > {diff_file}")
                if os.path.getsize(diff_file) != 0:
                    tdLog.info(f"Error: {stream['stream_name']}_{i} notify result is not correct")
                    all_right = False
        if not all_right:
            raise Exception("Error: notify result is not correct")

    def drop_all_streams(self):
        for stream in self.streams:
            tdSql.execute(f"drop stream if exists {stream['stream_name']};")
            # Also remove all generaetd files
            query_file = f"{self.current_dir}/{self.id}_{stream['stream_name']}.csv"
            if os.path.exists(query_file):
                os.remove(query_file)
            for i in range(self.num_addr_per_stream):
                log_file = f"{self.current_dir}/{self.id}_{stream['stream_name']}_{i}.log"
                parse_file = f"{self.current_dir}/{self.id}_{stream['stream_name']}_{i}.csv"
                diff_file = f"{self.current_dir}/{self.id}_{stream['stream_name']}_{i}.diff"
                if os.path.exists(log_file):
                    os.remove(log_file)
                if os.path.exists(parse_file):
                    os.remove(parse_file)
                if os.path.exists(diff_file):
                    os.remove(diff_file)

    def test_stream_notify(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx
        History:
            - xxx
            - xxx
        """
        tdLog.info(f"Start to execute TestStreamNotifySinglePass({self.num_addr_per_stream}, {self.trigger_mode}, {self.notify_event}, {self.disorder})")
        self.gen_streams()
        self.create_streams()
        self.insert_data()
        self.wait_all_streams_done()
        self.check_notify_result()
        self.drop_all_streams()
        tdLog.info(f"TestStreamNotifySinglePass({self.num_addr_per_stream}, {self.trigger_mode}, {self.notify_event}, {self.disorder}) successfully executed")

class TestStreamNotify:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def ts_6375(self):
        port = random.randint(10000, 20000)
        while TestStreamNotifySinglePass.is_port_in_use(port):
            port += 1
        log_file = os.path.dirname(os.path.abspath(__file__)) + f"/ts_6375.log"
        if os.path.exists(log_file):
            os.remove(log_file)
        server = StreamNotifyServer()
        server.run(port, log_file)


        tdSql.execute("drop database if exists ts_6375;")
        tdSql.execute("create database ts_6375 keep 3650;")
        tdSql.execute("use ts_6375;")
        tdSql.execute("create stable st(ts timestamp, current int, voltage int) tags (gid int);")
        tdSql.execute("create table ct0 using st(gid) tags (0);")
        tdSql.execute(
            f"""create stream s_6375 into dst as
                select _wstart+1s, min(current) as min_current from ct0 interval(1m)
                notify('ws://127.0.0.1:{port}') on ('WINDOW_OPEN', 'WINDOW_CLOSE');""")
        # Wait for the stream tasks to be ready
        for i in range(50):
            tdLog.info(f"i={i} wait for stream tasks ready ...")
            time.sleep(1)
            rows = tdSql.query("select * from information_schema.ins_stream_tasks where status <> 'ready';")
            if rows == 0:
                break
        tdSql.execute(
            f"""insert into ct0 values
                ('2025-01-01 00:00:00', 0, 0),
                ('2025-01-01 00:01:00', 1, 1),
                ('2025-01-01 00:02:00', 2, 2),
                ('2025-01-01 00:03:00', 3, 3),
                ('2025-01-01 00:04:00', 4, 4);"""
        )
        tdLog.info(f"wait for stream tasks done ...")
        time.sleep(5)
        server.stop()

        tdSql.query("select * from dst;")
        tdSql.checkRows(4)

    def test_stream_notify(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx
        History:
            - xxx
            - xxx
        """
        self.ts_6375()

        # Disable many tests due to long execution time

        # TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="AT_ONCE", notify_event="'window_open', 'window_close'", disorder=False).run()
        # TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="MAX_DELAY 10s", notify_event="'window_open', 'window_close'", disorder=False).run()
        TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="WINDOW_CLOSE", notify_event="'window_open', 'window_close'", disorder=False).run()
        # TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="FORCE_WINDOW_CLOSE", notify_event="'window_open', 'window_close'", disorder=False).run()

        # TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="AT_ONCE", notify_event="'window_open', 'window_close'", disorder=True).run()
        # TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="MAX_DELAY 10s", notify_event="'window_open', 'window_close'", disorder=True).run()
        TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="WINDOW_CLOSE", notify_event="'window_open', 'window_close'", disorder=True).run()
        # TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="FORCE_WINDOW_CLOSE", notify_event="'window_open', 'window_close'", disorder=True).run()

        # TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="AT_ONCE", notify_event="'window_close'", disorder=False).run()
        TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="MAX_DELAY 10s", notify_event="'window_close'", disorder=False).run()
        # TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="WINDOW_CLOSE", notify_event="'window_close'", disorder=False).run()
        # TestStreamNotifySinglePass(num_addr_per_stream=1, trigger_mode="FORCE_WINDOW_CLOSE", notify_event="'window_close'", disorder=False).run()

        TestStreamNotifySinglePass(num_addr_per_stream=3, trigger_mode="AT_ONCE", notify_event="'window_open'", disorder=False).run()
        # TestStreamNotifySinglePass(num_addr_per_stream=3, trigger_mode="MAX_DELAY 10s", notify_event="'window_open'", disorder=False).run()
        # TestStreamNotifySinglePass(num_addr_per_stream=3, trigger_mode="WINDOW_CLOSE", notify_event="'window_open'", disorder=False).run()
        # TestStreamNotifySinglePass(num_addr_per_stream=3, trigger_mode="FORCE_WINDOW_CLOSE", notify_event="'window_open'", disorder=False).run()

        tdLog.success(f"{__file__} successfully executed")



import random
import string
import re
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream

# Common time units
duration_lists = [
    "1b", "1u", "1a", "1s", "1m", "1h", "1d", "1w", "1n", "1y",
    "2b", "2u", "2a", "2s", "2m", "2h", "2d", "2w", "2n", "2y",
    "5b", "5u", "5a", "5s", "5m", "5h", "5d", "5w", "5n", "5y",
    "8b", "8u", "8a", "8s", "8m", "8h", "8d", "8w", "8n", "8y",
    "12b", "12u", "12a", "12s", "12m", "12h", "12d", "12w", "12n", "12y",
    "30b", "30u", "30a", "30s", "30m", "30h", "30d", "30w", "30n", "30y",
    "1365b", "1365u", "1365a", "1365s", "1365m", "1365h", "1365d", "1365w", "1365n", "1365y"
]

out_columns = ["out_col1", "out_col2", "out_col3", "out_col4", "out_col5", "out_col6", "out_col7", "out_col8", "out_col9", "out_col10"]
out_tags = ["out_tag1", "out_tag2", "out_tag3", "out_tag4", "out_tag5", "out_tag6", "out_tag7", "out_tag8", "out_tag9", "out_tag10"]
counts = [10, 100]
slidings = [1, 5]
ops = ["=", "<>", "!=", ">", "<", ">=", "<="]
arith_ops = ["+", "-", "*", "/"]
logic_ops = ["AND", "OR"]
notify_option_list = ["NOTIFY_HISTORY", "ON_FAILURE_PAUSE"]

query_table_col_list = ["ts", "q_col1", "q_col2", "q_col3", "q_col4", "q_col5", "q_col6"]

as_subquery_opts = [(" AS SELECT * FROM query_table", True, 7, ["ts", "q_col1", "q_col2", "q_col3", "q_col4", "q_col5", "q_col6"], False),
                    (" AS SELECT first(ts), count(q_col1) from query_table", True, 2, ["first(ts)", "count(q_col1)"], False),
                    (" AS SELECT first(ts), count(q_col1) from query_table WHERE q_col1 > 0", True, 2, ["first(ts)", "count(q_col1)"], False),
                    (" AS SELECT first(ts), count(q_col1), count(q_col2) from query_table WHERE q_col1 > 0 INTERVAL(1s)", True, 3, ["first(ts)", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT first(ts), count(q_col1), count(q_col2) from query_table WHERE q_col1 > 0 GROUP BY q_col2", True, 3, ["first(ts)", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT first(ts), count(q_col1) from query_table WHERE q_col1 > 0 GROUP BY q_col2 HAVING count(q_col1) > 0", True, 2, ["first(ts)", "count(q_col1)"], False),
                    (" AS SELECT first(ts), count(q_col1) from query_table WHERE q_col1 > 0 GROUP BY q_col2 HAVING count(q_col1) > 0 ORDER BY q_col2", True, 2, ["first(ts)", "count(q_col1)"], False),
                    (" AS SELECT _tcurrent_ts, count(q_col1), count(q_col2) from query_table", True, 3, ["_tcurrent_ts", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT _tcurrent_ts, count(q_col1), count(q_col2) from query_table WHERE _tcurrent_ts > 1", True, 3, ["_tcurrent_ts", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT _twstart, count(q_col1), count(q_col2) from query_table WHERE _twstart > 1", True, 3, ["_twstart", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT _twend, count(q_col1), count(q_col2) from query_table WHERE _twend > 1", True, 3, ["_twend", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT _twduration, count(q_col1), count(q_col2) from query_table WHERE _twduration > 1", False, 3, ["_twduration", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT _twrownum, count(q_col1), count(q_col2) from query_table WHERE _twrownum > 1", False, 3, ["_twrownum", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT _tgrpid, count(q_col1), count(q_col2) from query_table WHERE _tgrpid > 1", False, 3, ["_tgrpid", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT _tlocaltime, count(q_col1), count(q_col2) from query_table WHERE _tlocaltime > 1", True, 3, ["_tlocaltime", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT _twstart, %%1, count(q_col1), count(q_col2) from query_table", True, 4, ["_twstart", "%%1", "count(q_col1)", "count(q_col2)"], True),
                    (" AS SELECT %%tbname, count(q_col1), count(q_col2) from query_table", False, 3, ["%%tbname", "count(q_col1)", "count(q_col2)"], False),
                    (" AS SELECT 1 from %%tbname", False, 1, ["1"], False),
                    (" AS SELECT q_col1 from %%tbname", False, 1, ["q_col1"], False),
                    (" AS SELECT _twstart, %%1 from %%tbname", True, 2, ["_twstart", "%%1"], True),
                    (" AS SELECT q_col1, q_col2 from %%trows", False, 2, ["q_col1", "q_col2"], False),
                    (" AS SELECT col1, col2 from %%trows", True, 2, ["col1", "col2"], False),
                    (" AS SELECT _twstart, q_col1, q_col2 from %%trows", False, 3, ["_twstart", "q_col1", "q_col2"], False),
                    ("", True, 0, None, False)]

if_not_exists_opts = ["", " IF NOT EXISTS"]

db_name_list_valid = ["", "create_stream_db."]
db_name_list_invalid = ["non_exists_db."]

trigger_table_list_valid = ["trigger_table", "trigger_stable", "trigger_ctable"]
trigger_table_list_invalid = ["non_exists_table", ""]

# TODO(smj) : add different column_num and tag_num's table
into_option_list_valid = [
    " INTO create_stream_db.new_table",
    " INTO create_stream_db.exist_super_table",
    " INTO create_stream_db.exist_normal_table",
    " INTO new_table",
    " INTO exist_super_table",
    " INTO exist_normal_table",
    ""
]

into_option_list_invalid = [
    " INTO non_exists_db.new_table",
    " INTO exist_sub_table",
    " INTO create_stream_db.exist_sub_table",
]

# trigger_table(col1 timestamp, col2 int, col3 int, col4 int, col5 int, col6 int)
# trigger_stable(col1 timestamp, col2 int, col3 int, col4 int, col5 int, col6 int) tags(tag1 int, tag2 int, tag3 int, tag4 int)
# exist_super_table(out_col1 timestamp, out_col2 int, out_col3 int) tags(out_tag1 int, out_tag2 int)
# exist_normal_table(out_col1 timestamp, out_col2 int, out_col3 int)
# query_table(ts timestamp, col1 int, col2 int, col3 int, col4 int, col5 int, col6 int)

session_column_valid = ["col1"]
session_column_invalid = ["col2", "col3", "col4", "col5", "col6", "ts_col", "tag1", "tag2", "tag3", "tag4"]

state_column_valid = ["col2", "col3", "col4", "col5", "col6"]
state_column_invalid = ["col1", "col7", "col8", "col9", "col10", "ts_col", "tag5", "tag6", "tag7", "tag8"]

event_count_column_valid = ["col1", "col2", "col3", "col4", "col5", "col6", "tag1", "tag2", "tag3", "tag4"]
event_count_column_invalid = ["col7", "col8", "col9", "col10", "ts_col", "tag5", "tag6", "tag7", "tag8"]

trigger_column_valid = ["col1", "col2", "col3", "col4", "col5", "col6"]
trigger_column_invalid = ["col7", "col8", "col9", "col10", "ts_col", "tag1", "tag2", "tag3", "tag4"]

trigger_tag_valid = ["tag1", "tag2", "tag3", "tag4"]
trigger_tag_invalid = ["tag5", "tag6", "tag7", "tag8", "col1", "col2", "col3", "col4", "col5", "col6"]

partition_columns_valid = ["tag1", "tag2", "tag3", "tag4"]#, "tbname"]
partition_columns_invalid = ["ts_col", "tag5", "tag6", "now"]

duration_lists_valid = [
    "1b", "1u", "1a", "1s", "1m", "1h", "1d", "1w", "1n", "1y",
    "2b", "2u", "2a", "2s", "2m", "2h", "2d", "2w", "2n", "2y",
    "5b", "5u", "5a", "5s", "5m", "5h", "5d", "5w", "5n", "5y",
    "12b", "12u", "12a", "12s", "12m", "12h", "12d", "12w", "12n", "12y",
    "30b", "30u", "30a", "30s", "30m", "30h", "30d", "30w", "30n", "30y",
    "1365b", "1365u", "1365a", "1365s", "1365m", "1365h", "1365d", "1365w", "1365n", "1365y"
]

duration_lists_invalid = [
    "1x", "2x", "5x", "8x", "12x", "30x", "1365x",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
]

slide_lists_valid = [
    "1a", "1s", "1m", "1h", "1d", "1w",
    "2a", "2s", "2m", "2h", "2d", "2w",
    "5a", "5s", "5m", "5h", "5d", "5w",
    "12a", "12s", "12m", "12h", "12d", "12w",
    "30a", "30s", "30m", "30h", "30d", "30w",
    "1365a", "1365s", "1365m", "1365h", "1365d", "1365w",
]

slide_lists_invalid = [
    "1x", "2x", "5x", "8x", "12x", "30x", "1365x",
    "1b", "2b", "5b", "8b", "12b", "30b", "1365b",
    "1u", "2u", "5u", "8u", "12u", "30u", "1365u",
    "1n", "2n", "5n", "8n", "12n", "30n", "1365n",
    "1y", "2y", "5y", "8y", "12y", "30y", "1365y",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
]

slide_offset_lists_valid = [
    "1a", "1s", "1m", "1h",
    "2a", "2s", "2m", "2h",
    "5a", "5s", "5m", "5h",
    "8a", "8s", "8m", "8h",
    "12a", "12s", "12m", "12h",
    "30a", "30s", "30m", "30h",
    "1365a", "1365s", "1365m", "1365h",
]

slide_offset_lists_invalid = [
    "1x", "2x", "5x", "8x", "12x", "30x", "1365x",
    "1b", "2b", "5b", "8b", "12b", "30b", "1365b",
    "1u", "2u", "5u", "8u", "12u", "30u", "1365u",
    "1d", "2d", "5d", "8d", "12d", "30d", "1365d",
    "1w", "2w", "5w", "8w", "12w", "30w", "1365w",
    "1n", "2n", "5n", "8n", "12n", "30n", "1365n",
    "1y", "2y", "5y", "8y", "12y", "30y", "1365y",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
]

session_lists_valid = [
    "1a", "1s", "1m", "1h", "1d", "1w",
    "2a", "2s", "2m", "2h", "2d", "2w",
    "5a", "5s", "5m", "5h", "5d", "5w",
    "8a", "8s", "8m", "8h", "8d", "8w",
    "12a", "12s", "12m", "12h", "12d", "12w",
    "30a", "30s", "30m", "30h", "30d", "30w",
    "1365a", "1365s", "1365m", "1365h", "1365d", "1365w",
]

session_lists_invalid = [
    "1x", "2x", "5x", "8x", "12x", "30x", "1365x",
    "1n", "2n", "5n", "8n", "12n", "30n", "1365n",
    "1y", "2y", "5y", "8y", "12y", "30y", "1365y",
    "1b", "2b", "5b", "8b", "12b", "30b", "1365b",
    "1u", "2u", "5u", "8u", "12u", "30u", "1365u",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
]

true_for_lists_valid = [
    "1b", "1u", "1a", "1s", "1m", "1h", "1d", "1w",
    "2b", "2u", "2a", "2s", "2m", "2h", "2d", "2w",
    "5b", "5u", "5a", "5s", "5m", "5h", "5d", "5w",
    "8b", "8u", "8a", "8s", "8m", "8h", "8d", "8w",
    "12b", "12u", "12a", "12s", "12m", "12h", "12d", "12w",
    "30b", "30u", "30a", "30s", "30m", "30h", "30d", "30w",
    "1365b", "1365u", "1365a", "1365s", "1365m", "1365h", "1365d", "1365w",
]

true_for_lists_invalid = [
    "1x", "2x", "5x", "8x", "12x", "30x", "1365x",
    "1n", "2n", "5n", "8n", "12n", "30n", "1365n",
    "1y", "2y", "5y", "8y", "12y", "30y", "1365y",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
]

expired_time_list_valid = [
    "1a", "1s", "1m", "1h", "1d",
    "2a", "2s", "2m", "2h", "2d",
    "5a", "5s", "5m", "5h", "5d",
    "8a", "8s", "8m", "8h", "8d",
    "12a", "12s", "12m", "12h", "12d",
    "30a", "30s", "30m", "30h", "30d",
    "1365a", "1365s", "1365m", "1365h", "1365d",
]

expired_time_list_invalid = [
    "1x", "2x", "5x", "8x", "12x", "30x", "1365x",
    "1b", "2b", "5b", "8b", "12b", "30b", "1365b",
    "1u", "2u", "5u", "8u", "12u", "30u", "1365u",
    "1w", "2w", "5w", "8w", "12w", "30w", "1365w",
    "1n", "2n", "5n", "8n", "12n", "30n", "1365n",
    "1y", "2y", "5y", "8y", "12y", "30y", "1365y",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
]

# 10a - 13650d
period_time_list_valid = [
    "10a", "10s", "10m", "10h", "10d",
    "20a", "20s", "20m", "20h", "20d",
    "120a", "120s", "120m", "120h", "120d",
    "360a", "360s", "360m", "360h", "360d",
    "820a", "820s", "820m", "820h", "820d",
    "1440a", "1440s", "1440m", "1440h", "1440d",
    "13650a", "13650s", "13650m", "13650h", "13650d"
]

period_time_list_invalid = [
    "10x", "20x", "120x", "360x", "820x", "1440x", "13650x",
    "10b", "20b", "120b", "360b", "820b", "1440b", "13650b",
    "10u", "20u", "120u", "360u", "820u", "1440u", "13650u",
    "10w", "20w", "120w", "360w", "820w", "1440w", "13650w",
    "10n", "20n", "120n", "360n", "820n", "1440n", "13650n",
    "10y", "20y", "120y", "360y", "820y", "1440y", "13650y",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
]

period_offset_list_valid = [
    "1a", "1s", "1m", "1h",
    "2a", "2s", "2m", "2h",
    "5a", "5s", "5m", "5h",
    "8a", "8s", "8m", "8h",
    "12a", "12s", "12m", "12h",
    "30a", "30s", "30m", "30h",
]

period_offset_list_invalid = [
    "1x", "2x", "5x", "8x", "12x", "30x",
    "1b", "2b", "5b", "8b", "12b", "30b",
    "1u", "2u", "5u", "8u", "12u", "30u",
    "1w", "2w", "5w", "8w", "12w", "30w",
    "1n", "2n", "5n", "8n", "12n", "30n",
    "1y", "2y", "5y", "8y", "12y", "30y",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
]

start_time_valid = [" '2025-05-27 14:29:42' ", " '1970-01-01 08:00:00' ",
                    " 1748327382161 ", " 1 "]

start_time_invalid = [" '2025-05-27 14:29:42:00' ", " '1970-01-01 08:00:00:00' ",
                      " 2025-05-27 14:29:42 ", " 1970-01-01 08:00:00 ",
                      " 2025-05-27 14:29:42:00 ", " 1970-01-01 08:00:00:00 ",
                      " 1748327382161:00 ", " 1:00 ", " '2025-05-27 14:29' ",
                      " '2025-05-27' ", " '2025-05' ", " '2025' ",
                      " '2025-05-27T14:29:42Z' ", " '2025-05-27T14:29Z' ",
                      " '2025-05-27T14Z' ", " '2025-05-27T' ",
                      " '2025-05T14:29:42Z' ", " '2025-05T14Z' ",
                      " '2025-05T' ", " '2025T14:29:42Z' ", " '2025T14Z' ",
                      " '2025T' ", "'invalid_time'", "'invalid_date'",
                      "'invalid_timestamp'", "'invalid_format'",
                      "'another_invalid_format'", "'yet_another_invalid_format'"]

event_types_valid = ["WINDOW_OPEN", "WINDOW_CLOSE"]

event_types_invalid = ["INVALID_EVENT", "ANOTHER_INVALID_EVENT"]

urls_valid = [" 'http://example.com/notify' ", " 'http://localhost:8000/callback' ", " 'https://api.test.com/hook' "]
urls_invalid = [" 'invalid_url' ", " http://example.com/invalid ", " 12345678 "]

notify_option_valid = ["NOTIFY_HISTORY", "ON_FAILURE_PAUSE"]
notify_option_invalid = ["NOTIFY_WHAT", "NOTIFY_INVALID"]

out_tag_type_valid = ["VARCHAR(30)"]
out_tag_type_invalid = ["INVALID_TYPE", "BIGINT", "INT"]

string_literals_valid = ["'_v1'", "'_2024'", "'_tag'", "'_out'", "'_ts'", "'_X'"]
string_literals_invalid = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]

numeric_literals = [str(i) for i in range(0, 10)]
arithmetic_ops = ['+', '-', '*', '/', '%']
numeric_func_names = ['abs', 'acos', 'cos', 'asin', 'sin', 'log', 'floor', 'ceil', 'round']
string_func_names = [
    'concat', 'upper', 'lower', 'substr',
    'replace', 'ltrim', 'rtrim', 'trim'
]

class TestStreamSynatx:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_create_stream_syntax(self):
        """Stream syntax

        1. Test create stream sql parser and syntax
        2. Test different valid and invalid combinations of create stream syntax options
        3. Verify the create stream results

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-5-13 Jing Sima Create Case

        """

        self.createSnode()
        self.createDatabase()
        self.prepareTables()
        self.createStream()

        tdSql.pause()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info(f"create database")

        tdSql.prepare(dbname="create_stream_db", vgroups=1)
        clusterComCheck.checkDbReady("create_stream_db")

    def prepareTables(self):
        # trigger_table(col1 timestamp, col2 int, col3 int, col4 int, col5 int, col6 int)
        # trigger_stable(col1 timestamp, col2 int, col3 int, col4 int, col5 int, col6 int) tags(tag1 int, tag2 int, tag3 int, tag4 int)
        # exist_super_table(out_col1 timestamp, out_col2 int, out_col3 int) tags(out_tag1 int, out_tag2 int)
        # exist_normal_table(out_col1 timestamp, out_col2 int, out_col3 int)
        # query_table(ts timestamp, col1 int, col2 int, col3 int, col4 int, col5 int, col6 int)
        tdSql.execute("create table create_stream_db.trigger_table (col1 timestamp, col2 int, col3 int, col4 int, col5 int, col6 int);")
        tdSql.execute("create table create_stream_db.trigger_stable (col1 timestamp, col2 int, col3 int, col4 int, col5 int, col6 int) tags(tag1 varchar(30), tag2 varchar(30), tag3 varchar(30), tag4 varchar(30));")
        tdSql.execute("create table create_stream_db.trigger_ctable using create_stream_db.trigger_stable tags('1', '2', '3', '4');")
        tdSql.execute("create table create_stream_db.exist_super_table (out_col1 timestamp, out_col2 bigint, out_col3 bigint) tags(out_tag1 varchar(30), out_tag2 varchar(30));")
        tdSql.execute("create table create_stream_db.exist_normal_table (out_col1 timestamp, out_col2 bigint, out_col3 bigint);")
        tdSql.execute("create table create_stream_db.exist_sub_table using create_stream_db.exist_super_table tags(1,2);")
        tdSql.execute("create table create_stream_db.query_table (ts timestamp, q_col1 bigint, q_col2 bigint, q_col3 bigint, q_col4 bigint, q_col5 bigint, q_col6 bigint);")

    def createStream(self):
        tdSql.execute("use create_stream_db")
        sql_list = gen_create_stream_variants()
        for sql, valid, index in sql_list:
            tdLog.info(f"create stream sql:{sql}, valid:{valid}")
            if valid:
                tdSql.execute(sql)
                tdSql.execute(f"drop stream if exists create_stream_db.stream_{index}")
                tdSql.execute(f"drop table if exists create_stream_db.new_table_{index}")
            else:
                tdSql.error(sql)

def random_from_list(lst, n=1):
    """Return n random elements from a list."""
    if n == 1:
        return random.choice(lst)
    return random.sample(lst, n)

def random_bool(prob=0.5):
    """Return True with the given probability."""
    return random.random() < prob

def random_int(a, b):
    """Return a random integer between a and b, inclusive."""
    return random.randint(a, b)

def generate_arithmetic_expr(column_list):
    left = random_from_list(column_list)
    right = random_from_list([str(random_int(1, 100))])
    operator = random_from_list(arith_ops)
    return f"({left} {operator} {right})"

def generate_atomic_condition(full_list=None, valid_list=None, valid=True):
    if valid:
        if valid_list is None or len(valid_list) == 0:
            column_list = ["1", "2", "3"]
        else:
            column_list = valid_list
    else:
        column_list = list(set(full_list) - set(valid_list))

    left_expr = (
        generate_arithmetic_expr(column_list) if random_bool(0.3) else random_from_list(column_list)
    )
    op = random_from_list(ops)
    right_expr = (
        generate_arithmetic_expr(column_list) if random_bool(0.3) else random_from_list(column_list + [str(random_int(1, 100))])
    )

    return f"{left_expr} {op} {right_expr}"

def generate_logical_condition(max_depth=2, current_depth=0, full_column_list=None, valid_column_list=None, valid=True):
    if current_depth >= max_depth or random_bool(0.4):
        return generate_atomic_condition(full_column_list, valid_column_list, valid)
    else:
        left = generate_logical_condition(max_depth, current_depth + 1, full_column_list, valid_column_list, valid)
        right = generate_logical_condition(max_depth, current_depth + 1, full_column_list, valid_column_list, valid)
        op = random_from_list(logic_ops)
        return f"({left} {op} {right})"

def random_from_combined(valid_list, invalid_list):
    if random.random() < 0.2 and invalid_list:
        return random.choice(invalid_list), False
    return random.choice(valid_list), True

unit_to_ns = {
    "b": 1,
    "u": 1_000,
    "a": 1_000_000,
    "s": 1_000_000_000,
    "m": 60 * 1_000_000_000,
    "h": 3600 * 1_000_000_000,
    "d": 86400 * 1_000_000_000,
    "w": 7 * 86400 * 1_000_000_000,
    "n": 31 * 86400 * 1_000_000_000,
    "y": 365 * 86400 * 1_000_000_000
}

def duration_to_nanoseconds(duration_str):
    match = re.fullmatch(r"(\d+)([a-zA-Z])", duration_str)
    if not match:
        return 0
    value = int(match.group(1))
    unit = match.group(2)
    if unit not in unit_to_ns:
        return 0
    return value * unit_to_ns[unit]

def generate_trigger_section(each_window_count=200):
    triggers = []
    # SESSION
    max_session_count = each_window_count
    for _ in range(0, max_session_count + 1):
        col, v1 = random_from_combined(session_column_valid, session_column_invalid)
        dur, v2 = random_from_combined(session_lists_valid, session_lists_invalid)
        triggers.extend([
            (f" SESSION({col}) ", False),
            (f" SESSION({col}, '{dur}') ", v1 and v2),
            (f" SESSION({col}, {dur}) ", v1 and v2),
        ])

    # STATE_WINDOW
    max_state_count = each_window_count
    for _ in range(0, max_state_count + 1):
        col, v1 = random_from_combined(state_column_valid, state_column_invalid)
        dur, v2 = random_from_combined(true_for_lists_valid, true_for_lists_invalid)
        triggers.extend([
            (f" STATE_WINDOW({col}) ", v1),
            (f" STATE_WINDOW({col}) TRUE_FOR('{dur}') ", v1 and v2),
        ])

    # INTERVAL + SLIDING
    max_sliding_count = each_window_count
    for _ in range(0, max_sliding_count + 1):
        interval, v1 = random_from_combined(duration_lists_valid, duration_lists_invalid)
        offset, v2 = random_from_combined(duration_lists_valid, duration_lists_invalid)
        slide, v3 = random_from_combined(slide_lists_valid, slide_lists_invalid)
        slide_offset, v4 = random_from_combined(slide_offset_lists_valid, slide_offset_lists_invalid)

        interval_val = duration_to_nanoseconds(interval) if interval else None
        offset_val = duration_to_nanoseconds(offset) if offset else None
        slide_val = duration_to_nanoseconds(slide) if slide else None
        slide_offset_val = duration_to_nanoseconds(slide_offset) if slide_offset else None

        if interval_val and offset_val:
            v5 = interval_val > offset_val
        else:
            v5 = True

        if slide_val and slide_offset_val:
            v6 = slide_val > slide_offset_val
        else:
            v6 = True

        if interval_val and slide_val:
            v7 = interval_val >= slide_val
        else:
            v7 = True

        if 'y' in interval and 'n' in slide:
            v8 = False
        else:
            v8 = True

        if interval_val and slide_val and not (('y' in interval or 'n' in interval) and 'n' not in slide and 'y' not in slide):
            v9 = interval_val / slide_val < 100
        else:
            v9 = True

        if 'n' in interval and 'y' in offset:
            v10 = False
        else:
            v10 = True

        is_all_valid = v1 and v2 and v3 and v4 and v5 and v6 and v7 and v8 and v9 and v10

        int_part = f" INTERVAL('{interval}') "
        int_part_with_offset = f" INTERVAL('{interval}', '{offset}') "
        slide_part = f" SLIDING('{slide}') "
        slide_part_with_offset = f" SLIDING('{slide}', '{slide_offset}') "

        triggers.extend([
            (slide_part, v3),
            (slide_part_with_offset, v3 and v4 and v6),
            (f" {int_part} {slide_part} ", v1 and v3 and v7 and v8 and v9),
            (f" {int_part} {slide_part_with_offset} ", v1 and v3 and v4 and v6 and v7 and v8 and v9),
            (f" {int_part_with_offset} {slide_part} ", v1 and v2 and v3 and v5 and v7 and v8 and v9 and v10),
            (f" {int_part_with_offset} {slide_part_with_offset} ", is_all_valid)
        ])

    # EVENT_WINDOW
    max_event_count = each_window_count
    for _ in range(0, max_event_count + 1):
        start_valid = random_bool(0.8)
        end_valid = random_bool(0.8)
        start = generate_logical_condition(full_column_list=event_count_column_valid + event_count_column_invalid, valid_column_list=event_count_column_valid, valid=start_valid)
        end = generate_logical_condition(full_column_list=event_count_column_valid + event_count_column_invalid, valid_column_list=event_count_column_valid, valid=end_valid)
        triggers.append((f" EVENT_WINDOW(START WITH {start} END WITH {end}) ", start_valid and end_valid))

    # COUNT_WINDOW
    max_count_count = each_window_count
    for _ in range(0, max_count_count + 1):
        count = random_from_list([5, 10, 20])
        slide = random_from_list([None, 10, 20])
        col_length = random_int(0, 5)
        if random_bool(0.3):
            valid = False
            count_columns = random.choices(event_count_column_invalid, k=col_length + 1)
        else:
            valid = True
            count_columns = random.choices(event_count_column_valid, k=col_length)

        if slide is not None and slide > count:
            valid1 = False
        else:
            valid1 = True

        parts = [str(count)]
        if slide is not None:
            parts.append(str(slide))
        for cols in count_columns:
            parts.append(cols)
        triggers.append((f" COUNT_WINDOW({', '.join(parts)}) ", valid and valid1))

    # PERIOD
    max_period_count = each_window_count
    for _ in range(0, max_period_count + 1):
        period, v1 = random_from_combined(period_time_list_valid, period_time_list_invalid)
        offset, v2 = random_from_combined(period_offset_list_valid, period_offset_list_invalid)

        period_val = duration_to_nanoseconds(period) if period else None
        offset_val = duration_to_nanoseconds(offset) if offset else None

        if period_val and offset_val:
            v3 = period_val > offset_val
        else:
            v3 = True

        triggers.extend([
            (f" PERIOD('{period}') ", v1),
            (f" PERIOD('{period}', '{offset}') ", v1 and v2 and v3)
        ])
    return triggers

def generate_random_event_types(valid=True):
    if valid:
        types = [random_from_list(event_types_valid) for _ in range(random_int(1, 2))]
    else:
        types = [random_from_list(event_types_invalid)] + [random_from_list(event_types_valid) for _ in range(random_int(1, 2))]
    return "|".join(types)

def random_option(valid=True, max_options=3, trigger_has_tag=False):
    if max_options == 0:
        return []

    if valid:
        watermark_duration = random_from_list(duration_lists_valid)
        expired_time = random_from_list(expired_time_list_valid)
        start_time = random_from_list(start_time_valid)
        max_delay = random_from_list(expired_time_list_valid)
    else:
        watermark_duration = random_from_list(duration_lists_invalid)
        expired_time = random_from_list(expired_time_list_invalid)
        start_time = random_from_list(start_time_invalid)
        max_delay = random_from_list(expired_time_list_invalid)

    prev_filter = generate_logical_condition(
        full_column_list = trigger_column_valid + trigger_column_invalid,
        valid_column_list = trigger_column_valid + trigger_tag_valid if trigger_has_tag else trigger_column_valid,
        valid = valid)

    if valid:
        option_type = random.sample([
            lambda: f"WATERMARK({watermark_duration})",
            lambda: f"EXPIRED_TIME({expired_time})",
            lambda: "IGNORE_DISORDER",
            lambda: "DELETE_RECALC",
            lambda: "DELETE_OUTPUT_TABLE",
            lambda: f"FILL_HISTORY({start_time})",
            lambda: f"FILL_HISTORY_FIRST({start_time})",
            lambda: "CALC_NOTIFY_ONLY",
            lambda: "LOW_LATENCY_CALC",
            lambda: f"PRE_FILTER({prev_filter})",
            lambda: "FORCE_OUTPUT",
            lambda: f"MAX_DELAY({max_delay})",
            lambda: f"EVENT_TYPE({generate_random_event_types(valid)})"
        ],
        max_options)
    else:
        option_type = random.sample([
            lambda: f"WATERMARK({watermark_duration})",
            lambda: f"EXPIRED_TIME({expired_time})",
            lambda: f"FILL_HISTORY({start_time})",
            lambda: f"FILL_HISTORY_FIRST({start_time})",
            lambda: f"PRE_FILTER({prev_filter})",
            lambda: f"MAX_DELAY({max_delay})",
            lambda: f"EVENT_TYPE({generate_random_event_types(valid)})"
        ],
        max_options)

    return [f() for f in option_type]

def generate_options_section(max_options=10, trigger_has_tag = False):
    stream_options = random_option(valid=True, trigger_has_tag=trigger_has_tag, max_options=max_options)
    rand_val = random.random()
    if rand_val < 0.2:
        # 20% chance to generate empty stream_options clause
        return "", True
    elif rand_val < 0.3:
        # 10% chance to generate invalid stream_options clause
        stream_options = stream_options + random_option(valid=False, trigger_has_tag=trigger_has_tag, max_options=1)
        valid = False
    else:
        # 70% chance to generate valid stream_options clause
        valid = True
    combined = '|'.join(stream_options)

    # FILL_HISTORY and FILL_HISTORY_FIRST cannot be used together
    if "FILL_HISTORY(" in combined and "FILL_HISTORY_FIRST(" in combined:
        valid = False

    return f" stream_options({combined}) ", valid

def pick_random_combo(source_list, max_len):
    if max_len == 0:
        return []
    length = random_int(1, max_len)
    return [random_from_list(source_list) for _ in range(length)] if length > 0 else []


def random_expr_atom(column_list=None, valid=True):
    string_literals = string_literals_valid if valid else string_literals_invalid
    return random.choices(
        population=column_list + string_literals,
        weights=[7] * len(column_list) + [3] * len(string_literals),
        k=1
    )[0]

def random_numeric_atom(column_list=None):
    return random.choices(
        population=column_list + numeric_literals,
        weights=[7] * len(column_list) + [3] * len(numeric_literals),
        k=1
    )[0]

def gen_string_func(func, expr=None, invalid_col_list=None, valid_col_list=None, valid=True):
    if valid:
        atom_expr = random_expr_atom(valid_col_list, valid)
        string_literals = string_literals_valid
    else:
        atom_expr = random_expr_atom(invalid_col_list, valid)
        string_literals = string_literals_invalid

    if func == 'concat':
        args = [random_expr_atom(valid_col_list if valid else invalid_col_list, valid) for _ in range(random_int(2, 4))]
        return f"concat({', '.join(args)})"
    elif func == 'upper':
        return f"upper({expr or atom_expr})"
    elif func == 'lower':
        return f"lower({expr or atom_expr})"
    elif func == 'substr':
        expr = expr or atom_expr
        start = str(random_int(0, 3))
        length = str(random_int(1, 5))
        return f"substr({expr}, {start}, {length})"
    elif func == 'replace':
        expr = expr or atom_expr
        search = random_from_list(string_literals)
        repl = random_from_list(string_literals)
        return f"replace({expr}, {search}, {repl})"
    elif func == 'ltrim':
        return f"ltrim({expr or atom_expr})"
    elif func == 'rtrim':
        return f"rtrim({expr or atom_expr})"
    elif func == 'trim':
        return f"trim({expr or atom_expr})"
    else:
        raise ValueError(f"Unknown string func: {func}")

def gen_numeric_expr(depth=0, max_depth=3, invalid_col_list=None, valid_col_list=None, valid=True):
    if depth >= max_depth or random_bool(0.3):
        return random_numeric_atom(valid_col_list if valid else invalid_col_list)
    if random_bool(0.4):
        # function
        func = random_from_list(numeric_func_names)
        return gen_numeric_func(func, gen_numeric_expr(depth + 1, max_depth, invalid_col_list, valid_col_list, valid))
    else:
        # operators
        left = gen_numeric_expr(depth + 1, max_depth, invalid_col_list, valid_col_list, valid)
        op = random_from_list(arithmetic_ops)
        right = gen_numeric_expr(depth + 1, max_depth, invalid_col_list, valid_col_list, valid)
        return f"({left} {op} {right})"

def gen_numeric_func(func, expr=None, invalid_col_list=None, valid_col_list=None, valid=True):
    expr = expr or gen_numeric_expr(2, 3, invalid_col_list, valid_col_list, valid)
    return f"{func}({expr})"

def gen_string_expr(depth=0, max_depth=3, invalid_col_list=None, valid_col_list=None, valid=True):
    if depth >= max_depth or random_bool(0.3):
        return random_expr_atom(valid_col_list if valid else invalid_col_list, valid)
    func = random_from_list(string_func_names)
    inner = gen_string_expr(depth + 1, max_depth, invalid_col_list, valid_col_list, valid)
    return gen_string_func(func, inner, invalid_col_list, valid_col_list, valid)

def generate_tag_expr(max_depth=3, invalid_col_list=None, valid_col_list=None, valid=True):
    if random_bool(1):
        # generate string type expression
        return gen_string_expr(0, max_depth, invalid_col_list, valid_col_list, valid)
    else:
        # generate numeric type expression
        return gen_numeric_expr(0, max_depth, invalid_col_list, valid_col_list, valid)


def generate_column_section_base(out_col_list, out_col_num=6, into_exist=False, valid=True):
    out_col_num = 6 if out_col_num == 0 else out_col_num
    if valid:
        pk_index = 1
        if into_exist:
            selected = out_col_list[:out_col_num]
            with_primary = False
        else:
            selected = random.sample(out_col_list, out_col_num)
            with_primary = random_bool(0.5)
    else:
        pk_index = random_from_list([i for i in range(0, out_col_num) if i != 1])
        if into_exist:
            col_num = random_int(1, out_col_num)
            selected = random.choices(out_col_list, k=col_num)
            with_primary = random_bool(0.5)
        else:
            col_num = random_from_list([i for i in range(1, out_col_num + 3) if i != out_col_num])
            selected = random.choices(out_col_list, k=col_num)
            with_primary = random_bool(0.5)

    col_defs = []
    for i, col in enumerate(selected):
        if i == pk_index and with_primary:
            col_defs.append(f"{col} PRIMARY KEY")
        else:
            col_defs.append(col)
    return f" ({', '.join(col_defs)}) "

def random_string(length=5):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_stream_db_section():
    if random_bool(0.9):
        # Generate a valid database name
        dbname = random_from_list(db_name_list_valid)
        return f" {dbname}", True
    else:
        # Generate an invalid database name
        dbname = random_from_list(db_name_list_invalid)
        return f" {dbname}", False

# return a tuple (trigger_table, is_valid, trigger_null, has_tag)
def generate_random_trigger_table_section():
    if random_bool(0.2):
        # Do not generate a trigger table
        return "", True, True, False
    else :
        if random_bool(0.9):
            # Generate a valid database name
            dbname = random_from_list(db_name_list_valid)
            if random_bool():
                # Generate a valid trigger table name
                trigger_table = random_from_list(trigger_table_list_valid)
                if trigger_table == "trigger_table":
                    return f" FROM {dbname}{trigger_table} ", True, False, False
                else :
                    return f" FROM {dbname}{trigger_table} ", True, False, True
            else:
                # Generate an invalid trigger table name
                trigger_table = random_from_list(trigger_table_list_invalid)
                return f" FROM {dbname}{trigger_table} ", False, False, False
        else:
            # Generate an invalid database name
            dbname = random_from_list(db_name_list_invalid)
            if random_bool():
                # Generate a valid trigger table name
                trigger_table = random_from_list(trigger_table_list_valid)
                return f" FROM {dbname}{trigger_table} ", False, False, False
            else:
                # Generate an invalid trigger table name
                trigger_table = random_from_list(trigger_table_list_invalid)
                return f" FROM {dbname}{trigger_table} ", False, False, False

def generate_random_into_table_section(stream_index=0):
    if random_bool(0.9):
        # Generate a valid into table section
        into_table = random_from_list(into_option_list_valid)
        if into_table == "":
            return "", True, True, False, False
        elif "new_table" in into_table:
            into_table += f"_{stream_index}"
            return f" {into_table} ", True, False, False, False
        else:
            return f" {into_table} ", True, False, "exist" in into_table, "super_table" in into_table
    else:
        # Generate an invalid into table section
        into_table = random_from_list(into_option_list_invalid)
        return f" {into_table} ", False, False, False, False

def generate_random_partition_section(max_partition_len = 3, trigger_null = False, trigger_has_tag = False):
    rand_val = random.random()
    if trigger_null:
        if random_bool(0.1):
            # 20% chance to generate invalid partition clause
            selected = pick_random_combo(partition_columns_valid, max_partition_len)
            return f" PARTITION BY {', '.join(selected)} ", False, selected
        else:
            return "", True, None

    if trigger_has_tag:
        valid_columns = partition_columns_valid
        invalid_columns = partition_columns_invalid
    else:
        valid_columns = ["tbname"]
        invalid_columns = list(set(partition_columns_valid + partition_columns_invalid) - set(valid_columns))

    # 20% chance to generate empty partition clause
    if rand_val < 0.2:
        return "", True, None
    # 30% chance to generate invalid partition clause
    elif rand_val < 0.4:
        selected = random.sample(valid_columns, min(max_partition_len , len(valid_columns))) + random.sample(invalid_columns, 1)
        return f" PARTITION BY {', '.join(selected)} ", False, selected
    # 50% chance to generate valid partition clause
    else:
        selected = random.sample(valid_columns, min(max_partition_len, len(valid_columns)))
        return f" PARTITION BY {', '.join(selected)} ", True, selected

def generate_random_notif_def_section(
        max_urls=2, max_events=2, max_options=2, max_condition_depth=2, query_cols = None
):
    # Each part has 20% chance to be invalid, and 80% chance to be valid. And total's invalid change is also 20%.
    valid = True

    # optional NOTIFY(url [, ...])
    if random_bool(0.1):
        # 20% chance to generate invalid NOTIFY clause
        if random_bool(0.5):
            notify_urls = ""
        else:
            # pick at least one URL, include invalid URLs
            notify_url = pick_random_combo(urls_valid, max_urls - 1) + pick_random_combo(urls_invalid, 1)
            notify_urls = f" NOTIFY({', '.join(notify_url)}) "
            valid = False
    else:
        # 80% chance to generate valid NOTIFY clause
        # pick at least one URL
        notify_url = pick_random_combo(urls_valid, max_urls)
        notify_urls = f" NOTIFY({', '.join(notify_url)}) "


    # optional ON (event_types)
    if valid == True and random_bool(0.1):
        # 20% chance to generate invalid NOTIFY_OPTIONS clause
        notify_event = pick_random_combo(event_types_valid, max_events - 1) + pick_random_combo(event_types_invalid, 1)
        notify_events = f" ON ({'|'.join(notify_event)}) "
        valid = False
    else:
        # 80% chance to generate valid NOTIFY_OPTIONS clause
        if random_bool(0.2):
            # 20% chance to generate empty ON clause
            notify_events = ""
        else:
            selected_events = pick_random_combo(event_types_valid, max_events)
            notify_events = f" ON ({'|'.join(selected_events)}) "

    # optional WHERE condition (using generate_logical_condition)
    if valid == True and random_bool(0.1):
        # 20% chance to generate invalid WHERE clause
        if query_cols is None:
            query_cols = ["col1", "col2", "col3"]
        condition = generate_logical_condition(max_depth=max_condition_depth, full_column_list=partition_columns_valid, valid_column_list=query_cols, valid=False)
        notify_conditions = f" WHERE {condition} "
        valid = False
    else:
        # 80% chance to generate valid WHERE clause
        if random_bool(0.2) or query_cols is None:
            # 20% chance to generate empty WHERE clause
            notify_conditions = ""
        else:
            condition = generate_logical_condition(max_depth=max_condition_depth, full_column_list=partition_columns_valid, valid_column_list=query_cols, valid=True)
            notify_conditions = f" WHERE {condition} "

    # optional NOTIFY_OPTIONS(...)
    if valid == True and random_bool(0.1):
        # 20% chance to generate invalid NOTIFY_OPTIONS clause
        notify_option = pick_random_combo(notify_option_valid, max_options - 1) + pick_random_combo(notify_option_invalid, 1)
        notify_options = f" NOTIFY_OPTIONS({'|'.join(notify_option)}) "
        valid = False
    else:
        # 80% chance to generate valid NOTIFY_OPTIONS clause
        if random_bool(0.2):
            # 20% chance to generate empty NOTIFY_OPTIONS clause
            notify_options = ""
        else:
            # pick at least one option
            notify_option = pick_random_combo(notify_option_list, max_options)
            notify_options = f" NOTIFY_OPTIONS({'|'.join(notify_option)}) "

    if notify_urls == "" and (notify_events != "" or notify_conditions != "" or notify_options != ""):
        # If URLs are empty, but other parts are not, it's invalid
        valid = False

    return notify_urls + notify_events + notify_conditions + notify_options, valid

def generate_random_column_list_section(out_col_num=1, into_exist=False, into_null=False):
    random_value = random.random()
    if into_null:
        if random_bool(0.8):
            # 80% chance to generate empty column list
            return "", True
        else:
            # 20% chance to generate invalid column list
            return generate_column_section_base(out_columns, out_col_num=out_col_num, into_exist=into_exist, valid=True), False

    if random_value < 0.2:
        # 20% chance to generate empty column list
        return "", True
    elif random_value < 0.3:
        # 20% chance to generate invalid column list
        return generate_column_section_base(out_columns, out_col_num, into_exist=into_exist, valid=False), False
    else:
        return generate_column_section_base(out_columns, out_col_num, into_exist=into_exist, valid=True), True

def generate_random_output_subtable(max_depth=3, partition_list=None, into_null=False):
    valid = True
    if into_null:
        if random_bool(0.8):
            return "", True
        else:
            valid = False

    if partition_list is None:
        if random_bool(0.8):
            return "", True
        else:
            valid = False

    valid_list = partition_list if partition_list else partition_columns_valid
    invalid_list = list(set(partition_columns_valid + partition_columns_invalid) - set(valid_list))

    if random_bool(0.1):
        # 20% chance to generate invalid OUTPUT_SUBTABLE
        expr = gen_string_expr(0, max_depth, invalid_list, valid_list, False)
        valid = False
    else:
        # 80% chance to generate valid OUTPUT_SUBTABLE
        expr = gen_string_expr(0, max_depth, invalid_list, valid_list, True)

    return f" OUTPUT_SUBTABLE({expr}) ", valid

def generate_random_tags_clause(partition_list=None, allow_comment=True, into_exist=False, into_stable=False, into_null=False):

    valid = True
    if partition_list == None or (into_exist and not into_stable) or into_null:
        if random_bool(0.8):
            return "", True
        else:
            if partition_list is None:
                partition_list = partition_columns_invalid
            valid = False

    if random_bool(0.1):
        return "", True

    if random_bool(0.1) and valid and into_exist and into_stable:
        num_tags = random_from_list([i for i in range(1, len(out_tags)) if i != 2])
        valid = False
    else:
        # 80% chance to generate valid TAGS clause
        num_tags = 2

    if random_bool(0.1) and valid and into_exist:
        selected_tags = random.sample(out_tags, num_tags - 1)
        valid = False
    else:
        selected_tags = out_tags[:num_tags]

    if random_bool(0.1) and valid:
        # 20% chance to generate invalid tag type
        out_type_list = out_tag_type_invalid
        valid = False
    else:
        out_type_list = out_tag_type_valid

    if random_bool(0.1) and valid:
        # 20% chance to generate invalid tag type
        gen_valid_expr = False
        valid = False
    else:
        gen_valid_expr = True

    tag_defs = []
    for tag in selected_tags:
        type_name = random_from_list(out_type_list)
        comment_str = f" COMMENT '{random_string(6)}'" if allow_comment and random_bool(0.5) else ""
        valid_list = partition_list
        invalid_list = list(set(partition_columns_valid + partition_columns_invalid) - set(valid_list))
        expr = generate_tag_expr(max_depth=2, invalid_col_list=invalid_list, valid_col_list=valid_list, valid=gen_valid_expr)
        tag_defs.append(f"{tag} {type_name}{comment_str} AS {expr}")
    return f" TAGS ({', '.join(tag_defs)}) ", valid


def gen_create_stream_variants():
    base_template = "CREATE STREAM{if_not_exists} {stream_name}{stream_options}{into_clause}{output_subtable}{columns}{tags}{as_subquery};"
    sql_variants = []
    stream_index = 0
    for trigger_type, v0 in generate_trigger_section(500):
        for as_subquery, v1, out_col_num, query_col_list, with_ph_column in as_subquery_opts:
            stream_db, v2 = generate_random_stream_db_section()
            trigger_table, v3, trigger_null, trigger_has_tag = generate_random_trigger_table_section()
            into_table, v4, into_null, into_exist, into_stable = generate_random_into_table_section(stream_index)
            partition, v5, partition_cols = generate_random_partition_section(trigger_null = trigger_null, trigger_has_tag = trigger_has_tag)
            stream_opt, v6 = generate_options_section(trigger_has_tag=trigger_has_tag)
            notify_opt, v7 = generate_random_notif_def_section(query_cols=query_col_list)
            output, v8 = generate_random_output_subtable(partition_list=partition_cols, into_null=into_null)
            column, v9 = generate_random_column_list_section(out_col_num=out_col_num, into_exist=into_exist, into_null=into_null)
            tag, v10 = generate_random_tags_clause(partition_list=partition_cols, into_exist=into_exist, into_stable=into_stable, into_null=into_null)

            if trigger_null and "PERIOD" not in trigger_type:
                v11 = False
            else:
                v11 = True

            if out_col_num != 3 and into_exist:
                v12 = False
            else:
                v12 = True

            if notify_opt == "" and out_col_num == 0:
                v13 = False
            else:
                v13 = True

            if partition == "" and into_exist and into_stable:
                v14 = False
            elif partition == "" and into_exist and not into_stable:
                v14 = True
            elif partition != "" and into_exist and into_stable:
                v14 = True
            elif partition != "" and into_exist and not into_stable:
                v14 = False
            else:
                v14 = True

            if partition == "" and with_ph_column:
                v15 = False
            else:
                v15 = True

            if "%%tbname" in as_subquery:
                if partition == "":
                    v16 = False
                elif "tbname" not in partition:
                    v16 = False
                else:
                    v16 = True
            else:
                v16 = True

            if "_twstart" in as_subquery or "_twend" in as_subquery or "_twduration" in as_subquery or "_twrownum" in as_subquery:
                if "PERIOD" in trigger_type or ("INTERVAL" not in trigger_type and "SLIDING" in trigger_type):
                    v17 = False
                else:
                    v17 = True
            else:
                v17 = True

            if "_tprev_ts" in as_subquery or "_tcurrent_ts" in as_subquery or "_tnext_ts" in as_subquery:
                if "SLIDING" not in trigger_type:
                    v18 = False
                else:
                    v18 = True
            else:
                v18 = True

            if "_tprev_localtime" in as_subquery or "_tnext_localtime" in as_subquery:
                if "PERIOD" not in trigger_type:
                    v19 = False
                else:
                    v19 = True
            else:
                v19 = True

            if into_null:
                if as_subquery == "" and notify_opt != "":
                    v20 = True
                elif as_subquery != "" and "CALC_NOTIFY_ONLY" in stream_opt:
                    v20 = True
                else:
                    v20 = False
            else:
                v20 = True

            if tag == "" and into_exist and into_stable and partition != "":
                if len(partition_cols) != 2:
                    v21 = False
                else:
                    v21 = True
            else:
                v21 = True

            if not into_null and as_subquery == "":
                v22 = False
            else:
                v22 = True

            if column == "" and into_exist and as_subquery != "":
                v23 = False
            else:
                v23 = True

            if ('EVENT' in trigger_type or 'COUNT' in trigger_type) and 'tag' in trigger_type:
                if 'trigger_stable' in trigger_table or 'trigger_ctable' in trigger_table:
                    v24 = True
                else:
                    v24 = False
            else:
                v24 = True

            if trigger_null and ('%%trows' in as_subquery or '%%tbname' in as_subquery):
                v25 = False
            else:
                v25 = True

            if trigger_null and 'PRE_FILTER' in stream_opt:
                v26 = False
            else:
                v26 = True

            valid = (v0 and v1 and v2 and v3 and v4 and v5 and v6 and v7 and v8 and v9 and
                     v10 and v11 and v12 and v13 and v14 and v15 and v16 and v17 and v18 and v19 and
                     v20 and v21 and v22 and v23 and v24 and v25 and v26)
            print(f"stream_index: {stream_index}, v0: {v0}, v1: {v1}, v2: {v2}, v3: {v3}, v4: {v4}, "
                  f"v5: {v5}, v6: {v6}, v7: {v7}, v8: {v8}, v9: {v9}, v10: {v10}, v11: {v11}, "
                  f"v12: {v12}, v13: {v13}, v14: {v14}, v15: {v15}, v16: {v16}, v17: {v17}, "
                  f"v18: {v18}, v19: {v19}, v20: {v20}, v21: {v21}, v22: {v22}, v23: {v23}, "
                  f"v24: {v24}, v25: {v25}, v26: {v26}, valid: {valid}")

            sql = base_template.format(
               if_not_exists=random_from_list(if_not_exists_opts),
               stream_name=stream_db + "stream_" + str(stream_index),
               stream_options=trigger_type + trigger_table + partition + stream_opt + notify_opt,
               into_clause=into_table,
               output_subtable=output,
               columns=column,
               tags= tag,
               as_subquery=as_subquery
            )
            sql_variants.append((sql.strip(), valid, stream_index))
            stream_index += 1
    return sql_variants


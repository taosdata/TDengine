import random
from itertools import product
import string

# Common time units
duration_lists = [
    "",
    "1b", "1u", "1a", "1s", "1m", "1h", "1d", "1w", "1n", "1y",
    "2b", "2u", "2a", "2s", "2m", "2h", "2d", "2w", "2n", "2y",
    "5b", "5u", "5a", "5s", "5m", "5h", "5d", "5w", "5n", "5y",
    "7b", "7u", "7a", "7s", "7m", "7h", "7d", "7w", "7n", "7y",
    "12b", "12u", "12a", "12s", "12m", "12h", "12d", "12w", "12n", "12y",
    "30b", "30u", "30a", "30s", "30m", "30h", "30d", "30w", "30n", "30y",
    "365b", "365u", "365a", "365s", "365m", "365h", "365d", "365w", "365n", "365y"
]

columns = ["ts_col", "col1", "col2", "tag1", "tag2", "tag3"]
partition_columns = ["ts_col", "tag1", "tag2", "tag3", "tag4", "tbname"]
placeholders = ["_tcurrent_ts", "_twstart", "_twend", "_twduration", "_twrownum", "_tgrpid", "_tlocaltime", "%%1", "%%2", "%%3", "%%tbname", "%%trows"]
out_columns = ["ts_col", "col1", "col2", "col3", "col4", "col5", "col6"]
out_tags = ["tag1", "tag2", "tag3", "tag4", "tag5"]
counts = [10, 100]
slidings = [1, 5]
event_types = ["WINDOW_OPEN", "WINDOW_CLOSE"]
ops = ["=", "<>", "!=", ">", "<", ">=", "<="]
arith_ops = ["+", "-", "*", "/"]
logic_ops = ["AND", "OR"]
timestamps = ["2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z"]
event_types_pool = ["WINDOW_OPEN", "WINDOW_CLOSE"]
urls = ["http://example.com/notify", "http://localhost:8000/callback", "https://api.test.com/hook"]
notify_option_list = ["NOTIFY_HISTORY", "ON_FAILURE_PAUSE"]
into_option_list = [
    "",
    " INTO create_stream_db.new_table",
    " INTO create_stream_db.exist_super_table",
    " INTO create_stream_db.exist_sub_table",
    " INTO create_stream_db.exist_normal_table",
    " INTO non_exists_db.new_table",
    " INTO create_stream_db.exist_super_table",
    " INTO new_table",
    " INTO exist_super_table",
    " INTO exist_sub_table",
    " INTO exist_normal_table"
]
if_not_exists_opts = ["", " IF NOT EXISTS"]
db_name_list = ["", "create_stream_db.", "non_exists_db."]
trigger_table_list = ["trigger_table", "trigger_stable", "trigger_ctable", "non_exists_table", ""]

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

def generate_arithmetic_expr():
    left = random_from_list(columns)
    right = random_from_list(columns + [str(random_int(1, 100))])
    operator = random_from_list(arith_ops)
    return f"({left} {operator} {right})"

def generate_atomic_condition():
    left_expr = (
        generate_arithmetic_expr() if random_bool(0.3) else random_from_list(columns)
    )
    op = random_from_list(ops)
    right_expr = (
        generate_arithmetic_expr()
        if random_bool(0.3)
        else random_from_list(columns + [str(random_int(1, 100))])
    )
    return f"{left_expr} {op} {right_expr}"

def generate_logical_condition(max_depth=2, current_depth=0):
    if current_depth >= max_depth or random_bool(0.4):
        return generate_atomic_condition()
    else:
        left = generate_logical_condition(max_depth, current_depth + 1)
        right = generate_logical_condition(max_depth, current_depth + 1)
        op = random_from_list(logic_ops)
        return f"({left} {op} {right})"

def generate_event_window_conditions(num_pairs=10):
    return [
        f"EVENT_WINDOW(START WITH {generate_logical_condition()} END WITH {generate_logical_condition()})"
        for _ in range(num_pairs)
    ]

def generate_trigger_section():
    triggers = []
    # SESSION
    for col in columns:
        dur = random_from_list(duration_lists)
        triggers.append(f" SESSION({col}, '{dur}') ")
        triggers.append(f" SESSION({col}, {dur}) ")
    # STATE_WINDOW
    for col in columns:
        triggers.append(f" STATE_WINDOW({col}) ")
        dur = random_from_list(duration_lists)
        triggers.append(f" STATE_WINDOW({col}) TRUE_FOR('{dur}') ")
    # INTERVAL + SLIDING
    max_sliding_count = 20
    for _ in range(0, max_sliding_count + 1):
        interval = random_from_list(duration_lists)
        offset = random_from_list(duration_lists)
        slide = random_from_list(duration_lists)
        slide_offset = random_from_list(duration_lists)
        int_part = f" INTERVAL('{interval}') "
        int_part_with_offset = f" INTERVAL('{interval}', '{offset}') "
        slide_part = f" SLIDING('{slide}') "
        slide_part_with_offset = f" SLIDING('{slide}', '{slide_offset}') "
        triggers.extend([
            slide_part,
            slide_part_with_offset,
            f" {int_part} {slide_part} ",
            f" {int_part} {slide_part_with_offset} ",
            f" {int_part_with_offset} {slide_part} ",
            f" {int_part_with_offset} {slide_part_with_offset} "
        ])
    # EVENT_WINDOW
    max_event_count = 20
    for _ in range(0, max_event_count + 1):
        start = generate_logical_condition()
        end = generate_logical_condition()
        ew = f" EVENT_WINDOW(START WITH {start} END WITH {end}) "
        triggers.append(ew)
    # COUNT_WINDOW
    max_col_len = 3
    max_samples_per_len = 10
    for count in [1, 10, 20]:
        for slide in [None, 10, 20]:
            for length in range(0, max_col_len + 1):
                all_combinations = list(product(columns, repeat=length))
                sampled_combinations = (
                    random.sample(all_combinations, min(len(all_combinations), max_samples_per_len))
                    if all_combinations else [()]
                )
                for cols in sampled_combinations:
                    parts = [str(count)]
                    if slide is not None:
                        parts.append(str(slide))
                    if cols:
                        parts.extend(cols)
                    triggers.append(f" COUNT_WINDOW({', '.join(parts)}) ")
    # PERIOD
    max_period_count = 20
    for _ in range(0, max_period_count + 1):
        period = random_from_list(duration_lists)
        offset = random_from_list(duration_lists)
        triggers.append(f" PERIOD('{period}', '{offset}') ")
        triggers.append(f" PERIOD('{period}') ")
    return triggers

def generate_partition_section():
    max_partition_len = 3  # Maximum number of columns (including duplicates)
    length = random_int(1, max_partition_len)
    combo = tuple(random.choices(partition_columns, k=length))  # allow duplicates
    if random_bool(0.2):  # 20% generate empty clause
        return ""
    return f" PARTITION BY {', '.join(combo)} "

def generate_event_types():
    types = [random_from_list(event_types_pool) for _ in range(random_int(1, 3))]
    return "|".join(types)

def random_option():
    option_type = random_from_list([
        lambda: f"WATERMARK({random_from_list(duration_lists)})",
        lambda: f"EXPIRED_TIME({random_from_list(duration_lists)})",
        lambda: "IGNORE_DISORDER",
        lambda: "DELETE_RECALC",
        lambda: "DELETE_OUTPUT_TABLE",
        lambda: f"FILL_HISTORY({random_from_list(timestamps)})",
        lambda: f"FILL_HISTORY_FIRST({random_from_list(timestamps)})",
        lambda: "CALC_NOTIFY_ONLY",
        lambda: "LOW_LATENCY_CALC",
        lambda: f"PRE_FILTER({generate_logical_condition()})",
        lambda: "FORCE_OUTPUT",
        lambda: f"MAX_DELAY({random_from_list(duration_lists)})",
        lambda: f"EVENT_TYPE({generate_event_types()})"
    ])
    return option_type()

def generate_options_section(n=10, max_options=10):
    options_clauses = [""]
    for _ in range(n):
        count = random_int(1, max_options)
        options = [random_option() for _ in range(count)]
        clause = f" OPTIONS({'|'.join(options)}) "
        options_clauses.append(clause)
    return options_clauses

def pick_random_combo(source_list, max_len):
    length = random_int(0, max_len)
    return [random_from_list(source_list) for _ in range(length)] if length > 0 else []

def generate_notif_def_section(
        total, max_urls=2, max_events=2, max_options=2, max_condition_depth=2
):
    result = []
    for _ in range(total):
        parts = []
        # optional NOTIFY(url [, ...])
        notify_urls = pick_random_combo(urls, max_urls)
        if notify_urls:
            parts.append(f" NOTIFY({', '.join(notify_urls)}) ")
        # optional ON (event_types)
        selected_events = pick_random_combo(event_types, max_events)
        if selected_events:
            parts.append(f" ON ({'|'.join(selected_events)}) ")
        # optional WHERE condition (using generate_logical_condition)
        if random_bool():
            condition = generate_logical_condition(max_depth=max_condition_depth)
            parts.append(f" WHERE {condition} ")
        # optional NOTIFY_OPTIONS(...)
        selected_options = pick_random_combo(notify_option_list, max_options)
        if selected_options:
            parts.append(f" NOTIFY_OPTIONS({'|'.join(selected_options)}) ")
        result.append(" ".join(parts))
    return result

string_literals = ["'_v1'", "'_2024'", "'_tag'", "'_out'", "'_ts'", "'_X'"]
numeric_literals = [str(i) for i in range(0, 10)]
arithmetic_ops = ['+', '-', '*', '/', '%']
numeric_func_names = ['abs', 'acos', 'cos', 'asin', 'sin', 'log', 'floor', 'ceil', 'round']
string_func_names = [
    'concat', 'upper', 'lower', 'length', 'substr',
    'replace', 'ltrim', 'rtrim', 'trim'
]

def random_expr_atom():
    return random.choices(
        population=columns + string_literals,
        weights=[7] * len(columns) + [3] * len(string_literals),
        k=1
    )[0]

def random_numeric_atom():
    return random.choices(
        population=columns + numeric_literals,
        weights=[7] * len(columns) + [3] * len(numeric_literals),
        k=1
    )[0]

def gen_string_func(func, expr=None):
    if func == 'concat':
        args = [random_expr_atom() for _ in range(random_int(2, 4))]
        return f"concat({', '.join(args)})"
    elif func == 'upper':
        return f"upper({expr or random_expr_atom()})"
    elif func == 'lower':
        return f"lower({expr or random_expr_atom()})"
    elif func == 'length':
        return f"length({expr or random_expr_atom()})"
    elif func == 'substr':
        expr = expr or random_expr_atom()
        start = str(random_int(0, 3))
        length = str(random_int(1, 5))
        return f"substr({expr}, {start}, {length})"
    elif func == 'replace':
        expr = expr or random_expr_atom()
        search = random_from_list(string_literals)
        repl = random_from_list(string_literals)
        return f"replace({expr}, {search}, {repl})"
    elif func == 'ltrim':
        return f"ltrim({expr or random_expr_atom()})"
    elif func == 'rtrim':
        return f"rtrim({expr or random_expr_atom()})"
    elif func == 'trim':
        return f"trim({expr or random_expr_atom()})"
    else:
        raise ValueError(f"Unknown string func: {func}")

def gen_numeric_expr(depth=0, max_depth=3):
    if depth >= max_depth or random_bool(0.3):
        return random_numeric_atom()
    if random_bool(0.4):
        # function
        func = random_from_list(numeric_func_names)
        return gen_numeric_func(func, gen_numeric_expr(depth + 1, max_depth))
    else:
        # operators
        left = gen_numeric_expr(depth + 1, max_depth)
        op = random_from_list(arithmetic_ops)
        right = gen_numeric_expr(depth + 1, max_depth)
        return f"({left} {op} {right})"

def gen_numeric_func(func, expr=None):
    expr = expr or gen_numeric_expr(depth=2)
    return f"{func}({expr})"

def gen_string_expr(depth, max_depth):
    if depth >= max_depth or random_bool(0.3):
        return random_expr_atom()
    func = random_from_list(string_func_names)
    inner = gen_string_expr(depth + 1, max_depth)
    return gen_string_func(func, inner)

def generate_tag_expr(max_depth=3):
    if random_bool(0.5):
        # generate string type expression
        return gen_string_expr(0, max_depth)
    else:
        # generate numeric type expression
        return gen_numeric_expr(depth=0, max_depth=max_depth)

def generate_output_subtable(max_depth=3, include_probability=0.7):
    if not random_bool(include_probability):
        return ""  # Do not include OUTPUT_SUBTABLE
    expr = gen_string_expr(0, max_depth)
    return f" OUTPUT_SUBTABLE({expr}) "

def generate_column_section_base(out_col_list, include_probability=0.8, max_cols=6, with_primary_key_prob=0.6):
    if not random_bool(include_probability):
        return ""
    num_cols = random_int(1, min(max_cols, len(out_col_list)))
    selected = random.sample(out_col_list, num_cols)
    with_primary = random_bool(with_primary_key_prob)
    pk_index = random_int(0, num_cols - 1) if with_primary else None
    col_defs = []
    for i, col in enumerate(selected):
        if i == pk_index:
            col_defs.append(f"{col} PRIMARY KEY")
        else:
            col_defs.append(col)
    return f" ({', '.join(col_defs)}) "

def generate_column_list_section(include_probability=0.8, max_cols=6, with_primary_key_prob=0.6):
    return generate_column_section_base(out_columns, include_probability, max_cols, with_primary_key_prob)

out_types = ["BIGINT", "SMALLINT"]

def random_string(length=5):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_tags_clause(include_probability=0.7, max_tags=4, allow_comment=True):
    if not random_bool(include_probability):
        return ""
    num_tags = random_int(1, min(max_tags, len(out_tags)))
    selected_tags = random.sample(out_tags, num_tags)
    tag_defs = []
    for tag in selected_tags:
        type_name = random_from_list(out_types)
        comment_str = f" COMMENT '{random_string(6)}'" if allow_comment and random_bool(0.5) else ""
        expr = generate_tag_expr(max_depth=2)
        tag_defs.append(f"{tag} {type_name}{comment_str} AS {expr}")
    return f" TAGS ({', '.join(tag_defs)}) "

def generate_trigger_from_table_section():
    trigger_from_table = []
    for db_name in db_name_list:
        for table_name in trigger_table_list:
            trigger_from_table.append(f" FROM {db_name}{table_name} ")
    return trigger_from_table

def gen_create_stream_variants():
    base_template = "CREATE STREAM{if_not_exists} {stream_name}{stream_options}{into_clause}{output_subtable}{columns}{tags}{as_subquery};"
    as_subquery_opts = [" AS SELECT * FROM query_table", ""]
    trigger_types = generate_trigger_section()
    trigger_tables = generate_trigger_from_table_section()
    stream_options = generate_options_section(10, max_options=10)
    notify_options = generate_notif_def_section(total=10)
    sql_variants = []
    stream_index = 0
    for if_not_exists, db_name, into, as_subquery in product(
            if_not_exists_opts, db_name_list, into_option_list, as_subquery_opts
    ):
        for trigger_type in trigger_types:
            for trigger_table in trigger_tables:
                for stream_opt in stream_options:
                    for notify in notify_options:
                        sql = base_template.format(
                           if_not_exists=if_not_exists,
                           stream_name=db_name + "stream_" + str(stream_index),
                           stream_options=trigger_type + generate_partition_section() + stream_opt + notify + trigger_table,
                           into_clause=into,
                           output_subtable=generate_output_subtable(),
                           columns=generate_column_list_section(),
                           tags=generate_tags_clause() + " ",
                           as_subquery=as_subquery
                        )
                        sql_variants.append(sql.strip())
                        stream_index += 1
                        if stream_index > 100000:
                            return sql_variants
    print(stream_index)
    return sql_variants


sql = gen_create_stream_variants()
for i in range(10000):
    print("======================")
    print(sql[i])



import random
from itertools import product
import string

# 常见时间单位
duration_lists = ["",
                  "1b", "1u", "1a", "1s", "1m", "1h", "1d", "1w", "1n", "1y",
                  "2b", "2u", "2a", "2s", "2m", "2h", "2d", "2w", "2n", "2y",
                  "5b", "5u", "5a", "5s", "5m", "5h", "5d", "5w", "5n", "5y",
                  "7b", "7u", "7a", "7s", "7m", "7h", "7d", "7w", "7n", "7y",
                  "12b", "12u", "12a", "12s", "12m", "12h", "12d", "12w", "12n", "12y",
                  "30b", "30u", "30a", "30s", "30m", "30h", "30d", "30w", "30n", "30y",
                  "365b", "365u", "365a", "365s", "365m", "365h", "365d", "365w", "365n", "365y"]

columns = ["ts_col", "col1", "col2", "tag1", "tag2", "tag3"]
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
into_option_list = [ "",
                     " INTO create_stream_db.new_table",
                     " INTO create_stream_db.exist_super_table",
                     " INTO create_stream_db.exist_sub_table",
                     " INTO create_stream_db.exist_normal_table",
                     " INTO non_exists_db.new_table",
                     " INTO create_stream_db.exist_super_table",
                     " INTO new_table",
                     " INTO exist_super_table",
                     " INTO exist_sub_table",
                     " INTO exist_normal_table"]
def generate_arithmetic_expr():
    left = random.choice(columns)
    right = random.choice(columns + [str(random.randint(1, 100))])
    operator = random.choice(arith_ops)
    return f"({left} {operator} {right})"

def generate_atomic_condition():
    left_expr = (
        generate_arithmetic_expr() if random.random() < 0.3 else random.choice(columns)
    )
    op = random.choice(ops)
    right_expr = (
        generate_arithmetic_expr()
        if random.random() < 0.3
        else random.choice(columns + [str(random.randint(1, 100))])
    )
    return f"{left_expr} {op} {right_expr}"

def generate_logical_condition(max_depth=2, current_depth=0):
    if current_depth >= max_depth or random.random() < 0.4:
        return generate_atomic_condition()
    else:
        left = generate_logical_condition(max_depth, current_depth + 1)
        right = generate_logical_condition(max_depth, current_depth + 1)
        op = random.choice(logic_ops)
        return f"({left} {op} {right})"

def generate_event_window_conditions(num_pairs=10):
    condition_pairs = []
    for _ in range(num_pairs):
        start = generate_logical_condition()
        end = generate_logical_condition()
        ew = f"EVENT_WINDOW(START WITH {start} END WITH {end})"
        condition_pairs.append(ew)
    return condition_pairs

def generate_trigger_variants():
    triggers = []

    # SESSION
    for col in columns:
        dur = random.choice(duration_lists)
        triggers.append(f"SESSION({col}, '{dur}')")
        triggers.append(f"SESSION({col}, {dur})")

    # STATE_WINDOW
    for col in columns:
        triggers.append(f"STATE_WINDOW({col})")
        dur = random.choice(duration_lists)
        triggers.append(f"STATE_WINDOW({col}) TRUE_FOR('{dur}')")

    # INTERVAL + SLIDING
    max_sliding_count = 20
    for _ in range(0, max_sliding_count + 1):
        interval = random.choice(duration_lists)
        offset = random.choice(duration_lists)
        slide = random.choice(duration_lists)
        slide_offset = random.choice(duration_lists)
        int_part = f"INTERVAL('{interval}')"
        int_part_with_offset = f"INTERVAL('{interval}', '{offset}')"
        slide_part = f"SLIDING('{slide}')"
        slide_part_with_offset = f"SLIDING('{slide}', '{slide_offset}')"
        triggers.append(f"{slide_part}")
        triggers.append(f"{slide_part_with_offset}")
        triggers.append(f"{int_part} {slide_part}")
        triggers.append(f"{int_part} {slide_part_with_offset}")
        triggers.append(f"{int_part_with_offset} {slide_part}")
        triggers.append(f"{int_part_with_offset} {slide_part_with_offset}")

    # EVENT_WINDOW
    max_event_count = 20
    for _ in range(0, max_event_count + 1):
        start = generate_logical_condition()
        end = generate_logical_condition()
        ew = f"EVENT_WINDOW(START WITH {start} END WITH {end})"
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
                    triggers.append(f"COUNT_WINDOW({', '.join(parts)})")

    # PERIOD
    max_period_count = 20
    for _ in range(0, max_period_count + 1):
        period = random.choice(duration_lists)
        offset = random.choice(duration_lists)
        triggers.append(f"PERIOD('{period}', '{offset}')")
        triggers.append(f"PERIOD('{period}')")

    return triggers

def generate_partition_variants():
    max_partition_len = 3       # 最多几个列（含重复）
    max_samples_per_len = 5     # 每种长度采样多少个
    partition_clauses = []

    # 枚举不同长度，允许重复组合
    for length in range(1, max_partition_len + 1):
        all_combos = list(product(columns, repeat=length))
        sampled = random.sample(all_combos, min(len(all_combos), max_samples_per_len))
        for combo in sampled:
            clause = f"PARTITION BY {', '.join(combo)}"
            partition_clauses.append(clause)

    # 加入空子句（不带 PARTITION BY）
    partition_clauses.append("")
    return partition_clauses

# helper: 生成 EVENT_TYPE 子句
def generate_event_types():
    types = [random.choice(event_types_pool) for _ in range(random.randint(1, 3))]
    return "|".join(types)

def random_option():
    option_type = random.choice([
        lambda: f"WATERMARK({random.choice(duration_lists)})",
        lambda: f"EXPIRED_TIME({random.choice(duration_lists)})",
        lambda: "IGNORE_DISORDER",
        lambda: "DELETE_RECALC",
        lambda: "DELETE_OUTPUT_TABLE",
        lambda: f"FILL_HISTORY({random.choice(timestamps)})",
        lambda: f"FILL_HISTORY_FIRST({random.choice(timestamps)})",
        lambda: "CALC_NOTIFY_ONLY",
        lambda: "LOW_LATENCY_CALC",
        lambda: f"PRE_FILTER({generate_logical_condition()})",
        lambda: "FORCE_OUTPUT",
        lambda: f"MAX_DELAY({random.choice(duration_lists)})",
        lambda: f"EVENT_TYPE({generate_event_types()})"
    ])
    return option_type()

def generate_options_clauses(n=10, max_options=10):
    options_clauses = []
    for _ in range(n):
        count = random.randint(1, max_options)
        options = [random_option() for _ in range(count)]
        clause = f"OPTIONS({'|'.join(options)})"
        options_clauses.append(clause)
    return options_clauses


def pick_random_combo(source_list, max_len):
    length = random.randint(0, max_len)
    return [random.choice(source_list) for _ in range(length)] if length > 0 else []

def generate_notification_definitions(
        total=5, max_urls=2, max_events=2, max_options=2, max_condition_depth=2
):
    result = []

    for _ in range(total):
        parts = []

        # optional NOTIFY(url [, ...])
        notify_urls = pick_random_combo(urls, max_urls)
        if notify_urls:
            parts.append(f"NOTIFY({', '.join(notify_urls)})")

        # optional ON (event_types)
        selected_events = pick_random_combo(event_types, max_events)
        if selected_events:
            parts.append(f"ON ({'|'.join(selected_events)})")

        # optional WHERE condition（using generate_logical_condition）
        if random.choice([True, False]):
            condition = generate_logical_condition(max_depth=max_condition_depth)
            parts.append(f"WHERE {condition}")

        # optional NOTIFY_OPTIONS(...)
        selected_options = pick_random_combo(notify_option_list, max_options)
        if selected_options:
            parts.append(f"NOTIFY_OPTIONS({'|'.join(selected_options)})")

        result.append(" ".join(parts))

    return result

string_literals = ["'_v1'", "'_2024'", "'_tag'", "'_out'", "'_ts'", "'_X'"]

# 原子表达式：列名或字符串字面量
def random_expr_atom():
    return random.choices(
        population=columns + string_literals,
        weights=[7] * len(columns) + [3] * len(string_literals),
        k=1
    )[0]

# 字符串函数生成器
def gen_concat(_):
    args = [random_expr_atom() for _ in range(random.randint(2, 4))]
    return f"concat({', '.join(args)})"

def gen_upper(expr=None): return f"upper({expr or random_expr_atom()})"
def gen_lower(expr=None): return f"lower({expr or random_expr_atom()})"
def gen_length(expr=None): return f"length({expr or random_expr_atom()})"
def gen_substr(expr=None):
    expr = expr or random_expr_atom()
    start = str(random.randint(0, 3))
    length = str(random.randint(1, 5))
    return f"substr({expr}, {start}, {length})"

def gen_replace(expr=None):
    expr = expr or random_expr_atom()
    search = random.choice(string_literals)
    repl = random.choice(string_literals)
    return f"replace({expr}, {search}, {repl})"

def gen_ltrim(expr=None): return f"ltrim({expr or random_expr_atom()})"
def gen_rtrim(expr=None): return f"rtrim({expr or random_expr_atom()})"
def gen_trim(expr=None): return f"trim({expr or random_expr_atom()})"

string_func_generators = [
    gen_concat, gen_upper, gen_lower, gen_length, gen_substr,
    gen_replace, gen_ltrim, gen_rtrim, gen_trim
]

def generate_tbname_expr(max_depth=3):
    def gen_nested_expr(depth):
        if depth >= max_depth or random.random() < 0.3:
            return random_expr_atom()
        func = random.choice(string_func_generators)
        inner = gen_nested_expr(depth + 1)
        return func(inner)

    return gen_nested_expr(0)

# 生成完整的 OUTPUT_SUBTABLE(...) 语法
def generate_output_subtable(max_depth=3, include_probability=0.7):
    if random.random() > include_probability:
        return ""  # 不包含 OUTPUT_SUBTABLE
    expr = generate_tbname_expr(max_depth=max_depth)
    return f"OUTPUT_SUBTABLE({expr})"

def generate_column_list_section(include_probability=0.8, max_cols=6, with_primary_key_prob=0.6):
    if random.random() > include_probability:
        return ""  # 不生成该部分

    num_cols = random.randint(1, min(max_cols, len(out_columns)))
    selected = random.sample(out_columns, num_cols)

    # 是否生成 PRIMARY KEY
    with_primary = random.random() < with_primary_key_prob
    pk_index = random.randint(0, num_cols - 1) if with_primary else None

    col_defs = []
    for i, col in enumerate(selected):
        if i == pk_index:
            col_defs.append(f"{col} PRIMARY KEY")
        else:
            col_defs.append(col)

    return f"({', '.join(col_defs)})"

def generate_column_tag_section(include_probability=0.8, max_cols=6, with_primary_key_prob=0.6):
    if random.random() > include_probability:
        return ""  # 不生成该部分

    num_cols = random.randint(1, min(max_cols, len(out_columns)))
    selected = random.sample(out_columns, num_cols)

    # 是否生成 PRIMARY KEY
    with_primary = random.random() < with_primary_key_prob
    pk_index = random.randint(0, num_cols - 1) if with_primary else None

    col_defs = []
    for i, col in enumerate(selected):
        if i == pk_index:
            col_defs.append(f"{col} PRIMARY KEY")
        else:
            col_defs.append(col)

    return f"({', '.join(col_defs)})"

out_types = ["BIGINT", "SMALLINT"]

# 工具函数：随机字符串
def random_string(length=5):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# 生成 TAGS 部分
def generate_tags_clause(include_probability=0.7, max_tags=4, allow_comment=True):
    if random.random() > include_probability:
        return ""

    num_tags = random.randint(1, min(max_tags, len(out_tags)))
    selected_tags = random.sample(out_tags, num_tags)

    tag_defs = []
    for tag in selected_tags:
        type_name = random.choice(out_types)
        comment_str = f" COMMENT '{random_string(6)}'" if allow_comment and random.random() < 0.5 else ""
        expr = generate_tbname_expr(max_depth=2)
        tag_defs.append(f"{tag} {type_name}{comment_str} AS {expr}")

    return f"TAGS ({', '.join(tag_defs)})"

def gen_create_stream_variants():
    base_template = "CREATE STREAM{if_not_exists} {stream_name}{stream_options}{into_clause}{output_subtable}{columns}{tags}{as_subquery};"

    # Optional elements
    if_not_exists_opts = ["", " IF NOT EXISTS"]
    as_subquery_opts = ["", " AS SELECT * FROM some_table"]
    db_name_list = ["", "create_stream_db.", "non_exists_db."]

    # Trigger types (one per variant)
    trigger_types = generate_trigger_variants()

    # Partition options
    partition_clauses = generate_partition_variants()

    stream_options = generate_options_clauses(10, max_options=10)

    notify_options = generate_notification_definitions(total = 10)

    # Loop through combinations
    sql_variants = []
    stream_index = 0
    for if_not_exists, dbnm, into, as_subquery in product(
            if_not_exists_opts, db_name_list, into_option_list, as_subquery_opts
    ):
        for tritype in trigger_types:
            for paritem in partition_clauses:
                for stream_opt in stream_options:
                    for notify in notify_options:
                        #sql = base_template.format(
                         #   if_not_exists=if_not_exists,
                         #   stream_name=dbnm + "stream_" + str(stream_index) + "\n",
                         #   stream_options=" " + tritype  + "\n" + " " + paritem + " "  + "\n" + stream_opt + " "  + "\n" + notify  + "\n",
                         #   into_clause=into + "\n",
                         #   output_subtable=" " + generate_output_subtable() + " " + "\n",
                         ##   columns= generate_column_list_section() + "\n",
                          #  tags= generate_tags_clause() + "\n",
                          #  as_subquery=as_subquery + "\n"
                        #)
                       # sql_variants.append(sql.strip())
                        stream_index += 1
                        #if stream_index > 100000:
                            #return sql_variants

    print(stream_index)
    return sql_variants

# Generate and print a few variants
#variants = gen_create_stream_variants()
variants = gen_create_stream_variants()
#for i, sql in enumerate(variants[:100000]):  # limit output for readability
    #print(f"-- Variant {i+1} --")
    #print(sql)
    #print()



"""TDengine DQL Grammar — auto-generated from sql.y, DO NOT EDIT."""
# Source: /root/TDinternal/community/source/libs/parser/inc/sql.y
# Generated: 2026-03-31T21:40:57.188054
# Entry rule: query_or_subquery
# Rules: 113, Terminals: 190

GRAMMAR = {
    "alias_opt": [
        [],  # alt 0
        ["table_alias"],  # alt 1
        ["AS", "table_alias"],  # alt 2
    ],
    "anomaly_col_list": [
        ["expr_or_subquery"],  # alt 0
        ["anomaly_col_list", "NK_COMMA", "expr_or_subquery"],  # alt 1
    ],
    "anti_joined": [
        ["table_reference", "LEFT", "ANTI", "JOIN", "table_reference", "join_on_clause"],  # alt 0
        ["table_reference", "RIGHT", "ANTI", "JOIN", "table_reference", "join_on_clause"],  # alt 1
    ],
    "asof_joined": [
        ["table_reference", "LEFT", "ASOF", "JOIN", "table_reference", "join_on_clause_opt", "jlimit_clause_opt"],  # alt 0
        ["table_reference", "RIGHT", "ASOF", "JOIN", "table_reference", "join_on_clause_opt", "jlimit_clause_opt"],  # alt 1
    ],
    "boolean_primary": [
        ["predicate"],  # alt 0
        ["NK_LP", "boolean_value_expression", "NK_RP"],  # alt 1
    ],
    "boolean_value_expression": [
        ["boolean_primary"],  # alt 0
        ["NOT", "boolean_primary"],  # alt 1
        ["boolean_value_expression", "OR", "boolean_value_expression"],  # alt 2
        ["boolean_value_expression", "AND", "boolean_value_expression"],  # alt 3
    ],
    "case_when_else_opt": [
        [],  # alt 0
        ["ELSE", "common_expression"],  # alt 1
    ],
    "case_when_expression": [
        ["CASE", "when_then_list", "case_when_else_opt", "END"],  # alt 0
        ["CASE", "common_expression", "when_then_list", "case_when_else_opt", "END"],  # alt 1
    ],
    "cols_func": [
        ["COLS"],  # alt 0
    ],
    "cols_func_expression": [
        ["expr_or_subquery"],  # alt 0
        ["NK_STAR"],  # alt 1
        ["expr_or_subquery", "column_alias"],  # alt 2
        ["expr_or_subquery", "AS", "column_alias"],  # alt 3
    ],
    "cols_func_expression_list": [
        ["cols_func_expression"],  # alt 0
        ["cols_func_expression_list", "NK_COMMA", "cols_func_expression"],  # alt 1
    ],
    "cols_func_para_list": [
        ["function_expression", "NK_COMMA", "cols_func_expression_list"],  # alt 0
    ],
    "column_alias": [
        ["NK_ID"],  # alt 0
        ["NK_ALIAS"],  # alt 1
    ],
    "column_name": [
        ["NK_ID"],  # alt 0
    ],
    "column_name_list": [
        ["trigger_col_name"],  # alt 0
        ["column_name_list", "NK_COMMA", "trigger_col_name"],  # alt 1
    ],
    "column_reference": [
        ["column_name"],  # alt 0
        ["table_name", "NK_DOT", "column_name"],  # alt 1
        ["NK_ALIAS"],  # alt 2
        ["table_name", "NK_DOT", "NK_ALIAS"],  # alt 3
    ],
    "common_expression": [
        ["expr_or_subquery"],  # alt 0
        ["boolean_value_expression"],  # alt 1
    ],
    "compare_op": [
        ["quantified_compare_op"],  # alt 0
        ["LIKE"],  # alt 1
        ["NOT", "LIKE"],  # alt 2
        ["MATCH"],  # alt 3
        ["NMATCH"],  # alt 4
        ["REGEXP"],  # alt 5
        ["NOT", "REGEXP"],  # alt 6
        ["CONTAINS"],  # alt 7
    ],
    "count_window_args": [
        ["NK_INTEGER"],  # alt 0
        ["NK_INTEGER", "NK_COMMA", "NK_INTEGER"],  # alt 1
        ["NK_INTEGER", "NK_COMMA", "column_name_list"],  # alt 2
        ["NK_INTEGER", "NK_COMMA", "NK_INTEGER", "NK_COMMA", "column_name_list"],  # alt 3
    ],
    "db_name": [
        ["NK_ID"],  # alt 0
    ],
    "duration_literal": [
        ["NK_VARIABLE"],  # alt 0
    ],
    "every_opt": [
        [],  # alt 0
        ["EVERY", "NK_LP", "duration_literal", "NK_RP"],  # alt 1
    ],
    "expr_or_subquery": [
        ["expression"],  # alt 0
        ["subquery"],  # alt 1
    ],
    "expression": [
        ["literal"],  # alt 0
        ["pseudo_column"],  # alt 1
        ["column_reference"],  # alt 2
        ["function_expression"],  # alt 3
        ["if_expression"],  # alt 4
        ["case_when_expression"],  # alt 5
        ["NK_LP", "expression", "NK_RP"],  # alt 6
        ["NK_PLUS", "expr_or_subquery"],  # alt 7
        ["NK_MINUS", "expr_or_subquery"],  # alt 8
        ["expr_or_subquery", "NK_PLUS", "expr_or_subquery"],  # alt 9
        ["expr_or_subquery", "NK_MINUS", "expr_or_subquery"],  # alt 10
        ["expr_or_subquery", "NK_STAR", "expr_or_subquery"],  # alt 11
        ["expr_or_subquery", "NK_SLASH", "expr_or_subquery"],  # alt 12
        ["expr_or_subquery", "NK_REM", "expr_or_subquery"],  # alt 13
        ["column_reference", "NK_ARROW", "NK_STRING"],  # alt 14
        ["expr_or_subquery", "NK_BITAND", "expr_or_subquery"],  # alt 15
        ["expr_or_subquery", "NK_BITOR", "expr_or_subquery"],  # alt 16
    ],
    "expression_list": [
        ["expr_or_subquery"],  # alt 0
        ["expression_list", "NK_COMMA", "expr_or_subquery"],  # alt 1
    ],
    "extend_literal": [
        ["NK_INTEGER"],  # alt 0
    ],
    "external_window_fill_opt": [
        [],  # alt 0
        ["FILL", "NK_LP", "fill_mode", "NK_RP"],  # alt 1
    ],
    "fill_mode": [
        ["NONE"],  # alt 0
        ["NULL"],  # alt 1
        ["NULL_F"],  # alt 2
        ["LINEAR"],  # alt 3
    ],
    "fill_opt": [
        [],  # alt 0
        ["fill_value"],  # alt 1
        ["FILL", "NK_LP", "fill_mode", "NK_RP"],  # alt 2
        ["FILL", "NK_LP", "fill_position_mode", "NK_RP", "surround_opt"],  # alt 3
        ["FILL", "NK_LP", "fill_position_mode", "NK_COMMA", "expression_list", "NK_RP"],  # alt 4
    ],
    "fill_position_mode": [
        ["PREV"],  # alt 0
        ["NEXT"],  # alt 1
        ["NEAR"],  # alt 2
    ],
    "fill_value": [
        ["FILL", "NK_LP", "VALUE", "NK_RP"],  # alt 0
        ["FILL", "NK_LP", "VALUE", "NK_COMMA", "expression_list", "NK_RP"],  # alt 1
        ["FILL", "NK_LP", "VALUE_F", "NK_RP"],  # alt 2
        ["FILL", "NK_LP", "VALUE_F", "NK_COMMA", "expression_list", "NK_RP"],  # alt 3
    ],
    "from_clause_opt": [
        [],  # alt 0
        ["FROM", "table_reference_list"],  # alt 1
    ],
    "function_expression": [
        ["function_name", "NK_LP", "expression_list", "NK_RP"],  # alt 0
        ["star_func", "NK_LP", "star_func_para_list", "NK_RP"],  # alt 1
        ["cols_func", "NK_LP", "cols_func_para_list", "NK_RP"],  # alt 2
        ["CAST", "NK_LP", "common_expression", "AS", "type_name", "NK_RP"],  # alt 3
        ["CAST", "NK_LP", "common_expression", "AS", "type_name_default_len", "NK_RP"],  # alt 4
        ["POSITION", "NK_LP", "expr_or_subquery", "IN", "expr_or_subquery", "NK_RP"],  # alt 5
        ["TRIM", "NK_LP", "expr_or_subquery", "NK_RP"],  # alt 6
        ["TRIM", "NK_LP", "trim_specification_type", "FROM", "expr_or_subquery", "NK_RP"],  # alt 7
        ["TRIM", "NK_LP", "expr_or_subquery", "FROM", "expr_or_subquery", "NK_RP"],  # alt 8
        ["TRIM", "NK_LP", "trim_specification_type", "expr_or_subquery", "FROM", "expr_or_subquery", "NK_RP"],  # alt 9
        ["substr_func", "NK_LP", "expression_list", "NK_RP"],  # alt 10
        ["substr_func", "NK_LP", "expr_or_subquery", "FROM", "expr_or_subquery", "NK_RP"],  # alt 11
        ["substr_func", "NK_LP", "expr_or_subquery", "FROM", "expr_or_subquery", "FOR", "expr_or_subquery", "NK_RP"],  # alt 12
        ["REPLACE", "NK_LP", "expression_list", "NK_RP"],  # alt 13
        ["literal_func"],  # alt 14
        ["rand_func"],  # alt 15
    ],
    "function_name": [
        ["NK_ID"],  # alt 0
    ],
    "group_by_clause_opt": [
        [],  # alt 0
        ["GROUP", "BY", "group_by_list"],  # alt 1
    ],
    "group_by_list": [
        ["expr_or_subquery"],  # alt 0
        ["group_by_list", "NK_COMMA", "expr_or_subquery"],  # alt 1
    ],
    "having_clause_opt": [
        [],  # alt 0
        ["HAVING", "search_condition"],  # alt 1
    ],
    "hint_list": [
        [],  # alt 0
        ["NK_HINT"],  # alt 1
    ],
    "if_expression": [
        ["IF", "NK_LP", "common_expression", "NK_COMMA", "common_expression", "NK_COMMA", "common_expression", "NK_RP"],  # alt 0
        ["IFNULL", "NK_LP", "common_expression", "NK_COMMA", "common_expression", "NK_RP"],  # alt 1
        ["NVL", "NK_LP", "common_expression", "NK_COMMA", "common_expression", "NK_RP"],  # alt 2
        ["NVL2", "NK_LP", "common_expression", "NK_COMMA", "common_expression", "NK_COMMA", "common_expression", "NK_RP"],  # alt 3
        ["NULLIF", "NK_LP", "common_expression", "NK_COMMA", "common_expression", "NK_RP"],  # alt 4
        ["COALESCE", "NK_LP", "expression_list", "NK_RP"],  # alt 5
    ],
    "in_op": [
        ["IN"],  # alt 0
        ["NOT", "IN"],  # alt 1
    ],
    "in_predicate_value": [
        ["NK_LP", "literal_list", "NK_RP"],  # alt 0
        ["NK_LP", "query_expression", "NK_RP"],  # alt 1
        ["NK_LP", "subquery", "NK_RP"],  # alt 2
    ],
    "inner_joined": [
        ["table_reference", "JOIN", "table_reference", "join_on_clause_opt"],  # alt 0
        ["table_reference", "INNER", "JOIN", "table_reference", "join_on_clause_opt"],  # alt 1
    ],
    "interval_sliding_duration_literal": [
        ["NK_VARIABLE"],  # alt 0
        ["NK_STRING"],  # alt 1
        ["NK_INTEGER"],  # alt 2
        ["NK_QUESTION"],  # alt 3
    ],
    "jlimit_clause_opt": [
        [],  # alt 0
        ["JLIMIT", "unsigned_integer"],  # alt 1
    ],
    "join_on_clause": [
        ["ON", "search_condition"],  # alt 0
    ],
    "join_on_clause_opt": [
        [],  # alt 0
        ["join_on_clause"],  # alt 1
    ],
    "joined_table": [
        ["inner_joined"],  # alt 0
        ["outer_joined"],  # alt 1
        ["semi_joined"],  # alt 2
        ["anti_joined"],  # alt 3
        ["asof_joined"],  # alt 4
        ["win_joined"],  # alt 5
    ],
    "limit_clause_opt": [
        [],  # alt 0
        ["LIMIT", "unsigned_integer"],  # alt 1
        ["LIMIT", "unsigned_integer", "OFFSET", "unsigned_integer"],  # alt 2
        ["LIMIT", "unsigned_integer", "NK_COMMA", "unsigned_integer"],  # alt 3
    ],
    "literal": [
        ["NK_INTEGER"],  # alt 0
        ["NK_FLOAT"],  # alt 1
        ["NK_STRING"],  # alt 2
        ["NK_BOOL"],  # alt 3
        ["TIMESTAMP", "NK_STRING"],  # alt 4
        ["duration_literal"],  # alt 5
        ["NULL"],  # alt 6
        ["NK_QUESTION"],  # alt 7
    ],
    "literal_func": [
        ["noarg_func", "NK_LP", "NK_RP"],  # alt 0
        ["NOW"],  # alt 1
        ["TODAY"],  # alt 2
    ],
    "literal_list": [
        ["signed_literal"],  # alt 0
        ["literal_list", "NK_COMMA", "signed_literal"],  # alt 1
    ],
    "noarg_func": [
        ["NOW"],  # alt 0
        ["TODAY"],  # alt 1
        ["TIMEZONE"],  # alt 2
        ["DATABASE"],  # alt 3
        ["CLIENT_VERSION"],  # alt 4
        ["SERVER_VERSION"],  # alt 5
        ["SERVER_STATUS"],  # alt 6
        ["CURRENT_USER"],  # alt 7
        ["USER"],  # alt 8
        ["PI"],  # alt 9
    ],
    "null_ordering_opt": [
        [],  # alt 0
        ["NULLS", "FIRST"],  # alt 1
        ["NULLS", "LAST"],  # alt 2
    ],
    "order_by_clause_opt": [
        [],  # alt 0
        ["ORDER", "BY", "sort_specification_list"],  # alt 1
    ],
    "ordering_specification_opt": [
        [],  # alt 0
        ["ASC"],  # alt 1
        ["DESC"],  # alt 2
    ],
    "other_para_list": [
        ["star_func_para"],  # alt 0
        ["other_para_list", "NK_COMMA", "star_func_para"],  # alt 1
    ],
    "outer_joined": [
        ["table_reference", "LEFT", "JOIN", "table_reference", "join_on_clause"],  # alt 0
        ["table_reference", "RIGHT", "JOIN", "table_reference", "join_on_clause"],  # alt 1
        ["table_reference", "FULL", "JOIN", "table_reference", "join_on_clause"],  # alt 2
        ["table_reference", "LEFT", "OUTER", "JOIN", "table_reference", "join_on_clause"],  # alt 3
        ["table_reference", "RIGHT", "OUTER", "JOIN", "table_reference", "join_on_clause"],  # alt 4
        ["table_reference", "FULL", "OUTER", "JOIN", "table_reference", "join_on_clause"],  # alt 5
    ],
    "parenthesized_joined_table": [
        ["NK_LP", "joined_table", "NK_RP"],  # alt 0
        ["NK_LP", "parenthesized_joined_table", "NK_RP"],  # alt 1
    ],
    "partition_by_clause_opt": [
        [],  # alt 0
        ["PARTITION", "BY", "partition_list"],  # alt 1
    ],
    "partition_item": [
        ["expr_or_subquery"],  # alt 0
        ["expr_or_subquery", "column_alias"],  # alt 1
        ["expr_or_subquery", "AS", "column_alias"],  # alt 2
    ],
    "partition_list": [
        ["partition_item"],  # alt 0
        ["partition_list", "NK_COMMA", "partition_item"],  # alt 1
    ],
    "predicate": [
        ["expr_or_subquery", "compare_op", "expr_or_subquery"],  # alt 0
        ["expr_or_subquery", "quantified_compare_op", "quantified_expr", "subquery"],  # alt 1
        ["expr_or_subquery", "BETWEEN", "expr_or_subquery", "AND", "expr_or_subquery"],  # alt 2
        ["expr_or_subquery", "NOT", "BETWEEN", "expr_or_subquery", "AND", "expr_or_subquery"],  # alt 3
        ["expr_or_subquery", "IS", "NULL"],  # alt 4
        ["expr_or_subquery", "IS", "NOT", "NULL"],  # alt 5
        ["ISNULL", "NK_LP", "expr_or_subquery", "NK_RP"],  # alt 6
        ["ISNOTNULL", "NK_LP", "expr_or_subquery", "NK_RP"],  # alt 7
        ["expr_or_subquery", "in_op", "in_predicate_value"],  # alt 8
        ["EXISTS", "subquery"],  # alt 9
        ["NOT", "EXISTS", "subquery"],  # alt 10
    ],
    "pseudo_column": [
        ["ROWTS"],  # alt 0
        ["TBNAME"],  # alt 1
        ["table_name", "NK_DOT", "TBNAME"],  # alt 2
        ["QSTART"],  # alt 3
        ["QEND"],  # alt 4
        ["QDURATION"],  # alt 5
        ["WSTART"],  # alt 6
        ["WEND"],  # alt 7
        ["WDURATION"],  # alt 8
        ["IROWTS"],  # alt 9
        ["ISFILLED"],  # alt 10
        ["QTAGS"],  # alt 11
        ["FLOW"],  # alt 12
        ["FHIGH"],  # alt 13
        ["FROWTS"],  # alt 14
        ["IROWTS_ORIGIN"],  # alt 15
        ["TPREV_TS"],  # alt 16
        ["TCURRENT_TS"],  # alt 17
        ["TNEXT_TS"],  # alt 18
        ["TWSTART"],  # alt 19
        ["TWEND"],  # alt 20
        ["TWDURATION"],  # alt 21
        ["TWROWNUM"],  # alt 22
        ["TPREV_LOCALTIME"],  # alt 23
        ["TNEXT_LOCALTIME"],  # alt 24
        ["TLOCALTIME"],  # alt 25
        ["TGRPID"],  # alt 26
        ["NK_PH", "NK_INTEGER"],  # alt 27
        ["NK_PH", "TBNAME"],  # alt 28
        ["IMPROWTS"],  # alt 29
        ["IMPMARK"],  # alt 30
        ["ANOMALYMARK"],  # alt 31
        ["TIDLESTART"],  # alt 32
        ["TIDLEEND"],  # alt 33
    ],
    "quantified_compare_op": [
        ["NK_LT"],  # alt 0
        ["NK_GT"],  # alt 1
        ["NK_LE"],  # alt 2
        ["NK_GE"],  # alt 3
        ["NK_NE"],  # alt 4
        ["NK_EQ"],  # alt 5
    ],
    "quantified_expr": [
        ["ANY"],  # alt 0
        ["SOME"],  # alt 1
        ["ALL"],  # alt 2
    ],
    "query_expression": [
        ["query_simple", "order_by_clause_opt", "slimit_clause_opt", "limit_clause_opt"],  # alt 0
    ],
    "query_or_subquery": [
        ["query_expression"],  # alt 0
        ["subquery"],  # alt 1
    ],
    "query_simple": [
        ["query_specification"],  # alt 0
        ["union_query_expression"],  # alt 1
    ],
    "query_simple_or_subquery": [
        ["query_simple"],  # alt 0
        ["subquery"],  # alt 1
    ],
    "query_specification": [
        ["SELECT", "hint_list", "set_quantifier_opt", "tag_mode_opt", "select_list", "from_clause_opt", "where_clause_opt", "partition_by_clause_opt", "range_opt", "every_opt", "fill_opt", "twindow_clause_opt", "group_by_clause_opt", "having_clause_opt"],  # alt 0
    ],
    "rand_func": [
        ["RAND", "NK_LP", "NK_RP"],  # alt 0
        ["RAND", "NK_LP", "expression_list", "NK_RP"],  # alt 1
    ],
    "range_opt": [
        [],  # alt 0
        ["RANGE", "NK_LP", "expr_or_subquery", "NK_COMMA", "expr_or_subquery", "NK_COMMA", "expr_or_subquery", "NK_RP"],  # alt 1
        ["RANGE", "NK_LP", "expr_or_subquery", "NK_COMMA", "expr_or_subquery", "NK_RP"],  # alt 2
        ["RANGE", "NK_LP", "expr_or_subquery", "NK_RP"],  # alt 3
    ],
    "search_condition": [
        ["common_expression"],  # alt 0
    ],
    "select_item": [
        ["NK_STAR"],  # alt 0
        ["common_expression"],  # alt 1
        ["common_expression", "column_alias"],  # alt 2
        ["common_expression", "AS", "column_alias"],  # alt 3
        ["table_name", "NK_DOT", "NK_STAR"],  # alt 4
    ],
    "select_list": [
        ["select_item"],  # alt 0
        ["select_list", "NK_COMMA", "select_item"],  # alt 1
    ],
    "semi_joined": [
        ["table_reference", "LEFT", "SEMI", "JOIN", "table_reference", "join_on_clause"],  # alt 0
        ["table_reference", "RIGHT", "SEMI", "JOIN", "table_reference", "join_on_clause"],  # alt 1
    ],
    "set_quantifier_opt": [
        [],  # alt 0
        ["DISTINCT"],  # alt 1
        ["ALL"],  # alt 2
    ],
    "signed": [
        ["signed_integer"],  # alt 0
        ["signed_float"],  # alt 1
    ],
    "signed_float": [
        ["NK_FLOAT"],  # alt 0
        ["NK_PLUS", "NK_FLOAT"],  # alt 1
        ["NK_MINUS", "NK_FLOAT"],  # alt 2
    ],
    "signed_integer": [
        ["NK_INTEGER"],  # alt 0
        ["NK_PLUS", "NK_INTEGER"],  # alt 1
        ["NK_MINUS", "NK_INTEGER"],  # alt 2
    ],
    "signed_literal": [
        ["signed"],  # alt 0
        ["NK_STRING"],  # alt 1
        ["NK_BOOL"],  # alt 2
        ["TIMESTAMP", "NK_STRING"],  # alt 3
        ["duration_literal"],  # alt 4
        ["NULL"],  # alt 5
        ["literal_func"],  # alt 6
        ["NK_QUESTION"],  # alt 7
    ],
    "sliding_opt": [
        [],  # alt 0
        ["SLIDING", "NK_LP", "interval_sliding_duration_literal", "NK_RP"],  # alt 1
    ],
    "slimit_clause_opt": [
        [],  # alt 0
        ["SLIMIT", "unsigned_integer"],  # alt 1
        ["SLIMIT", "unsigned_integer", "SOFFSET", "unsigned_integer"],  # alt 2
        ["SLIMIT", "unsigned_integer", "NK_COMMA", "unsigned_integer"],  # alt 3
    ],
    "sort_specification": [
        ["expr_or_subquery", "ordering_specification_opt", "null_ordering_opt"],  # alt 0
    ],
    "sort_specification_list": [
        ["sort_specification"],  # alt 0
        ["sort_specification_list", "NK_COMMA", "sort_specification"],  # alt 1
    ],
    "star_func": [
        ["COUNT"],  # alt 0
        ["FIRST"],  # alt 1
        ["LAST"],  # alt 2
        ["LAST_ROW"],  # alt 3
    ],
    "star_func_para": [
        ["expr_or_subquery"],  # alt 0
        ["table_name", "NK_DOT", "NK_STAR"],  # alt 1
    ],
    "star_func_para_list": [
        ["NK_STAR"],  # alt 0
        ["other_para_list"],  # alt 1
    ],
    "state_window_opt": [
        [],  # alt 0
        ["NK_COMMA", "extend_literal"],  # alt 1
        ["NK_COMMA", "extend_literal", "NK_COMMA", "zeroth_literal"],  # alt 2
    ],
    "subquery": [
        ["NK_LP", "query_expression", "NK_RP"],  # alt 0
        ["NK_LP", "subquery", "NK_RP"],  # alt 1
    ],
    "substr_func": [
        ["SUBSTR"],  # alt 0
        ["SUBSTRING"],  # alt 1
    ],
    "surround_opt": [
        [],  # alt 0
        ["SURROUND", "NK_LP", "duration_literal", "NK_RP"],  # alt 1
        ["SURROUND", "NK_LP", "duration_literal", "NK_COMMA", "expression_list", "NK_RP"],  # alt 2
    ],
    "table_alias": [
        ["NK_ID"],  # alt 0
    ],
    "table_name": [
        ["NK_ID"],  # alt 0
    ],
    "table_primary": [
        ["table_name", "alias_opt"],  # alt 0
        ["db_name", "NK_DOT", "table_name", "alias_opt"],  # alt 1
        ["subquery", "alias_opt"],  # alt 2
        ["parenthesized_joined_table"],  # alt 3
        ["NK_PH", "TBNAME", "alias_opt"],  # alt 4
        ["NK_PH", "TROWS", "alias_opt"],  # alt 5
    ],
    "table_reference": [
        ["table_primary"],  # alt 0
        ["joined_table"],  # alt 1
    ],
    "table_reference_list": [
        ["table_reference"],  # alt 0
        ["table_reference_list", "NK_COMMA", "table_reference"],  # alt 1
    ],
    "tag_mode_opt": [
        [],  # alt 0
        ["TAGS"],  # alt 1
    ],
    "trigger_col_name": [
        ["column_name"],  # alt 0
        ["TBNAME"],  # alt 1
    ],
    "trim_specification_type": [
        ["BOTH"],  # alt 0
        ["TRAILING"],  # alt 1
        ["LEADING"],  # alt 2
    ],
    "true_for_opt": [
        [],  # alt 0
        ["TRUE_FOR", "NK_LP", "interval_sliding_duration_literal", "NK_RP"],  # alt 1
        ["TRUE_FOR", "NK_LP", "COUNT", "NK_INTEGER", "NK_RP"],  # alt 2
        ["TRUE_FOR", "NK_LP", "interval_sliding_duration_literal", "AND", "COUNT", "NK_INTEGER", "NK_RP"],  # alt 3
        ["TRUE_FOR", "NK_LP", "interval_sliding_duration_literal", "OR", "COUNT", "NK_INTEGER", "NK_RP"],  # alt 4
    ],
    "twindow_clause_opt": [
        [],  # alt 0
        ["SESSION", "NK_LP", "column_reference", "NK_COMMA", "interval_sliding_duration_literal", "NK_RP"],  # alt 1
        ["STATE_WINDOW", "NK_LP", "expr_or_subquery", "state_window_opt", "NK_RP", "true_for_opt"],  # alt 2
        ["INTERVAL", "NK_LP", "interval_sliding_duration_literal", "NK_RP", "sliding_opt", "fill_opt"],  # alt 3
        ["INTERVAL", "NK_LP", "interval_sliding_duration_literal", "NK_COMMA", "interval_sliding_duration_literal", "NK_RP", "sliding_opt", "fill_opt"],  # alt 4
        ["INTERVAL", "NK_LP", "interval_sliding_duration_literal", "NK_COMMA", "AUTO", "NK_RP", "sliding_opt", "fill_opt"],  # alt 5
        ["EVENT_WINDOW", "START", "WITH", "search_condition", "END", "WITH", "search_condition", "true_for_opt"],  # alt 6
        ["COUNT_WINDOW", "NK_LP", "count_window_args", "NK_RP"],  # alt 7
        ["ANOMALY_WINDOW", "NK_LP", "anomaly_col_list", "NK_RP"],  # alt 8
        ["EXTERNAL_WINDOW", "NK_LP", "subquery", "table_alias", "external_window_fill_opt", "NK_RP"],  # alt 9
    ],
    "type_name": [
        ["BOOL"],  # alt 0
        ["TINYINT"],  # alt 1
        ["SMALLINT"],  # alt 2
        ["INT"],  # alt 3
        ["INTEGER"],  # alt 4
        ["BIGINT"],  # alt 5
        ["FLOAT"],  # alt 6
        ["DOUBLE"],  # alt 7
        ["BINARY", "NK_LP", "NK_INTEGER", "NK_RP"],  # alt 8
        ["TIMESTAMP"],  # alt 9
        ["NCHAR", "NK_LP", "NK_INTEGER", "NK_RP"],  # alt 10
        ["TINYINT", "UNSIGNED"],  # alt 11
        ["SMALLINT", "UNSIGNED"],  # alt 12
        ["INT", "UNSIGNED"],  # alt 13
        ["BIGINT", "UNSIGNED"],  # alt 14
        ["JSON"],  # alt 15
        ["VARCHAR", "NK_LP", "NK_INTEGER", "NK_RP"],  # alt 16
        ["MEDIUMBLOB"],  # alt 17
        ["BLOB"],  # alt 18
        ["VARBINARY", "NK_LP", "NK_INTEGER", "NK_RP"],  # alt 19
        ["GEOMETRY", "NK_LP", "NK_INTEGER", "NK_RP"],  # alt 20
        ["DECIMAL", "NK_LP", "NK_INTEGER", "NK_RP"],  # alt 21
        ["DECIMAL", "NK_LP", "NK_INTEGER", "NK_COMMA", "NK_INTEGER", "NK_RP"],  # alt 22
    ],
    "type_name_default_len": [
        ["BINARY"],  # alt 0
        ["NCHAR"],  # alt 1
        ["VARCHAR"],  # alt 2
        ["VARBINARY"],  # alt 3
    ],
    "union_query_expression": [
        ["query_simple_or_subquery", "UNION", "ALL", "query_simple_or_subquery"],  # alt 0
        ["query_simple_or_subquery", "UNION", "query_simple_or_subquery"],  # alt 1
    ],
    "unsigned_integer": [
        ["NK_INTEGER"],  # alt 0
        ["NK_QUESTION"],  # alt 1
    ],
    "when_then_expr": [
        ["WHEN", "common_expression", "THEN", "common_expression"],  # alt 0
    ],
    "when_then_list": [
        ["when_then_expr"],  # alt 0
        ["when_then_list", "when_then_expr"],  # alt 1
    ],
    "where_clause_opt": [
        [],  # alt 0
        ["WHERE", "search_condition"],  # alt 1
    ],
    "win_joined": [
        ["table_reference", "LEFT", "WINDOW", "JOIN", "table_reference", "join_on_clause_opt", "window_offset_clause", "jlimit_clause_opt"],  # alt 0
        ["table_reference", "RIGHT", "WINDOW", "JOIN", "table_reference", "join_on_clause_opt", "window_offset_clause", "jlimit_clause_opt"],  # alt 1
    ],
    "window_offset_clause": [
        ["WINDOW_OFFSET", "NK_LP", "window_offset_literal", "NK_COMMA", "window_offset_literal", "NK_RP"],  # alt 0
    ],
    "window_offset_literal": [
        ["NK_VARIABLE"],  # alt 0
        ["NK_MINUS", "NK_VARIABLE"],  # alt 1
    ],
    "zeroth_literal": [
        ["signed_integer"],  # alt 0
        ["NK_STRING"],  # alt 1
        ["NK_BOOL"],  # alt 2
    ],
}

TERMINALS = {
    "ALL",
    "AND",
    "ANOMALYMARK",
    "ANOMALY_WINDOW",
    "ANTI",
    "ANY",
    "AS",
    "ASC",
    "ASOF",
    "AUTO",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BLOB",
    "BOOL",
    "BOTH",
    "BY",
    "CASE",
    "CAST",
    "CLIENT_VERSION",
    "COALESCE",
    "COLS",
    "CONTAINS",
    "COUNT",
    "COUNT_WINDOW",
    "CURRENT_USER",
    "DATABASE",
    "DECIMAL",
    "DESC",
    "DISTINCT",
    "DOUBLE",
    "ELSE",
    "END",
    "EVENT_WINDOW",
    "EVERY",
    "EXISTS",
    "EXTERNAL_WINDOW",
    "FHIGH",
    "FILL",
    "FIRST",
    "FLOAT",
    "FLOW",
    "FOR",
    "FROM",
    "FROWTS",
    "FULL",
    "GEOMETRY",
    "GROUP",
    "HAVING",
    "IF",
    "IFNULL",
    "IMPMARK",
    "IMPROWTS",
    "IN",
    "INNER",
    "INT",
    "INTEGER",
    "INTERVAL",
    "IROWTS",
    "IROWTS_ORIGIN",
    "IS",
    "ISFILLED",
    "ISNOTNULL",
    "ISNULL",
    "JLIMIT",
    "JOIN",
    "JSON",
    "LAST",
    "LAST_ROW",
    "LEADING",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LINEAR",
    "MATCH",
    "MEDIUMBLOB",
    "NCHAR",
    "NEAR",
    "NEXT",
    "NK_ALIAS",
    "NK_ARROW",
    "NK_BITAND",
    "NK_BITOR",
    "NK_BOOL",
    "NK_COMMA",
    "NK_DOT",
    "NK_EQ",
    "NK_FLOAT",
    "NK_GE",
    "NK_GT",
    "NK_HINT",
    "NK_ID",
    "NK_INTEGER",
    "NK_LE",
    "NK_LP",
    "NK_LT",
    "NK_MINUS",
    "NK_NE",
    "NK_PH",
    "NK_PLUS",
    "NK_QUESTION",
    "NK_REM",
    "NK_RP",
    "NK_SLASH",
    "NK_STAR",
    "NK_STRING",
    "NK_VARIABLE",
    "NMATCH",
    "NONE",
    "NOT",
    "NOW",
    "NULL",
    "NULLIF",
    "NULLS",
    "NULL_F",
    "NVL",
    "NVL2",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "PARTITION",
    "PI",
    "POSITION",
    "PREV",
    "QDURATION",
    "QEND",
    "QSTART",
    "QTAGS",
    "RAND",
    "RANGE",
    "REGEXP",
    "REPLACE",
    "RIGHT",
    "ROWTS",
    "SELECT",
    "SEMI",
    "SERVER_STATUS",
    "SERVER_VERSION",
    "SESSION",
    "SLIDING",
    "SLIMIT",
    "SMALLINT",
    "SOFFSET",
    "SOME",
    "START",
    "STATE_WINDOW",
    "SUBSTR",
    "SUBSTRING",
    "SURROUND",
    "TAGS",
    "TBNAME",
    "TCURRENT_TS",
    "TGRPID",
    "THEN",
    "TIDLEEND",
    "TIDLESTART",
    "TIMESTAMP",
    "TIMEZONE",
    "TINYINT",
    "TLOCALTIME",
    "TNEXT_LOCALTIME",
    "TNEXT_TS",
    "TODAY",
    "TPREV_LOCALTIME",
    "TPREV_TS",
    "TRAILING",
    "TRIM",
    "TROWS",
    "TRUE_FOR",
    "TWDURATION",
    "TWEND",
    "TWROWNUM",
    "TWSTART",
    "UNION",
    "UNSIGNED",
    "USER",
    "VALUE",
    "VALUE_F",
    "VARBINARY",
    "VARCHAR",
    "WDURATION",
    "WEND",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WINDOW_OFFSET",
    "WITH",
    "WSTART",
}

ENTRY_RULE = "query_or_subquery"

LIST_PATTERNS = {
    "anomaly_col_list": {"item": "expr_or_subquery", "sep": "NK_COMMA"},
    "cols_func_expression_list": {"item": "cols_func_expression", "sep": "NK_COMMA"},
    "column_name_list": {"item": "trigger_col_name", "sep": "NK_COMMA"},
    "expression_list": {"item": "expr_or_subquery", "sep": "NK_COMMA"},
    "group_by_list": {"item": "expr_or_subquery", "sep": "NK_COMMA"},
    "literal_list": {"item": "signed_literal", "sep": "NK_COMMA"},
    "other_para_list": {"item": "star_func_para", "sep": "NK_COMMA"},
    "partition_list": {"item": "partition_item", "sep": "NK_COMMA"},
    "select_list": {"item": "select_item", "sep": "NK_COMMA"},
    "sort_specification_list": {"item": "sort_specification", "sep": "NK_COMMA"},
    "table_reference_list": {"item": "table_reference", "sep": "NK_COMMA"},
}

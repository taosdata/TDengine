#!/usr/bin/env python3
"""Terminal symbol resolver for the grammar-driven SQL generator.

This module maps grammar terminal tokens to concrete SQL text, bridging
the abstract grammar symbols to actual SQL output.
"""

from .misc import Dice
from .constraints import WalkContext
from .schema_provider import SchemaProvider


# Map sql.y terminal token names to SQL text
KEYWORD_MAP = {
    # SQL keywords
    "SELECT": "SELECT", "FROM": "FROM", "WHERE": "WHERE",
    "AS": "AS", "ON": "ON", "IN": "IN", "AND": "AND", "OR": "OR",
    "NOT": "NOT", "IS": "IS", "NULL": "NULL", "LIKE": "LIKE",
    "BETWEEN": "BETWEEN", "EXISTS": "EXISTS",
    "JOIN": "JOIN", "INNER": "INNER", "LEFT": "LEFT", "RIGHT": "RIGHT",
    "FULL": "FULL", "OUTER": "OUTER", "SEMI": "SEMI", "ANTI": "ANTI",
    "ASOF": "ASOF", "WINDOW": "WINDOW",
    "UNION": "UNION", "ALL": "ALL", "MINUS": "MINUS",
    "EXCEPT": "EXCEPT", "INTERSECT": "INTERSECT",
    "DISTINCT": "DISTINCT",
    "GROUP": "GROUP", "BY": "BY", "HAVING": "HAVING",
    "ORDER": "ORDER", "ASC": "ASC", "DESC": "DESC",
    "LIMIT": "LIMIT", "OFFSET": "OFFSET", "SLIMIT": "SLIMIT", "SOFFSET": "SOFFSET",
    "PARTITION": "PARTITION",
    "INTERVAL": "INTERVAL", "SLIDING": "SLIDING", "FILL": "FILL",
    "SESSION": "SESSION", "STATE_WINDOW": "STATE_WINDOW",
    "EVENT_WINDOW": "EVENT_WINDOW", "COUNT_WINDOW": "COUNT_WINDOW",
    "ANOMALY_WINDOW": "ANOMALY_WINDOW", "EXTERNAL_WINDOW": "EXTERNAL_WINDOW",
    "START": "START", "END": "END", "WITH": "WITH",
    "CASE": "CASE", "WHEN": "WHEN", "THEN": "THEN", "ELSE": "ELSE",
    "CAST": "CAST", "POSITION": "POSITION", "TRIM": "TRIM",
    "SUBSTR": "SUBSTR", "SUBSTRING": "SUBSTRING", "REPLACE": "REPLACE",
    "RANGE": "RANGE", "EVERY": "EVERY",
    "NULLS": "NULLS", "FIRST": "FIRST", "LAST": "LAST",
    "TRUE": "TRUE", "FALSE": "FALSE",
    "JLIMIT": "JLIMIT", "WINDOW_OFFSET": "WINDOW_OFFSET",
    "PREV": "PREV", "NEXT": "NEXT", "LINEAR": "LINEAR", "NONE": "NONE",
    "VALUE": "VALUE", "VALUE_F": "VALUE_F",
    "BOTH": "BOTH", "LEADING": "LEADING", "TRAILING": "TRAILING",
    "TAGS": "TAGS", "TBNAME": "TBNAME",
    "NOW": "NOW", "TODAY": "TODAY", "TIMEZONE": "TIMEZONE",
    "DATABASE": "DATABASE", "CLIENT_VERSION": "CLIENT_VERSION",
    "SERVER_VERSION": "SERVER_VERSION", "SERVER_STATUS": "SERVER_STATUS",
    "CURRENT_USER": "CURRENT_USER", "USER": "USER", "PI": "PI",
    "COUNT": "COUNT", "RAND": "RAND",
    "FOR": "FOR", "AUTO": "AUTO", "TRUE_FOR": "TRUE_FOR",
    "MATCH": "MATCH", "NMATCH": "NMATCH", "REGEXP": "REGEXP",
    "CONTAINS": "CONTAINS", "ANY": "ANY", "SOME": "SOME",
    "IF": "IF", "COALESCE": "COALESCE", "ANOMALYMARK": "ANOMALYMARK",
    "COLS": "COLS", "FHIGH": "FHIGH", "FLOW": "FLOW", "FROWTS": "FROWTS",
    "SURROUND": "SURROUND", "TGRPID": "TGRPID", "TROWS": "TROWS",
    "TCURRENT_TS": "TCURRENT_TS", "TIDLEEND": "TIDLEEND", "TIDLESTART": "TIDLESTART",
    "TLOCALTIME": "TLOCALTIME", "TNEXT_LOCALTIME": "TNEXT_LOCALTIME",
    "TNEXT_TS": "TNEXT_TS", "TPREV_LOCALTIME": "TPREV_LOCALTIME", "TPREV_TS": "TPREV_TS",
    "TWDURATION": "TWDURATION", "TWEND": "TWEND", "TWROWNUM": "TWROWNUM", "TWSTART": "TWSTART",
    
    # Operators and punctuation
    "NK_COMMA": ",", "NK_LP": "(", "NK_RP": ")",
    "NK_DOT": ".", "NK_STAR": "*", "NK_SEMI": ";",
    "NK_PLUS": "+", "NK_MINUS": "-", "NK_SLASH": "/", "NK_REM": "%",
    "NK_EQ": "=", "NK_NE": "!=", "NK_LT": "<", "NK_GT": ">",
    "NK_LE": "<=", "NK_GE": ">=",
    "NK_BITAND": "&", "NK_BITOR": "|",
    "NK_LSHIFT": "<<", "NK_RSHIFT": ">>",
    "NK_ARROW": "->", "NK_CONCAT": "||",
    "NK_QUESTION": "?", "NK_PH": "?",
    "NK_HINT": "/*+ no_batch_scan */",  # complete hint with closing marker
    # "NK_ALIAS": "@", "NK_ID": "id",  # Now handled by improved logic in resolve()
    
    # Pseudo columns  
    "WSTART": "_wstart", "WEND": "_wend", "WDURATION": "_wduration",
    "QSTART": "_qstart", "QEND": "_qend",
    "ROWTS": "_rowts", "IROWTS": "_irowts",
    
    # Data types
    "BOOL": "BOOL", "TINYINT": "TINYINT", "SMALLINT": "SMALLINT",
    "INT": "INT", "INTEGER": "INTEGER", "BIGINT": "BIGINT", "FLOAT": "FLOAT", "DOUBLE": "DOUBLE",
    "DECIMAL": "DECIMAL", "BINARY": "BINARY", "NCHAR": "NCHAR",
    "VARCHAR": "VARCHAR", "VARBINARY": "VARBINARY", "TIMESTAMP": "TIMESTAMP",
    "BLOB": "BLOB", "MEDIUMBLOB": "MEDIUMBLOB", "JSON": "JSON",
    "GEOMETRY": "GEOMETRY", "UNSIGNED": "UNSIGNED",
    
    # Additional functions and keywords
    "IFNULL": "IFNULL", "NULLIF": "NULLIF", "NVL": "NVL", "NVL2": "NVL2",
    "ISNULL": "ISNULL", "ISNOTNULL": "ISNOTNULL", "ISFILLED": "ISFILLED",
    "LAST_ROW": "LAST_ROW", "NULL_F": "NULL", "NEAR": "NEAR",
    "IMPMARK": "_imark", "IMPROWTS": "_irowts", "IROWTS_ORIGIN": "_irowts_origin",
    "QDURATION": "_qduration", "QTAGS": "_qtags",
}


class TerminalResolver:
    """Resolve grammar terminal symbols to concrete SQL text."""

    def __init__(self, schema: SchemaProvider):
        self._schema = schema

    def resolve(self, token: str, ctx: WalkContext) -> str:
        """Main entry: resolve any terminal token to SQL text."""
        
        # 1. Keyword/operator direct map
        if token in KEYWORD_MAP:
            return KEYWORD_MAP[token]

        # 2. Identifiers (from schema)
        if token == "table_name":
            return self._schema.rand_table_name_only()
        if token == "column_name":
            return self._schema.rand_column(
                need_numeric=ctx.need_numeric,
                need_string=ctx.need_string)
        if token == "db_name":
            return self._schema.get_db()
        if token == "function_name":
            return self._resolve_function_name(ctx)
        if token in ("table_alias", "column_alias"):
            return f"a{Dice.throw(100)}"
        if token == "star_func_para_list":
            return Dice.choice(["*", self._schema.rand_column()])
        
        # 3. Literals
        if token in ("NK_INTEGER", "unsigned_integer"):
            return str(Dice.throw(1000) + 1)
        if token == "NK_FLOAT":
            return f"{Dice.throw(1000)}.{Dice.throw(100):02d}"
        if token == "NK_STRING":
            return Dice.choice(["'hello'", "'test'", "'abc'", "'中文'", "'val_1'"])
        if token == "NK_BOOL":
            return Dice.choice(["TRUE", "FALSE"])
        if token in ("duration_literal", "interval_sliding_duration_literal"):
            return Dice.choice(["1s", "5s", "10s", "30s", "1m", "5m", "1h", "1d"])
        if token == "NK_VARIABLE":
            return Dice.choice(["1s", "5s", "10s", "1m", "5m"])

        # 4. Type names (for CAST)
        if token in ("type_name", "type_name_default_len"):
            return Dice.choice(["INT", "BIGINT", "FLOAT", "DOUBLE",
                                "BINARY(64)", "NCHAR(64)", "TIMESTAMP",
                                "BOOL", "SMALLINT", "TINYINT"])

        # 5. Generic identifiers (fallback)
        if token == "NK_ID":
            # Generic identifier fallback — use column name 
            return self._schema.rand_column()
        if token == "NK_ALIAS":
            return f"t{Dice.throw(10)}"

        # 6. Fallback: return token as-is (might be an unmapped keyword)
        return token

    def _resolve_function_name(self, ctx: WalkContext) -> str:
        """Choose a function name based on category weights."""
        from .function_registry import FUNCTION_CATEGORIES, CATEGORY_WEIGHTS
        
        # Weighted category selection
        categories = list(CATEGORY_WEIGHTS.keys())
        weights = list(CATEGORY_WEIGHTS.values())
        total = sum(weights)
        r = Dice.throw(total)
        cumulative = 0
        chosen_cat = categories[0]
        for cat, w in zip(categories, weights):
            cumulative += w
            if r < cumulative:
                chosen_cat = cat
                break

        funcs = FUNCTION_CATEGORIES.get(chosen_cat, [])
        if not funcs:
            return "COUNT"

        func = Dice.choice(funcs)
        name = func["name"]
        ctx.used_functions.add(name)
        
        # Track aggregate status
        from .function_registry import AGGREGATE_NAMES, SELECTION_NAMES
        if name in AGGREGATE_NAMES or name in SELECTION_NAMES:
            ctx.has_aggregate = True

        return name
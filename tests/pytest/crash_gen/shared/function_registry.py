#!/usr/bin/env python3
"""
TDengine Function Registry - Comprehensive registry of all built-in functions
"""

from typing import Dict, Set, List, Any

# Type alias for function definitions
FuncDef = Dict[str, Any]

# Function Categories with complete definitions

SCALAR_MATH: List[FuncDef] = [
    {"name": "ABS", "args": "ncol"},
    {"name": "ACOS", "args": "ncol"},
    {"name": "ASIN", "args": "ncol"},
    {"name": "ATAN", "args": "ncol"},
    {"name": "CEIL", "args": "ncol"},
    {"name": "COS", "args": "ncol"},
    {"name": "DEGREES", "args": "ncol"},
    {"name": "EXP", "args": "ncol"},
    {"name": "FLOOR", "args": "ncol"},
    {"name": "LN", "args": "ncol"},
    {"name": "LOG", "args": "ncol,int"},
    {"name": "MOD", "args": "ncol,int"},
    {"name": "PI", "args": "none"},
    {"name": "POW", "args": "ncol,int"},
    {"name": "RADIANS", "args": "ncol"},
    {"name": "RAND", "args": "none"},
    {"name": "ROUND", "args": "ncol"},
    {"name": "SIGN", "args": "ncol"},
    {"name": "SIN", "args": "ncol"},
    {"name": "SQRT", "args": "ncol"},
    {"name": "TAN", "args": "ncol"},
    {"name": "TRUNCATE", "args": "ncol,int"},
    {"name": "GREATEST", "args": "ncol,ncol"},
    {"name": "LEAST", "args": "ncol,ncol"},
]

SCALAR_STRING: List[FuncDef] = [
    {"name": "CHAR_LENGTH", "args": "scol"},
    {"name": "CONCAT", "args": "scol,scol"},
    {"name": "CONCAT_WS", "args": "str,scol,scol"},
    {"name": "LENGTH", "args": "scol"},
    {"name": "LOWER", "args": "scol"},
    {"name": "LTRIM", "args": "scol"},
    {"name": "RTRIM", "args": "scol"},
    {"name": "SUBSTR", "args": "scol,int,int"},
    {"name": "UPPER", "args": "scol"},
    {"name": "TRIM", "args": "scol"},
    {"name": "REPLACE", "args": "scol,str,str"},
    {"name": "REPEAT", "args": "scol,int"},
    {"name": "POSITION", "args": "str_in_scol"},
    {"name": "SUBSTRING_INDEX", "args": "scol,str,int"},
]

SCALAR_CONVERT: List[FuncDef] = [
    {"name": "CAST", "args": "expr,type"},
    {"name": "TO_ISO8601", "args": "ts_col"},
    {"name": "TO_UNIXTIMESTAMP", "args": "str"},
    {"name": "TO_CHAR", "args": "ts_col,str"},
    {"name": "TO_TIMESTAMP", "args": "str,str"},
]

SCALAR_DATETIME: List[FuncDef] = [
    {"name": "NOW", "args": "none"},
    {"name": "TODAY", "args": "none"},
    {"name": "TIMEZONE", "args": "none"},
    {"name": "TIMEDIFF", "args": "ts_col,ts_col"},
    {"name": "TIMETRUNCATE", "args": "ts_col,dur"},
    {"name": "DAYOFWEEK", "args": "ts_col"},
    {"name": "WEEKDAY", "args": "ts_col"},
    {"name": "WEEK", "args": "ts_col"},
    {"name": "WEEKOFYEAR", "args": "ts_col"},
]

SCALAR_HASH: List[FuncDef] = [
    {"name": "MD5", "args": "scol"},
    {"name": "SHA1", "args": "scol"},
    {"name": "SHA2", "args": "scol,int"},
    {"name": "TO_BASE64", "args": "scol"},
    {"name": "FROM_BASE64", "args": "scol"},
    {"name": "CRC32", "args": "scol"},
]

AGGREGATE: List[FuncDef] = [
    {"name": "COUNT", "args": "star"},
    {"name": "AVG", "args": "ncol"},
    {"name": "SUM", "args": "ncol"},
    {"name": "STDDEV", "args": "ncol"},
    {"name": "VARIANCE", "args": "ncol"},
    {"name": "MIN", "args": "col"},
    {"name": "MAX", "args": "col"},
    {"name": "FIRST", "args": "col"},
    {"name": "LAST", "args": "col"},
    {"name": "SPREAD", "args": "ncol"},
    {"name": "APERCENTILE", "args": "ncol,int"},
    {"name": "PERCENTILE", "args": "ncol,int"},
    {"name": "LEASTSQUARES", "args": "ncol,int,int"},
    {"name": "HYPERLOGLOG", "args": "col"},
    {"name": "ELAPSED", "args": "ts_col"},
    {"name": "GROUP_CONCAT", "args": "scol"},
]

SELECTION: List[FuncDef] = [
    {"name": "TOP", "args": "ncol,int"},
    {"name": "BOTTOM", "args": "ncol,int"},
    {"name": "LAST_ROW", "args": "col", "no_subquery": True},
    {"name": "TAIL", "args": "col,int"},
    {"name": "UNIQUE", "args": "col"},
    {"name": "MODE", "args": "col"},
    {"name": "SAMPLE", "args": "col,int"},
]

TIMESERIES: List[FuncDef] = [
    {"name": "DIFF", "args": "ncol", "no_window": True},
    {"name": "DERIVATIVE", "args": "ncol,dur,int", "no_window": True},
    {"name": "CSUM", "args": "ncol", "no_window": True},
    {"name": "MAVG", "args": "ncol,int", "no_window": True},
    {"name": "IRATE", "args": "ncol", "no_window": True},
    {"name": "TWA", "args": "ncol", "need_window": True},
    {"name": "STATECOUNT", "args": "ncol,str,ncol", "no_window": True},
    {"name": "STATEDURATION", "args": "ncol,str,ncol", "no_window": True},
    {"name": "FILL_FORWARD", "args": "col", "no_window": True},
]

COMPARISON: List[FuncDef] = [
    {"name": "IF", "args": "cond,expr,expr"},
    {"name": "IFNULL", "args": "col,expr"},
    {"name": "NVL", "args": "col,expr"},
    {"name": "NVL2", "args": "col,expr,expr"},
    {"name": "NULLIF", "args": "col,col"},
]

SYSTEM: List[FuncDef] = [
    {"name": "DATABASE", "args": "none"},
    {"name": "CLIENT_VERSION", "args": "none"},
    {"name": "SERVER_VERSION", "args": "none"},
    {"name": "SERVER_STATUS", "args": "none"},
    {"name": "CURRENT_USER", "args": "none"},
]

# Category weights for selection probability
CATEGORY_WEIGHTS = {
    "scalar_math": 30,
    "scalar_string": 15,
    "scalar_convert": 8,
    "scalar_datetime": 8,
    "scalar_hash": 4,
    "aggregate": 25,
    "selection": 12,
    "timeseries": 10,
    "comparison": 5,
    "system": 3,
}

# Build derived constants
ALL_FUNCTIONS: Dict[str, FuncDef] = {}
AGGREGATE_NAMES: Set[str] = set()
SELECTION_NAMES: Set[str] = set()
TIMESERIES_NAMES: Set[str] = set()
SCALAR_NAMES: Set[str] = set()

# Populate the derived constants
def _build_registries():
    """Build all derived registries from category definitions"""
    global ALL_FUNCTIONS, AGGREGATE_NAMES, SELECTION_NAMES, TIMESERIES_NAMES, SCALAR_NAMES
    
    # Collect all functions
    all_categories = {
        "scalar_math": SCALAR_MATH,
        "scalar_string": SCALAR_STRING,
        "scalar_convert": SCALAR_CONVERT,
        "scalar_datetime": SCALAR_DATETIME,
        "scalar_hash": SCALAR_HASH,
        "aggregate": AGGREGATE,
        "selection": SELECTION,
        "timeseries": TIMESERIES,
        "comparison": COMPARISON,
        "system": SYSTEM,
    }
    
    for category_name, functions in all_categories.items():
        for func_def in functions:
            name = func_def["name"]
            ALL_FUNCTIONS[name] = func_def
            
            # Build category-specific sets
            if category_name == "aggregate":
                AGGREGATE_NAMES.add(name)
            elif category_name == "selection":
                SELECTION_NAMES.add(name)
            elif category_name == "timeseries":
                TIMESERIES_NAMES.add(name)
            elif category_name.startswith("scalar_") or category_name in ("comparison", "system"):
                SCALAR_NAMES.add(name)

# Initialize the registries
_build_registries()

# Export the categories for easy access
FUNCTION_CATEGORIES = {
    "scalar_math": SCALAR_MATH,
    "scalar_string": SCALAR_STRING,
    "scalar_convert": SCALAR_CONVERT,
    "scalar_datetime": SCALAR_DATETIME,
    "scalar_hash": SCALAR_HASH,
    "aggregate": AGGREGATE,
    "selection": SELECTION,
    "timeseries": TIMESERIES,
    "comparison": COMPARISON,
    "system": SYSTEM,
}

def get_function_by_name(name: str) -> FuncDef:
    """Get function definition by name"""
    return ALL_FUNCTIONS.get(name.upper())

def get_functions_by_category(category: str) -> List[FuncDef]:
    """Get all functions in a category"""
    return FUNCTION_CATEGORIES.get(category, [])

def is_aggregate_function(name: str) -> bool:
    """Check if function is aggregate"""
    return name.upper() in AGGREGATE_NAMES

def is_selection_function(name: str) -> bool:
    """Check if function is selection"""
    return name.upper() in SELECTION_NAMES

def is_timeseries_function(name: str) -> bool:
    """Check if function is timeseries"""
    return name.upper() in TIMESERIES_NAMES

def is_scalar_function(name: str) -> bool:
    """Check if function is scalar"""
    return name.upper() in SCALAR_NAMES

def get_function_count():
    """Get function counts by category"""
    counts = {}
    for category, functions in FUNCTION_CATEGORIES.items():
        counts[category] = len(functions)
    counts["total"] = len(ALL_FUNCTIONS)
    return counts

if __name__ == "__main__":
    # Print function counts for verification
    counts = get_function_count()
    print("Function Registry Statistics:")
    print(f"Total functions: {counts['total']}")
    print("\nFunctions by category:")
    for category, count in sorted(counts.items()):
        if category != "total":
            print(f"  {category}: {count}")
    
    # Verification tests
    print("\nVerification tests:")
    print(f"COUNT in AGGREGATE_NAMES: {'COUNT' in AGGREGATE_NAMES}")
    print(f"ABS in SCALAR_NAMES: {'ABS' in SCALAR_NAMES}")
    print(f"DIFF not in AGGREGATE_NAMES: {'DIFF' not in AGGREGATE_NAMES}")
    print(f"Total function count: {len(ALL_FUNCTIONS)}")
#!/usr/bin/env python3
"""
真正的语法树驱动 SQL 生成器
基于 TDengine sql.y 文件，全面覆盖 SELECT 查询语法

覆盖特性:
- query_specification 完整子句: hint_list, tag_mode_opt, set_quantifier_opt,
  select_list, from_clause_opt, where_clause_opt, partition_by_clause_opt,
  range_opt, every_opt, interp_fill_opt, twindow_clause_opt,
  group_by_clause_opt, having_clause_opt
- query_expression: order_by_clause_opt, slimit_clause_opt, limit_clause_opt
- 所有 JOIN 类型: INNER/LEFT/RIGHT/FULL OUTER/SEMI/ANTI/ASOF/WINDOW
- 所有窗口类型: INTERVAL/SESSION/STATE_WINDOW/EVENT_WINDOW/COUNT_WINDOW/ANOMALY_WINDOW
- 完整谓词: compare/BETWEEN/IS NULL/IN/LIKE/MATCH/NMATCH/REGEXP/CONTAINS
- 完整表达式: 伪列/CASE WHEN/IF/NVL/CAST/TRIM/POSITION/REPLACE/SUBSTR
- UNION/UNION ALL, 子查询(FROM/WHERE), INTERP 查询
"""

import re
import random
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional, Tuple
from pathlib import Path


@dataclass
class Production:
    """产生式（一条语法规则）"""
    lhs: str
    rhs: List[str]

    def __repr__(self):
        return f"{self.lhs} ::= {' '.join(self.rhs)}"

    @property
    def complexity(self) -> int:
        return len(self.rhs)


@dataclass
class Grammar:
    """语法定义"""
    productions: Dict[str, List[Production]] = field(default_factory=dict)
    terminals: Set[str] = field(default_factory=set)
    nonterminals: Set[str] = field(default_factory=set)

    def add_production(self, prod: Production):
        if prod.lhs not in self.productions:
            self.productions[prod.lhs] = []
        self.productions[prod.lhs].append(prod)
        self.nonterminals.add(prod.lhs)
        for symbol in prod.rhs:
            if self._is_terminal(symbol):
                self.terminals.add(symbol)
            elif symbol:
                self.nonterminals.add(symbol)

    def _is_terminal(self, symbol: str) -> bool:
        return (symbol.isupper() or
                symbol.startswith('NK_') or
                symbol.startswith('TK_'))


class LemonParser:
    """Lemon 语法文件解析器（改进版：支持跨行产生式 + 嵌套花括号剥离）"""

    def __init__(self, sql_y_path: str):
        self.sql_y_path = sql_y_path
        self.grammar = Grammar()

    def parse(self) -> Grammar:
        with open(self.sql_y_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        # 第一步：去掉 C 动作块（支持嵌套花括号）
        content_no_actions = self._strip_c_actions(content)

        # 第二步：去掉单行注释
        content_no_actions = re.sub(r'//[^\n]*', '', content_no_actions)

        # 第三步：提取产生式
        # 改进正则：lhs(A) ::= rhs. 其中 rhs 不含 '.' 结尾（终止符为独立的 '.'）
        # 使用多行模式，允许跨行
        pattern = r'([a-z_][a-z_0-9]*)\s*\(A\)\s*::=([^;]*?)\.'
        matches = re.findall(pattern, content_no_actions, re.DOTALL)

        for lhs, rhs_str in matches:
            rhs_str = re.sub(r'\s+', ' ', rhs_str).strip()
            # 过滤掉 %type / %destructor 等 lemon 指令块后的残留
            if rhs_str.startswith('%') or rhs_str.startswith('#'):
                continue
            if not rhs_str:
                prod = Production(lhs=lhs, rhs=[])
                self.grammar.add_production(prod)
            else:
                rhs = self._parse_rhs(rhs_str)
                prod = Production(lhs=lhs, rhs=rhs)
                self.grammar.add_production(prod)

        return self.grammar

    @staticmethod
    def _strip_c_actions(content: str) -> str:
        """去掉所有 C 动作块 { ... }，正确处理嵌套花括号"""
        result = []
        depth = 0
        i = 0
        n = len(content)
        while i < n:
            ch = content[i]
            if ch == '{':
                depth += 1
                i += 1
            elif ch == '}':
                depth -= 1
                i += 1
            elif depth == 0:
                result.append(ch)
                i += 1
            else:
                i += 1
        return ''.join(result)

    def _parse_rhs(self, rhs_str: str) -> List[str]:
        symbols = []
        # 匹配 TOKEN(X) 或 token(X) 或纯大写 TOKEN 或纯小写 token
        tokens = re.findall(r'[A-Z_][A-Z_0-9]*\([A-Z]\)|[a-z_][a-z_0-9]*\([A-Z]\)|[A-Z_][A-Z_0-9]*|[a-z_][a-z_0-9]+', rhs_str)
        for token in tokens:
            symbol = re.sub(r'\([A-Z]\)', '', token)
            if symbol and symbol not in {'.', ',', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N'}:
                symbols.append(symbol)
        return symbols


class GrammarBasedSQLGenerator:
    """
    基于语法树的 SQL 生成器 - 全面覆盖 sql.y SELECT 查询语法

    hybrid 模式: 使用简化的手写生成函数，覆盖所有 sql.y SELECT 特性
    full_grammar 模式: 直接递归展开 sql.y 产生式
    """

    # ==================== 常量定义 ====================

    # 伪列 (sql.y pseudo_column 全部 26+ 种)
    PSEUDO_COLS_BASIC = ['_rowts', 'tbname']
    PSEUDO_COLS_WINDOW = ['_wstart', '_wend', '_wduration']
    PSEUDO_COLS_QUERY = ['_qstart', '_qend', '_qduration']
    PSEUDO_COLS_INTERP = ['_isfilled', '_irowts']
    PSEUDO_COLS_ADVANCED = [
        '_qtags', '_flow', '_fhigh', '_frowts', '_irowts_origin',
        '_tprev_ts', '_tcurrent_ts', '_tnext_ts',
        '_twstart', '_twend', '_twduration', '_twrownum',
        '_tprev_localtime', '_tnext_localtime', '_tlocaltime', '_tgrpid',
        '_improwts', '_impmark', '_anomalymark',
    ]

    # 聚合函数 (sql.y star_func + function_name 中的聚合)
    SAFE_AGG_FUNCS = ['COUNT', 'SUM', 'AVG', 'STDDEV', 'MIN', 'MAX', 'FIRST', 'LAST']
    STAR_FUNCS = ['COUNT', 'FIRST', 'LAST', 'LAST_ROW']

    # 标量函数
    SCALAR_NUMERIC = ['ABS', 'CEIL', 'FLOOR', 'ROUND', 'SQRT',
                      'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN',
                      'POW', 'LOG', 'SIGN', 'MOD']
    SCALAR_STRING = ['CHAR_LENGTH', 'LENGTH', 'LOWER', 'UPPER',
                     'LTRIM', 'RTRIM', 'CONCAT', 'REVERSE']
    SCALAR_TIME = ['TO_ISO8601', 'TIMETRUNCATE', 'TIMEDIFF', 'TO_CHAR']

    # 无参函数 (sql.y noarg_func)
    NOARG_FUNCS = ['NOW', 'TODAY', 'TIMEZONE', 'DATABASE', 'CLIENT_VERSION',
                   'SERVER_VERSION', 'SERVER_STATUS', 'CURRENT_USER', 'USER', 'PI']

    # CAST 类型
    CAST_TYPES = ['BIGINT', 'INT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE',
                  'BINARY(64)', 'NCHAR(64)', 'TIMESTAMP', 'BOOL', 'VARCHAR(64)']

    # 比较运算符 (sql.y compare_op)
    COMPARE_OPS = ['<', '>', '<=', '>=', '!=', '=']

    # FILL 模式 (sql.y fill_mode + fill_value，含 VALUE_F)
    FILL_MODES = ['NONE', 'NULL', 'NULL_F', 'LINEAR', 'PREV', 'NEXT', 'VALUE', 'VALUE_F']
    INTERP_FILL_MODES = FILL_MODES + ['NEAR']

    # JOIN 类型 (sql.y joined_table 所有变体)
    JOIN_TYPES = [
        'JOIN', 'INNER JOIN',
        'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN',
        'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN',
        'LEFT SEMI JOIN', 'RIGHT SEMI JOIN',
        'LEFT ANTI JOIN', 'RIGHT ANTI JOIN',
        'LEFT ASOF JOIN', 'RIGHT ASOF JOIN',
        'LEFT WINDOW JOIN', 'RIGHT WINDOW JOIN',
    ]

    # 时间间隔
    DURATIONS = ['1s', '2s', '5s', '10s', '30s', '1m', '5m', '10m', '30m', '1h']
    SHORT_DURATIONS = ['1s', '2s', '5s', '10s', '30s']

    # ==================== 初始化 ====================

    def __init__(self, sql_y_path: str, seed=None, mode='hybrid'):
        self.rng = random.Random(seed)
        self.mode = mode

        # 解析 sql.y
        parser = LemonParser(sql_y_path)
        self.grammar = parser.parse()

        # 深度控制：full_grammar 增大深度以避免过早截断
        self.max_depth = 12 if mode == 'full_grammar' else 15
        self.current_depth = 0

        # 上下文 (由 set_context 覆盖)
        self.context = {
            'db': 'test_db',
            'table': 'test_stable',
            'columns': {'temperature': 'INT', 'humidity': 'FLOAT',
                        'pressure': 'DOUBLE', 'voltage': 'BIGINT'},
            'tags': {'region': 'BINARY(64)', 'device_id': 'INT',
                     'group_id': 'NCHAR(32)'},
        }
        self._update_col_categories()

        # 必需规则 (full_grammar 模式)：这些非终结符强制选非空产生式
        self.required_rules = {
            'select_list', 'select_item', 'table_reference',
            'table_primary', 'expr_or_subquery', 'expression',
            'common_expression',
        }

    def _update_col_categories(self):
        """根据 context 中的列类型信息分类列"""
        columns = self.context.get('columns', {})
        if isinstance(columns, list):
            self.context['numeric_cols'] = list(columns)
            self.context['string_cols'] = []
            self.context['ts_col'] = 'ts'
            return
        numeric, string, ts = [], [], 'ts'
        for name, ctype in columns.items():
            ct = str(ctype).upper()
            if 'TIMESTAMP' in ct:
                ts = name
            elif any(t in ct for t in ['INT', 'FLOAT', 'DOUBLE', 'BIGINT',
                                        'SMALLINT', 'TINYINT', 'DECIMAL', 'BOOL']):
                numeric.append(name)
            elif any(t in ct for t in ['BINARY', 'NCHAR', 'VARCHAR', 'CHAR',
                                        'GEOMETRY', 'VARBINARY']):
                string.append(name)
            else:
                numeric.append(name)
        self.context['numeric_cols'] = numeric
        self.context['string_cols'] = string
        self.context['ts_col'] = ts

    def set_context(self, context: Dict):
        """设置生成上下文"""
        self.context.update(context)
        self._update_col_categories()

    # ==================== 便捷访问器 ====================

    def _col_names(self) -> List[str]:
        cols = self.context.get('columns', {})
        if isinstance(cols, dict):
            return [n for n in cols.keys() if n != self.context.get('ts_col', 'ts')]
        return list(cols)

    def _all_col_names(self) -> List[str]:
        cols = self.context.get('columns', {})
        return list(cols.keys()) if isinstance(cols, dict) else ['ts'] + list(cols)

    def _tag_names(self) -> List[str]:
        tags = self.context.get('tags', {})
        return list(tags.keys()) if isinstance(tags, dict) else list(tags)

    def _num_cols(self) -> List[str]:
        return self.context.get('numeric_cols', []) or self._col_names()

    def _str_cols(self) -> List[str]:
        return self.context.get('string_cols', [])

    def _db_table(self) -> str:
        return f"{self.context['db']}.{self.context['table']}"

    def _rc(self) -> str:
        """Random column"""
        cols = self._col_names()
        return self.rng.choice(cols) if cols else 'ts'

    def _rnc(self) -> str:
        """Random numeric column"""
        cols = self._num_cols()
        return self.rng.choice(cols) if cols else self._rc()

    def _rsc(self) -> str:
        """Random string column"""
        cols = self._str_cols()
        return self.rng.choice(cols) if cols else self._rc()

    def _rt(self) -> str:
        """Random tag"""
        tags = self._tag_names()
        return self.rng.choice(tags) if tags else 'tbname'

    def _ri(self, lo=0, hi=1000) -> int:
        return self.rng.randint(lo, hi)

    def _rdur(self) -> str:
        return self.rng.choice(self.DURATIONS)

    def _rsdur(self) -> str:
        return self.rng.choice(self.SHORT_DURATIONS)

    # ==================== 表达式生成 (sql.y expression) ====================

    def _gen_literal(self) -> str:
        """sql.y literal"""
        t = self.rng.choice(['int', 'float', 'string', 'bool', 'null'])
        if t == 'int': return str(self._ri(-100, 1000))
        if t == 'float': return f"{self.rng.uniform(-100, 1000):.4f}"
        if t == 'string': return f"'val_{self._ri(1, 999)}'"
        if t == 'bool': return self.rng.choice(['TRUE', 'FALSE'])
        return 'NULL'

    def _gen_value_for(self, col: str = None) -> str:
        if col and col in self._num_cols():
            return str(self._ri(0, 1000))
        if col and col in self._str_cols():
            return f"'val_{self._ri(1, 100)}'"
        return str(self._ri(0, 1000))

    def _gen_pseudo_column(self) -> str:
        """sql.y pseudo_column (26+ 种 + NK_PH 占位符形式)

        额外覆盖:
          pseudo_column ::= NK_PH NK_INTEGER(B)   →  $1, $2, ...
          pseudo_column ::= NK_PH TBNAME(B)       →  $TBNAME
        """
        r = self.rng.random()
        # NK_PH NK_INTEGER: 占位符列 ($1, $2, ...)
        if r < 0.06:
            return f'${self._ri(1, 5)}'
        # NK_PH TBNAME: 占位符 tbname 函数
        if r < 0.10:
            return '$TBNAME'
        pool = (self.PSEUDO_COLS_BASIC + self.PSEUDO_COLS_WINDOW +
                self.PSEUDO_COLS_QUERY + self.PSEUDO_COLS_INTERP +
                self.PSEUDO_COLS_ADVANCED)
        return self.rng.choice(pool)

    def _gen_noarg_func(self) -> str:
        """sql.y noarg_func / literal_func"""
        func = self.rng.choice(self.NOARG_FUNCS)
        if func in ('NOW', 'TODAY'):
            return func
        return f"{func}()"

    def _gen_agg_call(self, col: str = None) -> str:
        """聚合函数调用"""
        func = self.rng.choice(self.SAFE_AGG_FUNCS)
        if func == 'COUNT' and self.rng.random() < 0.3:
            return 'COUNT(*)'
        return f"{func}({col or self._rnc()})"

    def _gen_scalar_call(self) -> str:
        """标量函数调用"""
        if self.rng.random() < 0.6 or not self._str_cols():
            func = self.rng.choice(self.SCALAR_NUMERIC)
            col = self._rnc()
            if func == 'POW': return f"POW({col}, {self.rng.choice([2, 3])})"
            if func == 'LOG': return f"LOG(ABS({col})+1)"
            if func == 'ROUND': return f"ROUND({col}, {self.rng.choice([0, 1, 2])})"
            if func == 'MOD': return f"MOD({col}, {self.rng.choice([3, 7, 10])})"
            return f"{func}({col})"
        func = self.rng.choice(self.SCALAR_STRING)
        col = self._rsc()
        if func == 'CONCAT': return f"CONCAT({col}, {col})"
        return f"{func}({col})"

    def _gen_cast_expr(self) -> str:
        """sql.y CAST(expr AS type)"""
        return f"CAST({self._rc()} AS {self.rng.choice(self.CAST_TYPES)})"

    def _gen_case_when(self) -> str:
        """sql.y case_when_expression"""
        col = self._rnc()
        val = self._ri(0, 500)
        if self.rng.random() < 0.5:
            return f"CASE WHEN {col} > {val} THEN 1 ELSE 0 END"
        val2 = self._ri(0, val) if val > 0 else 0
        return (f"CASE WHEN {col} > {val} THEN 'high' "
                f"WHEN {col} > {val2} THEN 'medium' ELSE 'low' END")

    def _gen_if_expr(self) -> str:
        """sql.y if_expression: IF/IFNULL/NVL/NVL2/NULLIF/COALESCE"""
        col = self._rc()
        val = self._gen_value_for(col)
        kind = self.rng.choice(['IF', 'IFNULL', 'NVL', 'NVL2', 'NULLIF', 'COALESCE'])
        if kind == 'IF':
            return f"IF({self._rnc()} > {self._ri()}, {val}, 0)"
        if kind in ('IFNULL', 'NVL'):
            return f"{kind}({col}, {val})"
        if kind == 'NVL2':
            return f"NVL2({col}, {val}, 0)"
        if kind == 'NULLIF':
            return f"NULLIF({col}, {val})"
        return f"COALESCE({col}, {self._rc()}, {val})"

    def _gen_trim_expr(self) -> str:
        """sql.y TRIM 的 4 种形式"""
        col = self._rsc() if self._str_cols() else self._rc()
        form = self.rng.choice(['basic', 'spec_from', 'expr_from', 'spec_expr_from'])
        if form == 'basic':
            return f"TRIM({col})"
        if form == 'spec_from':
            spec = self.rng.choice(['BOTH', 'LEADING', 'TRAILING'])
            return f"TRIM({spec} FROM {col})"
        if form == 'expr_from':
            return f"TRIM(' ' FROM {col})"
        spec = self.rng.choice(['BOTH', 'LEADING', 'TRAILING'])
        return f"TRIM({spec} ' ' FROM {col})"

    def _gen_position_expr(self) -> str:
        """sql.y POSITION(expr IN expr)"""
        col = self._rsc() if self._str_cols() else self._rc()
        return f"POSITION('test' IN {col})"

    def _gen_replace_expr(self) -> str:
        """sql.y REPLACE(expr_list)"""
        col = self._rsc() if self._str_cols() else self._rc()
        return f"REPLACE({col}, 'old', 'new')"

    def _gen_substr_expr(self) -> str:
        """sql.y substr_func 的 3 种形式"""
        col = self._rsc() if self._str_cols() else self._rc()
        func = self.rng.choice(['SUBSTR', 'SUBSTRING'])
        form = self.rng.choice(['args', 'from', 'from_for'])
        if form == 'args': return f"{func}({col}, 1, 5)"
        if form == 'from': return f"{func}({col} FROM 1)"
        return f"{func}({col} FROM 1 FOR 5)"

    def _gen_arithmetic(self) -> str:
        """sql.y 算术表达式 (+, -, *, /, %)"""
        c1 = self._rnc()
        op = self.rng.choice(['+', '-', '*', '/'])
        c2 = self.rng.choice([str(self._ri(1, 100)), self._rnc()])
        return f"({c1} {op} {c2})"

    def _gen_bitwise(self) -> str:
        """sql.y 位运算：完整覆盖 NK_BITAND/NK_BITOR/NK_BITXOR/NK_BITNOT/NK_LSHIFT/NK_RSHIFT"""
        kind = self.rng.choice(['and', 'or', 'xor', 'not', 'lshift', 'rshift'])
        c1 = self._rnc()
        val = self._ri(0, 255)
        if kind == 'and':    return f"({c1} & {val})"
        if kind == 'or':     return f"({c1} | {val})"
        if kind == 'xor':    return f"({c1} ^ {val})"
        if kind == 'not':    return f"(~{c1})"
        if kind == 'lshift': return f"({c1} << {self.rng.choice([1, 2, 4, 8])})"
        if kind == 'rshift': return f"({c1} >> {self.rng.choice([1, 2, 4, 8])})"
        return f"({c1} & {val})"

    def _gen_json_arrow(self) -> str:
        """sql.y expression ::= column_reference NK_ARROW NK_STRING (JSON -> 取值)"""
        # JSON 列通常是 tag，用字符串列或普通列
        col = self._rc()
        key = self.rng.choice(['key', 'name', 'type', 'value', 'id', 'code'])
        return f"{col}->'{key}'"

    def _gen_concat_expr(self) -> str:
        """sql.y NK_CONCAT (||): 字符串连接运算符"""
        if self._str_cols():
            c1 = self._rsc()
            c2 = self.rng.choice([f"'{self._ri(1,99)}'", self._rsc()])
        else:
            c1 = f"'{self._ri(1, 99)}'"
            c2 = f"'{self._ri(1, 99)}'"
        return f"({c1} || {c2})"

    def _gen_select_expr(self, allow_agg=True) -> str:
        """生成单个 SELECT 表达式 (覆盖 sql.y expression 所有分支)"""
        kind = self.rng.choices(
            ['column', 'agg_func', 'scalar_func', 'cast', 'case_when',
             'if_expr', 'pseudo', 'noarg_func', 'arithmetic', 'trim',
             'position', 'replace', 'substr', 'rand_func', 'cols_func',
             'star_func', 'bitwise', 'json_arrow', 'concat'],
            weights=[24, 12 if allow_agg else 0, 10, 4, 4,
                     4, 5, 3, 7, 3,
                     2, 2, 2, 2, 2,
                     4 if allow_agg else 0, 3, 2, 2],
            k=1
        )[0]

        if kind == 'column':      return self._rc()
        if kind == 'agg_func':    return self._gen_agg_call()
        if kind == 'scalar_func': return self._gen_scalar_call()
        if kind == 'cast':        return self._gen_cast_expr()
        if kind == 'case_when':   return self._gen_case_when()
        if kind == 'if_expr':     return self._gen_if_expr()
        if kind == 'pseudo':      return self._gen_pseudo_column()
        if kind == 'noarg_func':  return self._gen_noarg_func()
        if kind == 'arithmetic':  return self._gen_arithmetic()
        if kind == 'trim':        return self._gen_trim_expr()
        if kind == 'position':    return self._gen_position_expr()
        if kind == 'replace':     return self._gen_replace_expr()
        if kind == 'substr':      return self._gen_substr_expr()
        if kind == 'rand_func':
            return 'RAND()' if self.rng.random() < 0.5 else f"RAND({self._ri(1, 100)})"
        if kind == 'cols_func':
            # COLS(agg_func, col_expr [AS alias], ...)
            agg = self._gen_agg_call()
            extra = self._rc()
            if self.rng.random() < 0.3:
                extra += f" AS c_{self._ri(1, 9)}"
            return f"COLS({agg}, {extra})"
        if kind == 'star_func':
            func = self.rng.choice(self.STAR_FUNCS)
            r = self.rng.random()
            if r < 0.25:   return f"{func}(*)"
            # star_func_para ::= table_name NK_DOT NK_STAR (sql.y 2270)
            if r < 0.40:
                tbl = self.context['table']
                return f"{func}({tbl}.*)"
            return f"{func}({self._rc()})"
        if kind == 'bitwise':    return self._gen_bitwise()
        if kind == 'json_arrow': return self._gen_json_arrow()
        if kind == 'concat':     return self._gen_concat_expr()
        return self._rc()

    # ==================== SELECT 列表 ====================

    def _gen_select_list(self, allow_agg=True, table_alias: str = None) -> str:
        """sql.y select_list: *, table.*, expr [AS alias]"""
        r = self.rng.random()
        # NK_STAR: SELECT *
        if r < 0.07:
            return '*'
        # table_name NK_DOT NK_STAR: SELECT tbl.*  (sql.y select_item 2542)
        if r < 0.10 and table_alias:
            return f"{table_alias}.*"

        num_items = self.rng.randint(1, 4)
        items = []
        for _ in range(num_items):
            r2 = self.rng.random()
            if r2 < 0.04 and table_alias:
                # table_name.* 作为列表项之一
                expr = f"{table_alias}.*"
            else:
                expr = self._gen_select_expr(allow_agg=allow_agg)
            # 列别名 (sql.y column_alias / AS column_alias)
            if self.rng.random() < 0.15 and expr != '*' and '.*' not in expr:
                alias = f"c_{self._ri(1, 99)}"
                expr = f"{expr} AS {alias}" if self.rng.random() < 0.5 else f"{expr} {alias}"
            items.append(expr)
        return ', '.join(items)

    # ==================== 谓词与条件 (sql.y predicate / boolean_value_expression) ====================

    def _gen_predicate(self, depth=0) -> str:
        """sql.y predicate 所有分支，含 IN(subquery)"""
        if depth > 2:
            return f"{self._rnc()} > {self._ri()}"

        kind = self.rng.choices(
            ['compare', 'between', 'not_between', 'is_null', 'is_not_null',
             'isnull_func', 'isnotnull_func', 'in_op', 'not_in', 'in_subquery',
             'like', 'not_like', 'match', 'nmatch', 'regexp', 'not_regexp',
             'contains'],
            weights=[30, 6, 3, 6, 6,
                     3, 3, 5, 2, 2,
                     6, 3, 3, 3, 2, 1,
                     2],
            k=1
        )[0]

        col = self._rc()
        if kind == 'compare':
            return f"{col} {self.rng.choice(self.COMPARE_OPS)} {self._gen_value_for(col)}"
        if kind == 'between':
            v1, v2 = sorted([self._ri(0, 500), self._ri(0, 1000)])
            return f"{col} BETWEEN {v1} AND {v2}"
        if kind == 'not_between':
            v1, v2 = sorted([self._ri(0, 500), self._ri(0, 1000)])
            return f"{col} NOT BETWEEN {v1} AND {v2}"
        if kind == 'is_null':
            return f"{col} IS NULL"
        if kind == 'is_not_null':
            return f"{col} IS NOT NULL"
        if kind == 'isnull_func':
            return f"ISNULL({col})"
        if kind == 'isnotnull_func':
            return f"ISNOTNULL({col})"
        if kind == 'in_op':
            vals = ', '.join([str(self._ri()) for _ in range(self.rng.randint(2, 5))])
            return f"{col} IN ({vals})"
        if kind == 'not_in':
            vals = ', '.join([str(self._ri()) for _ in range(self.rng.randint(2, 5))])
            return f"{col} NOT IN ({vals})"
        if kind == 'in_subquery':
            # expr IN (SELECT col FROM tbl LIMIT n)  —— sql.y in_predicate_value 子查询
            lim = self.rng.choice([5, 10, 20])
            return (f"{col} IN "
                    f"(SELECT {col} FROM {self._db_table()} LIMIT {lim})")
        if kind == 'like':
            sc = self._rsc() if self._str_cols() else col
            return f"{sc} LIKE '%test%'"
        if kind == 'not_like':
            sc = self._rsc() if self._str_cols() else col
            return f"{sc} NOT LIKE '%bad%'"
        if kind == 'match':
            sc = self._rsc() if self._str_cols() else col
            return f"{sc} MATCH '.*test.*'"
        if kind == 'nmatch':
            sc = self._rsc() if self._str_cols() else col
            return f"{sc} NMATCH '.*bad.*'"
        if kind == 'regexp':
            sc = self._rsc() if self._str_cols() else col
            return f"{sc} REGEXP '.*[0-9]+.*'"
        if kind == 'not_regexp':
            sc = self._rsc() if self._str_cols() else col
            return f"{sc} NOT REGEXP '.*bad.*'"
        if kind == 'contains':
            return f"{col} CONTAINS 'key'"
        return f"{col} > 0"

    def _gen_condition(self, depth=0) -> str:
        """sql.y boolean_value_expression"""
        if depth > 2:
            return self._gen_predicate(depth)

        kind = self.rng.choices(
            ['simple', 'and', 'or', 'not', 'paren'],
            weights=[50, 20, 15, 10, 5],
            k=1
        )[0]

        if kind == 'simple': return self._gen_predicate(depth)
        if kind == 'and':
            return f"{self._gen_predicate(depth+1)} AND {self._gen_predicate(depth+1)}"
        if kind == 'or':
            return f"{self._gen_predicate(depth+1)} OR {self._gen_predicate(depth+1)}"
        if kind == 'not':
            return f"NOT ({self._gen_predicate(depth+1)})"
        if kind == 'paren':
            return f"({self._gen_condition(depth+1)})"
        return self._gen_predicate(depth)

    # ==================== 窗口子句 (sql.y twindow_clause_opt) ====================

    def _gen_window_clause(self) -> str:
        """sql.y twindow_clause_opt 所有窗口类型"""
        kind = self.rng.choices(
            ['interval', 'interval_slide', 'interval_auto',
             'session', 'state_window', 'event_window',
             'count_window', 'anomaly_window'],
            weights=[20, 15, 5, 12, 10, 12, 14, 12],
            k=1
        )[0]

        if kind == 'interval':
            # interval_sliding_duration_literal ::= NK_QUESTION  (参数化占位符, 5% 概率)
            dur = '?' if self.rng.random() < 0.05 else self._rdur()
            return f"INTERVAL({dur}){self._gen_fill_opt()}"
        if kind == 'interval_slide':
            dur = '?' if self.rng.random() < 0.05 else self._rdur()
            sdur = '?' if self.rng.random() < 0.05 else self._rsdur()
            return f"INTERVAL({dur}) SLIDING({sdur}){self._gen_fill_opt()}"
        if kind == 'interval_auto':
            dur = '?' if self.rng.random() < 0.05 else self._rdur()
            return f"INTERVAL({dur}, AUTO){self._gen_fill_opt()}"
        if kind == 'session':
            return f"SESSION(ts, {self._rsdur()})"
        if kind == 'state_window':
            col = self._rnc()
            opt = ''
            if self.rng.random() < 0.3:
                opt = f", {self._ri(1, 10)}"
                if self.rng.random() < 0.3:
                    opt += f", {self._ri(0, 5)}"
            return f"STATE_WINDOW({col}{opt}){self._gen_true_for_opt()}"
        if kind == 'event_window':
            col = self._rnc()
            v1, v2 = self._ri(0, 50), self._ri(50, 100)
            return (f"EVENT_WINDOW START WITH {col} > {v1} "
                    f"END WITH {col} > {v2}{self._gen_true_for_opt()}")
        if kind == 'count_window':
            n = self.rng.choice([5, 10, 20, 50, 100])
            form = self.rng.choices(
                ['simple', 'slide', 'collist', 'slide_collist'],
                weights=[40, 25, 20, 15], k=1)[0]
            if form == 'simple':
                return f"COUNT_WINDOW({n})"
            if form == 'slide':
                return f"COUNT_WINDOW({n}, {self.rng.choice([2, 5, 10])})"
            # 带列名列表的形式：COUNT_WINDOW(n, col_list) / COUNT_WINDOW(n, slide, col_list)
            # sql.y count_window_args 第3/4种形式
            cols = self._col_names()
            col_list = ', '.join(self.rng.sample(cols, min(self.rng.randint(1, 2), len(cols)))) if cols else self._rc()
            if form == 'collist':
                return f"COUNT_WINDOW({n}, {col_list})"
            return f"COUNT_WINDOW({n}, {self.rng.choice([2, 5, 10])}, {col_list})"
        if kind == 'anomaly_window':
            col = self._rnc()
            if self.rng.random() < 0.4:
                return f"ANOMALY_WINDOW({col}, 'algo=iqr')"
            return f"ANOMALY_WINDOW({col})"
        return ''

    def _gen_fill_opt(self) -> str:
        """sql.y fill_opt (NONE/NULL/NULL_F/LINEAR/PREV/NEXT/VALUE/VALUE_F)"""
        if self.rng.random() < 0.6:
            return ''
        mode = self.rng.choice(self.FILL_MODES)
        if mode in ('VALUE', 'VALUE_F'):
            vals = ', '.join([str(self._ri(0, 100)) for _ in range(self.rng.randint(1, 3))])
            return f" FILL({mode}, {vals})"
        return f" FILL({mode})"

    def _gen_interp_fill_opt(self) -> str:
        """sql.y interp_fill_opt 完整覆盖:
           - FILL(mode)
           - FILL(VALUE, expr_list)   / FILL(VALUE_F, expr_list)
           - FILL(fill_position_mode_extension, expr_list)  -- PREV/NEXT/NEAR + 值列表
           - FILL(NONE) / FILL(NULL) / FILL(NULL_F) / FILL(LINEAR)
        """
        if self.rng.random() < 0.4:
            return ''
        mode = self.rng.choice(self.INTERP_FILL_MODES)
        vals = ', '.join([str(self._ri(0, 100)) for _ in range(self.rng.randint(1, 3))])

        # VALUE / VALUE_F: FILL(VALUE, expr_list)
        if mode in ('VALUE', 'VALUE_F'):
            return f"FILL({mode}, {vals})"

        # PREV / NEXT / NEAR 可带可选值列表
        # sql.y: interp_fill_opt ::= FILL(fill_position_mode_extension, expression_list)
        if mode in ('PREV', 'NEXT', 'NEAR'):
            if self.rng.random() < 0.4:
                return f"FILL({mode}, {vals})"
            return f"FILL({mode})"

        # NONE / NULL / NULL_F / LINEAR: 无值列表
        return f"FILL({mode})"

    def _gen_true_for_opt(self) -> str:
        """sql.y true_for_opt (duration/COUNT/AND/OR 4 种形式 + NK_QUESTION 占位符)"""
        if self.rng.random() < 0.7:
            return ''
        # interval_sliding_duration_literal ::= NK_QUESTION  (5% 概率用 ? 占位符)
        dur = '?' if self.rng.random() < 0.05 else self._rsdur()
        cnt = self.rng.choice([3, 5, 10])
        kind = self.rng.choice(['dur', 'cnt', 'and', 'or'])
        if kind == 'dur':  return f" TRUE_FOR({dur})"
        if kind == 'cnt':  return f" TRUE_FOR(COUNT {cnt})"
        if kind == 'and':  return f" TRUE_FOR({dur} AND COUNT {cnt})"
        return f" TRUE_FOR({dur} OR COUNT {cnt})"

    # ==================== 主生成入口 ====================

    def generate(self, start_symbol: str = 'query_specification') -> str:
        self.current_depth = 0

        if self.mode == 'full_grammar':
            sql = self._expand(start_symbol)
            sql = re.sub(r'\s+', ' ', sql).strip()
            sql = re.sub(r'\s*,\s*', ', ', sql)
            sql = re.sub(r'\s*\(\s*', '(', sql)
            sql = re.sub(r'\s*\)\s*', ')', sql)
            return sql

        if start_symbol == 'query_specification':
            r = self.rng.random()
            if r < 0.08:
                return self._generate_union_query()
            elif r < 0.22:
                return self._generate_join_query()
            elif r < 0.30:
                return self._generate_interp_query()
            elif r < 0.38:
                return self._generate_subquery_select()
            else:
                return self._generate_simple_select()

        sql = self._expand(start_symbol)
        sql = re.sub(r'\s+', ' ', sql).strip()
        return sql

    # ==================== SELECT 生成 (sql.y query_specification 完整覆盖) ====================

    def _generate_simple_select(self) -> str:
        """
        全面覆盖 sql.y query_specification + query_expression:
        SELECT hint_list set_quantifier_opt tag_mode_opt select_list
        from_clause_opt where_clause_opt partition_by_clause_opt
        range_opt every_opt interp_fill_opt
        twindow_clause_opt group_by_clause_opt having_clause_opt
        order_by_clause_opt slimit_clause_opt limit_clause_opt
        """
        parts = ['SELECT']

        # hint_list (sql.y NK_HINT)
        if self.rng.random() < 0.05:
            parts.append(self.rng.choice([
                '/*+ scan_order(1) */', '/*+ para_tables_sort() */',
                '/*+ partition_first() */',
            ]))

        # set_quantifier_opt (sql.y DISTINCT / ALL)
        if self.rng.random() < 0.12:
            parts.append(self.rng.choice(['DISTINCT', 'ALL']))

        # tag_mode_opt (sql.y TAGS)
        if self.rng.random() < 0.05:
            parts.append('TAGS')

        # 决定是否为聚合查询
        is_agg = self.rng.random() < 0.4
        has_window = False

        # select_list
        parts.append(self._gen_select_list(allow_agg=is_agg))

        # from_clause_opt
        if self.rng.random() < 0.95:
            r_from = self.rng.random()
            if r_from < 0.03:
                # table_primary ::= NK_PH TBNAME alias_opt  (分区占位符表)
                alias = f" AS t_{self._ri(1, 9)}" if self.rng.random() < 0.5 else ''
                parts.append(f"FROM $TBNAME{alias}")
            elif r_from < 0.05:
                # table_primary ::= NK_PH TROWS alias_opt  (分区行数占位符表)
                alias = f" AS t_{self._ri(1, 9)}" if self.rng.random() < 0.5 else ''
                parts.append(f"FROM $TROWS{alias}")
            elif r_from < 0.10:
                # table_reference_list ::= list NK_COMMA table_reference  (逗号隐式 INNER JOIN)
                # sql.y: createJoinTableNode(JOIN_TYPE_INNER, JOIN_STYPE_NONE, ...)
                t1 = self._db_table()
                t2 = self._db_table()
                a1 = f" a{self._ri(1, 5)}"
                a2 = f" b{self._ri(1, 5)}"
                parts.append(f"FROM {t1}{a1}, {t2}{a2}")
            else:
                parts.append(f"FROM {self._db_table()}")

        # where_clause_opt
        if self.rng.random() < 0.5:
            parts.append(f"WHERE {self._gen_condition()}")

        # partition_by_clause_opt (含 alias / 多列)
        # sql.y: partition_item ::= expr / expr column_alias / expr AS column_alias
        has_partition = False
        if self.rng.random() < 0.2:
            has_partition = True
            r_part = self.rng.random()
            if r_part < 0.25:
                # 多列 PARTITION BY: partition_list ::= partition_list, partition_item
                cols = self.rng.sample(
                    [self._rt(), 'tbname'] + self._tag_names()[:3],
                    min(2, len(self._tag_names()) + 1) if self._tag_names() else 2
                )
                items = []
                for c in cols:
                    if self.rng.random() < 0.30:
                        # partition_item ::= expr AS column_alias
                        items.append(f"{c} AS p_{self._ri(1, 9)}")
                    elif self.rng.random() < 0.20:
                        # partition_item ::= expr column_alias  (无 AS)
                        items.append(f"{c} p_{self._ri(1, 9)}")
                    else:
                        items.append(c)
                parts.append(f"PARTITION BY {', '.join(items)}")
            else:
                pcol = self.rng.choice([self._rt(), 'tbname'])
                if self.rng.random() < 0.30:
                    # partition_item ::= expr AS column_alias
                    parts.append(f"PARTITION BY {pcol} AS p_{self._ri(1, 10)}")
                elif self.rng.random() < 0.20:
                    # partition_item ::= expr column_alias  (无 AS)
                    parts.append(f"PARTITION BY {pcol} p_{self._ri(1, 10)}")
                else:
                    parts.append(f"PARTITION BY {pcol}")

        # twindow_clause_opt (需要聚合函数)
        if is_agg and self.rng.random() < 0.25:
            has_window = True
            parts.append(self._gen_window_clause())

        # group_by_clause_opt (与窗口互斥)
        has_group_by = False
        if not has_window and self.rng.random() < 0.25:
            has_group_by = True
            parts.append(f"GROUP BY {self._rt()}")

        # having_clause_opt
        if has_group_by and self.rng.random() < 0.3:
            func = self.rng.choice(self.SAFE_AGG_FUNCS)
            op = self.rng.choice(self.COMPARE_OPS)
            parts.append(f"HAVING {func}({self._rnc()}) {op} {self._ri()}")

        # order_by_clause_opt + null_ordering_opt
        if self.rng.random() < 0.3:
            oc = self.rng.choice([self._rc(), 'ts'])
            od = self.rng.choice(['ASC', 'DESC'])
            order = f"ORDER BY {oc} {od}"
            if self.rng.random() < 0.15:
                order += self.rng.choice([' NULLS FIRST', ' NULLS LAST'])
            parts.append(order)

        # slimit_clause_opt (配合 PARTITION BY)
        # sql.y: SLIMIT n / SLIMIT n SOFFSET m / SLIMIT m,n (m=offset, n=limit)
        if has_partition and self.rng.random() < 0.25:
            sl = self.rng.choice([5, 10, 20])
            form = self.rng.choices(['simple', 'soffset', 'comma'], weights=[50, 30, 20], k=1)[0]
            if form == 'soffset':
                parts.append(f"SLIMIT {sl} SOFFSET {self.rng.choice([0, 1, 5])}")
            elif form == 'comma':
                # SLIMIT offset,limit  —— sql.y: createLimitNode(pCxt, B=limit, C=offset)
                parts.append(f"SLIMIT {self.rng.choice([0, 1, 5])},{sl}")
            else:
                parts.append(f"SLIMIT {sl}")

        # limit_clause_opt (3 种形式: LIMIT n / LIMIT n OFFSET m / LIMIT offset,limit)
        if self.rng.random() < 0.4:
            lim = self.rng.choice([10, 50, 100, 1000])
            off = self.rng.choice([0, 5, 10])
            form = self.rng.choices(['simple', 'offset', 'comma'], weights=[60, 25, 15], k=1)[0]
            if form == 'offset':
                parts.append(f"LIMIT {lim} OFFSET {off}")
            elif form == 'comma':
                # LIMIT offset,limit  —— sql.y: createLimitNode(pCxt, B=limit, C=offset)
                parts.append(f"LIMIT {off},{lim}")
            else:
                parts.append(f"LIMIT {lim}")

        return ' '.join(parts)

    # ==================== JOIN 查询 (sql.y joined_table 所有类型) ====================

    def _generate_join_query(self) -> str:
        """覆盖 sql.y 全部 JOIN 类型: INNER/OUTER/SEMI/ANTI/ASOF/WINDOW"""
        parts = ['SELECT']

        if self.rng.random() < 0.15:
            parts.append('a.*, b.*')
        else:
            cols = []
            for _ in range(self.rng.randint(1, 3)):
                alias = self.rng.choice(['a', 'b'])
                cols.append(f"{alias}.{self.rng.choice([self._rc(), 'ts'])}")
            parts.append(', '.join(cols))

        parts.append(f"FROM {self._db_table()} a")

        jt = self.rng.choice(self.JOIN_TYPES)
        parts.append(jt)
        parts.append(f"{self._db_table()} b")

        is_asof = 'ASOF' in jt
        is_window = 'WINDOW' in jt

        # ON clause
        if is_asof:
            parts.append(f"ON a.ts {self.rng.choice(['>=', '<='])} b.ts")
        else:
            jcol = self.rng.choice(['ts'] + self._tag_names()[:2])
            parts.append(f"ON a.{jcol} = b.{jcol}")
            if self.rng.random() < 0.2:
                parts.append(f"AND a.{self._rnc()} > {self._ri(0, 50)}")

        # window_offset_clause (WINDOW JOIN 必需)
        if is_window:
            parts.append(f"WINDOW_OFFSET(-{self._rsdur()}, {self._rsdur()})")

        # jlimit_clause_opt
        if (is_asof or is_window) and self.rng.random() < 0.4:
            parts.append(f"JLIMIT {self.rng.choice([1, 3, 5, 10])}")

        # WHERE
        if self.rng.random() < 0.3:
            parts.append(f"WHERE a.{self._rc()} > {self._ri()}")

        # ORDER BY + null_ordering_opt
        if self.rng.random() < 0.2:
            od = f"ORDER BY a.ts {self.rng.choice(['ASC', 'DESC'])}"
            if self.rng.random() < 0.2:
                od += self.rng.choice([' NULLS FIRST', ' NULLS LAST'])
            parts.append(od)

        # LIMIT
        if self.rng.random() < 0.4:
            parts.append(f"LIMIT {self.rng.choice([10, 50, 100])}")

        return ' '.join(parts)

    # ==================== UNION / 集合操作查询 (sql.y union_query_expression + %left UNION MINUS EXCEPT INTERSECT) ====================

    def _generate_union_query(self) -> str:
        """sql.y union_query_expression: UNION / UNION ALL / MINUS / EXCEPT / INTERSECT"""
        q1 = self._generate_simple_select()
        q2 = self._generate_simple_select()
        # sql.y 优先级声明: %left UNION ALL MINUS EXCEPT INTERSECT
        op = self.rng.choices(
            ['UNION ALL', 'UNION', 'MINUS', 'EXCEPT', 'INTERSECT'],
            weights=[35, 35, 10, 10, 10], k=1)[0]
        sql = f"{q1} {op} {q2}"
        # query_expression 可附加 ORDER BY / LIMIT
        if self.rng.random() < 0.3:
            sql += f" ORDER BY {self._rc()} {self.rng.choice(['ASC', 'DESC'])}"
        if self.rng.random() < 0.3:
            sql += f" LIMIT {self.rng.choice([10, 50, 100])}"
        return sql

    # ==================== INTERP 查询 (sql.y range_opt / every_opt / interp_fill_opt) ====================

    def _generate_interp_query(self) -> str:
        """INTERP 查询: 覆盖 range_opt 3 种形式 + every_opt + interp_fill_opt"""
        parts = ['SELECT']

        cols = [self._rnc() for _ in range(self.rng.randint(1, 2))]
        parts.append(', '.join(f"INTERP({c})" for c in cols))

        parts.append(f"FROM {self._db_table()}")

        # range_opt (sql.y 3 种形式)
        rf = self.rng.choice(['two', 'three', 'one'])
        if rf == 'two':
            parts.append("RANGE('2024-01-01 00:00:00', '2024-01-01 01:00:00')")
        elif rf == 'three':
            parts.append("RANGE('2024-01-01 00:00:00', '2024-01-01 01:00:00', 1d)")
        else:
            parts.append("RANGE('2024-01-01 00:00:00')")

        # every_opt
        if self.rng.random() < 0.8:
            parts.append(f"EVERY({self._rdur()})")

        # interp_fill_opt
        fill = self._gen_interp_fill_opt()
        if fill:
            parts.append(fill)

        if self.rng.random() < 0.3:
            parts.append(f"LIMIT {self.rng.choice([10, 50, 100])}")

        return ' '.join(parts)

    # ==================== 子查询 (sql.y subquery / table_primary) ====================

    def _generate_subquery_select(self) -> str:
        """子查询: FROM 子查询 + WHERE 子查询"""
        kind = self.rng.choice(['from_sub', 'where_sub'])

        if kind == 'from_sub':
            inner = self._generate_simple_select()
            outer_expr = self.rng.choice(['*', self._rc()])
            sql = f"SELECT {outer_expr} FROM ({inner}) AS sub"
            if self.rng.random() < 0.3:
                sql += f" WHERE sub.{self._rc()} > {self._ri()}"
            if self.rng.random() < 0.3:
                sql += f" LIMIT {self.rng.choice([10, 50, 100])}"
            return sql
        else:
            col = self._rc()
            sql = (f"SELECT {self._gen_select_list()} FROM {self._db_table()} "
                   f"WHERE {col} IN (SELECT {col} FROM {self._db_table()} LIMIT 10)")
            if self.rng.random() < 0.3:
                sql += f" LIMIT {self.rng.choice([10, 50, 100])}"
            return sql

    # ==================== 完全语法驱动模式 (full_grammar) ====================

    def _expand(self, symbol: str) -> str:
        if self.current_depth > self.max_depth:
            return ''
        if symbol in self.grammar.terminals or symbol.isupper():
            return self._terminal_to_sql(symbol)
        if symbol not in self.grammar.productions:
            return ''
        productions = self.grammar.productions[symbol]
        prod = self._choose_production(productions, symbol)
        self.current_depth += 1
        parts = []
        for rhs_symbol in prod.rhs:
            if rhs_symbol:
                part = self._expand(rhs_symbol)
                if part:
                    parts.append(part)
        self.current_depth -= 1
        return ' '.join(parts)

    def _choose_production(self, productions: List[Production], symbol: str = '') -> Production:
        if symbol in self.required_rules:
            non_empty = [p for p in productions if p.complexity > 0]
            if non_empty:
                productions = non_empty
        if self.current_depth >= self.max_depth - 1:
            simple = [p for p in productions if p.complexity <= 2]
            if simple:
                return self.rng.choice(simple)
            return min(productions, key=lambda p: p.complexity)
        weights = [1.0 / (p.complexity + 1) for p in productions]
        return self.rng.choices(productions, weights=weights)[0]

    def _terminal_to_sql(self, terminal: str) -> str:
        """将终结符转换为 SQL 字符串 (全面覆盖 sql.y 所有终结符)"""
        keywords = {
            'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'LIMIT', 'OFFSET',
            'INTERVAL', 'SESSION', 'FILL', 'PARTITION', 'UNION', 'JOIN',
            'LEFT', 'RIGHT', 'INNER', 'OUTER', 'FULL', 'SEMI', 'ANTI', 'ASOF',
            'DISTINCT', 'HAVING', 'SLIMIT', 'SOFFSET', 'SLIDING', 'WINDOW',
            'STATE_WINDOW', 'EVENT_WINDOW', 'COUNT_WINDOW', 'ANOMALY_WINDOW',
            'RANGE', 'EVERY', 'TRUE_FOR', 'START', 'WITH', 'AUTO',
            'NULL', 'PREV', 'NEXT', 'LINEAR', 'VALUE', 'VALUE_F', 'NULL_F', 'NEAR', 'NONE',
            'BETWEEN', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS',
            'MATCH', 'NMATCH', 'REGEXP', 'CONTAINS',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST', 'AS',
            'IF', 'IFNULL', 'NVL', 'NVL2', 'NULLIF', 'COALESCE',
            'TRIM', 'BOTH', 'LEADING', 'TRAILING', 'POSITION', 'REPLACE',
            'SUBSTR', 'SUBSTRING', 'FOR',
            'EXISTS', 'ALL', 'ASC', 'DESC', 'ON', 'USING',
            'TBNAME', 'QTAGS', 'JLIMIT', 'TAGS', 'NULLS', 'FIRST', 'LAST',
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'LAST_ROW',
            'COLS', 'RAND',
            'ROWTS', 'IROWTS', 'WSTART', 'WEND', 'WDURATION',
            'QSTART', 'QEND', 'QDURATION', 'ISFILLED',
            'FLOW', 'FHIGH', 'FROWTS', 'IROWTS_ORIGIN',
            'TPREV_TS', 'TCURRENT_TS', 'TNEXT_TS',
            'TWSTART', 'TWEND', 'TWDURATION', 'TWROWNUM',
            'TPREV_LOCALTIME', 'TNEXT_LOCALTIME', 'TLOCALTIME', 'TGRPID',
            'IMPROWTS', 'IMPMARK', 'ANOMALYMARK',
            'NOW', 'TODAY', 'TIMEZONE', 'DATABASE', 'CLIENT_VERSION',
            'SERVER_VERSION', 'SERVER_STATUS', 'CURRENT_USER', 'USER', 'PI',
            'WINDOW_OFFSET', 'ISNULL', 'ISNOTNULL',
            # 集合操作 (sql.y %left UNION ALL MINUS EXCEPT INTERSECT)
            'MINUS', 'EXCEPT', 'INTERSECT',
            # 其他子句关键字
            'TROWS', 'OUTPUT_SUBTABLE', 'INTERP',
        }
        if terminal in keywords:
            return terminal

        symbol_map = {
            'NK_LP': '(', 'NK_RP': ')', 'NK_COMMA': ',', 'NK_DOT': '.',
            'NK_SEMI': ';', 'NK_STAR': '*',
            'NK_PLUS': '+', 'NK_MINUS': '-', 'NK_SLASH': '/', 'NK_REM': '%',
            'NK_EQ': '=', 'NK_NE': '!=', 'NK_LT': '<', 'NK_LE': '<=',
            'NK_GT': '>', 'NK_GE': '>=',
            'NK_CONCAT': '||',           # 字符串拼接
            'NK_BITNOT': '~',            # 按位取反
            'NK_BITAND': '&',
            'NK_BITOR': '|',
            'NK_BITXOR': '^',            # 按位异或
            'NK_LSHIFT': '<<',           # 左移
            'NK_RSHIFT': '>>',           # 右移
            'NK_ARROW': '->',            # JSON 取值
            'NK_COLON': ':',
            'NK_QUESTION': '?',
            'NK_PH': '$',               # 占位符前缀
        }
        if terminal in symbol_map:
            return symbol_map[terminal]

        if terminal in ('NK_INTEGER', 'unsigned_integer'):
            return str(self._ri(1, 100))
        if terminal in ('NK_STRING', 'literal_string'):
            return f"'test_{self._ri(1, 100)}'"
        if terminal == 'NK_FLOAT':
            return f"{self.rng.uniform(0, 100):.2f}"
        if terminal == 'NK_BOOL':
            return self.rng.choice(['TRUE', 'FALSE'])
        if terminal == 'NK_HINT':
            return ''
        if terminal == 'NK_VARIABLE':
            return self._rdur()
        if terminal == 'NK_ALIAS':
            return f"`alias_{self._ri(1, 10)}`"
        if terminal == 'NK_ID':
            return f"col_{self._ri(1, 10)}"
        if terminal in ('NK_BIN', 'NK_HEX'):
            return str(self._ri(0, 255))

        if terminal in ('table_name', 'table_reference', 'full_table_name'):
            return f"{self.context['db']}.{self.context['table']}"
        if terminal in ('column_name', 'column_reference', 'expression_name'):
            return self._rc()
        if terminal == 'db_name':
            return self.context['db']
        if terminal in ('column_alias', 'table_alias', 'table_alias_name'):
            return f"a_{self._ri(1, 10)}"
        if terminal == 'function_name':
            return self.rng.choice(self.SAFE_AGG_FUNCS + self.SCALAR_NUMERIC[:5])
        if terminal in ('duration_literal', 'interval_sliding_duration_literal',
                        'window_offset_literal'):
            return self._rdur()
        if terminal == 'timestamp':
            return "'2024-01-01 00:00:00'"
        if terminal == 'search_condition':
            return self._gen_condition()
        if terminal == 'literal':
            return self._gen_literal()

        return ''


# ==================== 工厂函数 ====================

_generator_instance = None
_generator_mode = None


def get_generator(sql_y_path: str = None, seed=None, mode='hybrid') -> GrammarBasedSQLGenerator:
    """
    获取生成器实例（单例）

    Args:
        sql_y_path: sql.y 文件路径
        seed: 随机种子
        mode: 生成模式 ('hybrid' 或 'full_grammar')
    """
    global _generator_instance, _generator_mode
    if _generator_instance is None or _generator_mode != mode:
        if sql_y_path is None:
            sql_y_path = '/root/TDinternal/community/source/libs/parser/inc/sql.y'
        _generator_instance = GrammarBasedSQLGenerator(sql_y_path, seed, mode)
        _generator_mode = mode
    return _generator_instance


if __name__ == '__main__':
    gen = get_generator()

    print("=" * 80)
    print("基于语法树的 SQL 生成器测试 (全面覆盖 sql.y)")
    print("=" * 80)

    for i in range(10):
        sql = gen.generate('query_specification')
        print(f"\n{i+1}. {sql}")

    print("\n" + "=" * 80)
    print("✓ 生成器工作正常")

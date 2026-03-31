"""Provide runtime schema information to the query generator."""

from typing import List, Tuple, Optional
from .types import TdDataType
from .misc import Dice


class SchemaProvider:
    """Supplies table/column/tag names and type metadata.
    
    Constructed from crash_gen's FULL_SCHEMA_COLS / FULL_SCHEMA_TAGS
    and the live super table + reg tables state.
    """

    def __init__(self, db_name: str, super_table_name: str,
                 reg_table_names: List[str],
                 cols: List[Tuple[str, TdDataType]],
                 tags: List[Tuple[str, TdDataType]]):
        self._db = db_name
        self._st_name = super_table_name
        self._reg_tables = reg_table_names
        self._cols = cols
        self._tags = tags

        # Pre-compute grouped lists
        self._numeric_cols = [c[0] for c in cols if c[1].is_numeric()]
        self._string_cols = [c[0] for c in cols if c[1].is_string()]
        self._all_col_names = [c[0] for c in cols]
        self._all_tag_names = [t[0] for t in tags]
        self._ts_col = cols[0][0] if cols else "ts"

        # Name → type lookup
        self._col_type_map = {c[0]: c[1] for c in cols}
        self._col_type_map.update({t[0]: t[1] for t in tags})

    def get_db(self) -> str:
        return self._db

    def rand_table(self) -> str:
        """Random fully qualified table name (db.table)"""
        if not self._reg_tables or Dice.throw(3) == 0:
            return f"{self._db}.{self._st_name}"
        return f"{self._db}.{Dice.choice(self._reg_tables)}"

    def rand_table_name_only(self) -> str:
        """Random table name without db prefix (for JOIN etc.)"""
        if not self._reg_tables or Dice.throw(3) == 0:
            return self._st_name
        return Dice.choice(self._reg_tables)

    def super_table_full(self) -> str:
        return f"{self._db}.{self._st_name}"

    def rand_column(self, need_numeric=False, need_string=False) -> str:
        """Random column name, optionally filtered by type"""
        if need_numeric and self._numeric_cols:
            return Dice.choice(self._numeric_cols)
        if need_string and self._string_cols:
            return Dice.choice(self._string_cols)
        return Dice.choice(self._all_col_names)

    def rand_tag(self) -> str:
        return Dice.choice(self._all_tag_names) if self._all_tag_names else "b"

    def rand_numeric_col(self) -> str:
        return Dice.choice(self._numeric_cols) if self._numeric_cols else "speed"

    def rand_string_col(self) -> str:
        return Dice.choice(self._string_cols) if self._string_cols else "color"

    def get_ts_col(self) -> str:
        return self._ts_col

    def col_type(self, name: str) -> Optional[TdDataType]:
        return self._col_type_map.get(name)

    def all_col_names(self) -> List[str]:
        return list(self._all_col_names)

    def all_tag_names(self) -> List[str]:
        return list(self._all_tag_names)
import datetime
import re
import json

from dataclasses import dataclass, field
from typing import List, Any, Tuple

from util.log import tdLog
from util.sql import tdSql
from util.cases import tdCases
from util.dnodes import tdDnodes
from util.constant import *
from util.common import is_json

PRIMARY_COL = "ts"

INT_COL = "c_int"
BINT_COL = "c_bint"
SINT_COL = "c_sint"
TINT_COL = "c_tint"
FLOAT_COL = "c_float"
DOUBLE_COL = "c_double"
BOOL_COL = "c_bool"
TINT_UN_COL = "c_utint"
SINT_UN_COL = "c_usint"
BINT_UN_COL = "c_ubint"
INT_UN_COL = "c_uint"
BINARY_COL = "c_binary"
NCHAR_COL = "c_nchar"
TS_COL = "c_ts"

NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, TINT_UN_COL, SINT_UN_COL, BINT_UN_COL, INT_UN_COL]
CHAR_COL = [BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [BOOL_COL, ]
TS_TYPE_COL = [TS_COL, ]

INT_TAG = "t_int"

ALL_COL = [PRIMARY_COL, INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BINARY_COL, NCHAR_COL, BOOL_COL, TS_COL]
TAG_COL = [INT_TAG]

# insert data args：
TIME_STEP = 10000
NOW = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
STBNAME = "stb1"
CTBNAME = "ct1"
NTBNAME = "nt1"


@dataclass
class DataSet:
    ts_data     : List[int]     = field(default_factory=list)
    int_data    : List[int]     = field(default_factory=list)
    bint_data   : List[int]     = field(default_factory=list)
    sint_data   : List[int]     = field(default_factory=list)
    tint_data   : List[int]     = field(default_factory=list)
    int_un_data : List[int]     = field(default_factory=list)
    bint_un_data: List[int]     = field(default_factory=list)
    sint_un_data: List[int]     = field(default_factory=list)
    tint_un_data: List[int]     = field(default_factory=list)
    float_data  : List[float]   = field(default_factory=list)
    double_data : List[float]   = field(default_factory=list)
    bool_data   : List[int]     = field(default_factory=list)
    binary_data : List[str]     = field(default_factory=list)
    nchar_data  : List[str]     = field(default_factory=list)


@dataclass
class Hsgschema:
    func_type           : str           = "SELECT"
    from_clause         : str           = STBNAME
    where_clause        : str           = None
    group_clause        : str           = None
    having_clause       : str           = None
    partition_clause    : str           = None

    histogram_flag      : str           = "HISTOGRAM"
    col                 : str           = None
    real_col            : Any           = None
    bin_type            : str           = None
    bin_desc            : Any           = None
    normalized          : int           = 0
    other               : dict          = None

    user_input          : str           = None
    linear_bin          : str           = None
    log_bin             : str           = None

    liner_width         : float         = None

    bin_start           : float         = None
    bin_count           : int           = None
    bin_infinity        : bool          = None

    def __post_init__(self):
        if isinstance(self.other, dict):
            for k,v in self.other.items():
                if k.lower().strip() == "func_type" and isinstance(v, str) and not self.func_type:
                    self.func_type = v
                    del self.other[k]

                if k.lower().strip() == "from_clause" and isinstance(v, str) and not self.from_clause:
                    self.from_clause = v
                    del self.other[k]

                if k.lower().strip() == "where_clause" and isinstance(v, str) and not self.where_clause:
                    self.where_clause = v
                    del self.other[k]

                if k.lower().strip() == "group_clause" and isinstance(v, str) and not self.group_clause:
                    self.group_clause = v
                    del self.other[k]

                if k.lower().strip() == "having_clause" and isinstance(v, str) and not self.having_clause:
                    self.having_clause = v
                    del self.other[k]

                if k.lower().strip() == "partition_clause" and isinstance(v, str) and not self.partition_clause:
                    self.partition_clause = v
                    del self.other[k]

                if k.lower().strip() == "histogram_flag" and isinstance(v, str) and not self.histogram_flag:
                    self.histogram_flag = v
                    del self.other[k]

                if k.lower().strip() == "col" and isinstance(v, str) and not self.col:
                    self.col = v
                    del self.other[k]

                if k.lower().strip() == "bin_type" and isinstance(v, str)  and not self.bin_type:
                    self.bin_type = v
                    del self.other[k]

                if k.lower().strip() == "user_input" and isinstance(v, str) and not self.user_input and self.bin_type.lower().strip() == "user_input":
                    self.user_input = v
                    del self.other[k]

                if k.lower().strip() == "linear_bin" and isinstance(v, str) and not self.linear_bin and self.bin_type.lower().strip() == "linear_bin":
                    self.linear_bin = v
                    del self.other[k]

                if k.lower().strip() == "log_bin" and isinstance(v, str) and not self.log_bin and self.bin_type.lower().strip()  == "log_bin":
                    self.log_bin = v
                    del self.other[k]

                if k.lower().strip() == "normalized" and isinstance(v, int) and not self.normalized:
                    self.normalized = v
                    del self.other[k]

        if isinstance(self.bin_type,str) and self.bin_type.upper().strip() == "USER_INPUT":
            self.bin_desc = self.user_input
        elif isinstance(self.bin_type,str) and self.bin_type.upper().strip() == "LINEAR_BIN":
            self.bin_desc = self.linear_bin
        elif isinstance(self.bin_type,str) and self.bin_type.upper().strip() == "LOG_BIN":
            self.bin_desc = self.log_bin

# from ...pytest.util.sql import *
# from ...pytest.util.constant import *

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)
        self.precision = "ms"
        self.sma_count = 0
        self.sma_created_index = []

    def __create_hsg(self, sma:Hsgschema):
        return  f"""{sma.histogram_flag}({sma.col}, '{sma.bin_type}', '{sma.bin_desc}', {sma.normalized})"""

    def __gen_sql(self, sma:Hsgschema):
        sql = f"{sma.func_type} {self.__create_hsg(sma)} from {sma.from_clause} "
        if sma.where_clause:
            sql += f" where {sma.where_clause}"
        if sma.partition_clause:
            sql += f" partition by {sma.partition_clause}"
        if sma.group_clause:
            sql += f" group by {sma.group_clause}"
            if sma.having_clause:
                sql += f" having {sma.having_clause}"
        return sql

    def __gen_no_hsg_sql(self, sma:Hsgschema):
        return f"{sma.func_type} {sma.col} from {sma.from_clause}"

    def __hsg_check(self, sma:Hsgschema):
        if not sma.histogram_flag:
            return False
        if not sma.col or (not isinstance(sma.col, str) and not isinstance(sma.col, int) and not isinstance(sma.col, float)):
            return False
        if tdSql.is_err_sql(self.__gen_no_hsg_sql(sma)):
            return False
        if any ([not sma.bin_type,  not isinstance(sma.bin_type, str) ]):
            return False
        if all([sma.bin_type.upper().strip() != "USER_INPUT", sma.bin_type.upper().strip() != "LINEAR_BIN" , sma.bin_type.upper().strip() != "LOG_BIN"]):
            return False
        if not sma.bin_desc:
            return False
        if sma.normalized  is None or not isinstance(sma.normalized, int) or (sma.normalized != 0 and sma.normalized != 1):
            return False
        if sma.bin_type.upper().strip() == "USER_INPUT":
            # user_raw = eval(sma.bin_desc) if isinstance(sma.bin_desc, str) else sma.bin_desc
            if not is_json(sma.bin_desc) and not isinstance(sma.bin_desc, list) and not isinstance(sma.bin_desc, set):
                return False
            user_raw = json.loads(sma.bin_desc) if is_json(sma.bin_desc) else sma.bin_desc
            if not isinstance(user_raw, list):
                return False
            if len(user_raw) >= 2:
                for i in range(len(user_raw)-1):
                    if user_raw[i] >= user_raw[ i+1 ]:
                        return False
                    if not isinstance(user_raw[i], int) and not isinstance(user_raw[i], float):
                        return False
                if not isinstance(user_raw[-1], int) and not isinstance(user_raw[-1], float):
                    return False
            else:
                if not isinstance(user_raw[-1], int) and not isinstance(user_raw[-1], float):
                    return False
            sma.bin_count = len(user_raw) - 1

        if sma.bin_type.upper().strip() == "LINEAR_BIN":
            if not is_json(sma.bin_desc):
                return False
            user_raw = json.loads(sma.bin_desc)
            if not isinstance(user_raw, dict):
                return False
            if any([len(user_raw.keys()) != 4, "start" not in user_raw.keys(), "width" not in user_raw.keys(), "count" not in user_raw.keys(), "infinity" not in user_raw.keys()]):
                return False
            if not isinstance(user_raw["start"], int) and not isinstance(user_raw["start"], float):
                return False
            if not isinstance(user_raw["width"], int) and not isinstance(user_raw["width"], float) or user_raw["width"] == 0 :
                return False
            if not isinstance(user_raw["count"], int) and not isinstance(user_raw["count"], float) or user_raw["count"] <= 0:
                return False
            if not isinstance(user_raw["infinity"], bool) :
                return False
            sma.bin_infinity = user_raw["infinity"]
            sma.bin_count = int(user_raw["count"]) + 2 if user_raw["infinity"]  else int(user_raw["count"])

        if sma.bin_type.upper().strip() == "LOG_BIN":
            if not is_json(sma.bin_desc):
                return False
            user_raw = json.loads(sma.bin_desc)
            if not isinstance(user_raw, dict):
                return False
            if any([ len(user_raw.keys()) != 4, "start" not in user_raw.keys(), "factor" not in user_raw.keys(), "count" not in user_raw.keys(), "infinity" not in user_raw.keys()]):
                return False
            if not isinstance(user_raw["start"], int) and not isinstance(user_raw["start"], float) or user_raw["start"] == 0:
                return False
            if not isinstance(user_raw["factor"], int) and not isinstance(user_raw["factor"], float) or user_raw["factor"] <= 0 :
                return False
            if not isinstance(user_raw["count"], int) and not isinstance(user_raw["count"], float)  or user_raw["count"] <= 0:
                return False
            if not isinstance(user_raw["infinity"], bool) :
                return False
            sma.bin_infinity = user_raw["infinity"]
            sma.bin_count = int(user_raw["count"]) + 2 if user_raw["infinity"]  else int(user_raw["count"])

        invalid_func = AGG_FUNC
        invalid_func.extend(SELECT_FUNC)
        invalid_func.extend(TS_FUNC)
        for func in invalid_func:
            if sma.where_clause and func in sma.where_clause.upper().strip():
                return False
            if sma.group_clause and func in sma.group_clause.upper().strip():
                return False
            if sma.partition_clause and func in sma.partition_clause.upper().strip():
                return False
            if isinstance(sma.col, str) and func in sma.col.upper().strip():
                return False

        tdSql.execute(self.__gen_no_hsg_sql(sma))
        if tdSql.cursor.istype(0, "BINARY") or tdSql.cursor.istype(0, "NCHAR") or tdSql.cursor.istype(0, "BOOL") or tdSql.cursor.istype(0, "TIMESTAMP"):
            return False

        return True

    def hsg_check(self, sma:Hsgschema):
        if self.__hsg_check(sma):
            tdSql.query(self.__gen_sql(sma))
            tdSql.checkRows(sma.bin_count)
            sum_rate = 0
            if sma.normalized and (not sma.bin_infinity or sma.bin_type.upper().strip() == "USER_INPUT"):
                for i in range(tdSql.queryRows):
                    row_data = json.loads(tdSql.queryResult[i][0])
                    sum_rate += row_data["count"]
                if sum_rate != 0 and (sum_rate-1) > 0.00001:
                    tdLog.exit(f"summary of result count should be 0 or 1, now summary is {sum_rate} !!!")
                else:
                    tdLog.success(f"summary of result count is {sum_rate}!")

        else:
            tdSql.error(self.__gen_sql(sma))

    @property
    def __hsg_querysql(self):
        err_sqls = []
        cur_sqls = []
        # err_set
        ### case 1.1: required fields check
        err_sqls.append( Hsgschema( histogram_flag="", bin_type="USER_INPUT", user_input="[0,3,6,9]" ) )
        err_sqls.append( Hsgschema( col="", bin_type="USER_INPUT", user_input="[0,3,6,9]" ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="", bin_desc="[0,3,6,9]", normalized=0 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="", normalized=0 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[0,3,6,9]", normalized="" ) )

        ## case 1.2: format check
        err_sqls.append( Hsgschema(col=(INT_COL, BINT_COL), bin_type="USER_INPUT", user_input="[0,3,6,9]" ) )
        err_sqls.append( Hsgschema(col={"col": INT_COL}, bin_type="USER_INPUT", user_input="[0,3,6,9]" ) )
        err_sqls.append( Hsgschema( col=(INT_UN_COL, INT_COL), bin_type="USER_INPUT", user_input="[0,3,6,9]" ) )
        err_sqls.append( Hsgschema( col=f"sum({INT_UN_COL}, {INT_COL})", bin_type="USER_INPUT", user_input="[0,3,6,9]" ) )
        err_sqls.append( Hsgschema(col=INT_COL, bin_type="USER_INPUT_1", user_input="[0,3,6,9]" ) )
        err_sqls.append( Hsgschema(col=INT_COL, bin_type=("USER_INPUT",), bin_desc="[0,3,6,9]" ) )
        err_sqls.append( Hsgschema(col=INT_COL, bin_type="USER_INPUT", user_input="0,3,6,9" ) )
        err_sqls.append( Hsgschema(col=INT_COL, bin_type="USER_INPUT", user_input={0,3,6,9} ) )
        err_sqls.append( Hsgschema(col=INT_COL, bin_type="USER_INPUT", user_input=(0,3,6,9) ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[0,3,6,9]", normalized=1.5 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[0,3,6,9]", normalized="null" ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input='{"start": -200, "width": 100, "count": 20, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input={"start": -200, "width": 100, "count": 20, "infinity": True}, normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[3,0,10,6,9]", normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[0,3,6,9,'a']", normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="['a']", normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin=['{"start": 1, "width": 3, "count": 10, "infinity": false}'], normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='[{"start": 1, "width": 3, "count": 10, "infinity": false}]', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"begin": 1, "width": 3, "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "length": 3, "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 3, "num": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 3, "count": 10, "withnull": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 3, "count": 10, "infinity": false, "other": 1}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": null, "width": 3, "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": "a", "width": 3, "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": "a", "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": null, "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 0, "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 1, "count": 0, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 1, "count": -10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 1, "count": 10, "infinity": "false"}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 1, "count": 10, "infinity": null}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin=['{"start": 1, "factor": 4, "count": 4, "infinity": true}'], normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='[{"start": 1, "factor": 4, "count": 4, "infinity": true}]', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"begin": 1, "factor": 4, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "step": 4, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "num": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 4, "witgnull": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 4, "infinity": true, "other": 2}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": null, "factor": 4, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": "a", "factor": 4, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 0, "factor": 4, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": "a", "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": null, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 0, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": -10, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 0, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": -10, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": "true"}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": null}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": false}', where_clause=f"count({INT_COL}) >= 0 ") )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": false}', group_clause=f"min({INT_COL}) ") )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": false}', partition_clause=f"CSUM({INT_COL}) ") )
        err_sqls.append( Hsgschema( col=f"TWA({INT_COL})", bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": false}') )
        err_sqls.append( Hsgschema( col=BINARY_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": false}') )
        err_sqls.append( Hsgschema( col=NCHAR_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": false}') )
        err_sqls.append( Hsgschema( col=TS_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": false}') )
        err_sqls.append( Hsgschema( col=BOOL_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10, "infinity": false}') )


        ### case 2:

        # current_set
        for num_col in NUM_COL:
            cur_sqls.append( Hsgschema( col=num_col, bin_type="USER_INPUT", user_input="[0,3,6,9,11]", normalized=0) )
            cur_sqls.append( Hsgschema( col=num_col, bin_type="USER_INPUT", user_input="[0,3,6,9,11]", normalized=1) )
            cur_sqls.append( Hsgschema( col=num_col, bin_type="linear_bin", linear_bin='{"start": -200, "width": 100, "count": 20, "infinity": false}', normalized=1) )
            cur_sqls.append( Hsgschema( col=num_col, bin_type="linear_bin", linear_bin='{"start": -200, "width": 100, "count": 20, "infinity": true}', normalized=1) )
            cur_sqls.append( Hsgschema( col=num_col, bin_type="linear_bin", linear_bin='{"start": -200, "width": 100, "count": 20, "infinity": false}', normalized=0) )
            cur_sqls.append( Hsgschema( col=num_col, bin_type="linear_bin", linear_bin='{"start": -200, "width": 100, "count": 20, "infinity": true}', normalized=0 ) )
            cur_sqls.append( Hsgschema( col=num_col, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 6, "infinity": false}', normalized=1 ) )
            cur_sqls.append( Hsgschema( col=num_col, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 6, "infinity": true}', normalized=1 ) )
            cur_sqls.append( Hsgschema( col=num_col, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 6, "infinity": false}', normalized=0 ) )
            cur_sqls.append( Hsgschema( col=num_col, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 6, "infinity": true}', normalized=0 ) )

        cur_sqls.append( Hsgschema( col=INT_UN_COL, bin_type="linear_bin", linear_bin='{"start": -200, "width": 100, "count": 20, "infinity": false}', normalized=1) )
        cur_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[0,3,6,9]", normalized=0 ) )
        cur_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input=[0,3,6,9] ) )
        cur_sqls.append( Hsgschema( col=1, bin_type="USER_INPUT", user_input="[0,3,6,9]" ) )
        cur_sqls.append( Hsgschema( col=BINT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 3, "count": 10, "infinity": false}', normalized=0 ) )
        cur_sqls.append( Hsgschema( col=FLOAT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 4, "infinity": true}', normalized=0 ) )
        cur_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 1, "count": 1.5, "infinity": false}', normalized=1 ) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="USER_INPUT", user_input="[0,3,6,9,11]", normalized=0) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="USER_INPUT", user_input="[0,3,6,9,11]", normalized=1) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="linear_bin", linear_bin='{"start": -200, "width": 100, "count": 20, "infinity": false}', normalized=1) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="linear_bin", linear_bin='{"start": -200, "width": 100, "count": 20, "infinity": true}', normalized=1) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="linear_bin", linear_bin='{"start": -200, "width": 100, "count": 20, "infinity": false}', normalized=0) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="linear_bin", linear_bin='{"start": -200, "width": 100, "count": 20, "infinity": true}', normalized=0 ) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 6, "infinity": false}', normalized=1 ) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 6, "infinity": true}', normalized=1 ) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 6, "infinity": false}', normalized=0 ) )
        cur_sqls.append( Hsgschema( col=INT_TAG, bin_type="log_bin", log_bin='{"start": 1, "factor": 4, "count": 6, "infinity": true}', normalized=0 ) )


        return err_sqls, cur_sqls

    def test_histogram(self,ctb_num=20):
        err_sqls , cur_sqls = self.__hsg_querysql
        for err_sql in err_sqls:
            self.hsg_check(err_sql)
        for cur_sql in cur_sqls:
            self.hsg_check(cur_sql)

        tdSql.query("SELECT HISTOGRAM(c_int, 'USER_INPUT', '[0,3,6,9]', 0) from stb1 where c_int < 10 ")
        tdSql.checkData(0, 0, f'{{"lower_bin":0, "upper_bin":3, "count":{ ( ctb_num - 2 ) * 3 }}}')
        tdSql.checkData(1, 0, f'{{"lower_bin":3, "upper_bin":6, "count":{ ( ctb_num - 2 ) * 3 }}}')
        tdSql.checkData(2, 0, f'{{"lower_bin":6, "upper_bin":9, "count":{ ( ctb_num - 2 ) * 3 }}}')

        tdSql.query("SELECT HISTOGRAM(c_int, 'USER_INPUT', '[0,3,6,9]', 0) from ct1 where c_int < 10")
        tdSql.checkData(0, 0, '{"lower_bin":0, "upper_bin":3, "count":3}')
        tdSql.checkData(1, 0, '{"lower_bin":3, "upper_bin":6, "count":3}')
        tdSql.checkData(2, 0, '{"lower_bin":6, "upper_bin":9, "count":3}')

        tdSql.query("SELECT HISTOGRAM(c_int, 'USER_INPUT', '[0,3,6,9]', 0) from nt1 where c_int < 10")
        tdSql.checkData(0, 0, '{"lower_bin":0, "upper_bin":3, "count":3}')
        tdSql.checkData(1, 0, '{"lower_bin":3, "upper_bin":6, "count":3}')
        tdSql.checkData(2, 0, '{"lower_bin":6, "upper_bin":9, "count":3}')

    def all_test(self):
        self.test_histogram()

    def __create_tb(self, stb=STBNAME, ctb_num=20, ntbnum=1):
        tdLog.printNoPrefix("==========step: create table")
        create_stb_sql = f'''create table {stb}(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags ({INT_TAG} int)
            '''
        for i in range(ntbnum):

            create_ntb_sql = f'''create table nt{i+1}(
                    ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                    {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                    {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                    {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                    {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                )
                '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(ctb_num):
            tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')

    def __data_set(self, rows):
        data_set = DataSet()

        for i in range(rows):
            data_set.ts_data.append(NOW + 1 * (rows - i))
            data_set.int_data.append(rows - i)
            data_set.bint_data.append(11111 * (rows - i))
            data_set.sint_data.append(111 * (rows - i) % 32767)
            data_set.tint_data.append(11 * (rows - i) % 127)
            data_set.int_un_data.append(rows - i)
            data_set.bint_un_data.append(11111 * (rows - i))
            data_set.sint_un_data.append(111 * (rows - i) % 32767)
            data_set.tint_un_data.append(11 * (rows - i) % 127)
            data_set.float_data.append(1.11 * (rows - i))
            data_set.double_data.append(1100.0011 * (rows - i))
            data_set.bool_data.append((rows - i) % 2)
            data_set.binary_data.append(f'binary{(rows - i)}')
            data_set.nchar_data.append(f'nchar_测试_{(rows - i)}')

        return data_set

    def __insert_data(self, ctbnum=20):
        tdLog.printNoPrefix("==========step: start inser data into tables now.....")
        data = self.__data_set(rows=self.rows)

        # now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        null_data = '''null, null, null, null, null, null, null, null, null, null, null, null, null, null'''
        zero_data = "0, 0, 0, 0, 0, 0, 0, 'binary_0', 'nchar_0', 0, 0, 0, 0, 0"

        for i in range(self.rows):
            row_data = f'''
                {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                {data.bool_data[i]}, '{data.binary_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {data.tint_un_data[i]},
                {data.sint_un_data[i]}, {data.int_un_data[i]}, {data.bint_un_data[i]}
            '''
            neg_row_data = f'''
                {-1 * data.int_data[i]}, {-1 * data.bint_data[i]}, {-1 * data.sint_data[i]}, {-1 * data.tint_data[i]}, {-1 * data.float_data[i]}, {-1 * data.double_data[i]},
                {data.bool_data[i]}, '{data.binary_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {1 * data.tint_un_data[i]},
                {1 * data.sint_un_data[i]}, {1 * data.int_un_data[i]}, {1 * data.bint_un_data[i]}
            '''

            tdSql.execute(
                f"insert into ct1 values ( {NOW - i * TIME_STEP}, {row_data} )")
            tdSql.execute(
                f"insert into ct2 values ( {NOW - i * int(TIME_STEP * 0.6)}, {neg_row_data} )")
            tdSql.execute(
                f"insert into nt1 values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )")

            for j in range(ctbnum-3):
                tdSql.execute(
                f"insert into ct{j+4} values ( {NOW - i * int(TIME_STEP * 0.8) }, {row_data} )")

        tdSql.execute(
            f"insert into ct2 values ( {NOW + int(TIME_STEP * 0.6)}, {null_data} )")
        tdSql.execute(
            f"insert into ct2 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.6)}, {null_data} )")
        tdSql.execute(
            f"insert into ct2 values ( {NOW - self.rows * int(TIME_STEP * 0.29) }, {null_data} )")

        tdSql.execute(
            f"insert into ct4 values ( {NOW + int(TIME_STEP * 0.8)}, {null_data} )")
        tdSql.execute(
            f"insert into ct4 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.8)}, {null_data} )")
        tdSql.execute(
            f"insert into ct4 values ( {NOW - self.rows * int(TIME_STEP * 0.39)}, {null_data} )")

        tdSql.execute(
            f"insert into {NTBNAME} values ( {NOW + int(TIME_STEP * 1.2)}, {null_data} )")
        tdSql.execute(
            f"insert into {NTBNAME} values ( {NOW - (self.rows + 1) * int(TIME_STEP * 1.2)}, {null_data} )")
        tdSql.execute(
            f"insert into {NTBNAME} values ( {NOW - self.rows * int(TIME_STEP * 0.59)}, {null_data} )")

    def run(self):
        self.rows = 10

        tdLog.printNoPrefix("==========step0:all check")

        tdLog.printNoPrefix("==========step1:create table in normal database")
        tdSql.prepare()
        self.__create_tb()
        self.__insert_data()
        self.all_test()

        tdLog.printNoPrefix("==========step2:create table in normal database")
        tdSql.execute("create database db1 vgroups 2")
        tdSql.execute("use db1")
        self.__create_tb()
        self.__insert_data()
        self.all_test()

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdLog.printNoPrefix("==========step3:after wal, all check again ")
        tdSql.execute("use db")
        self.all_test()
        tdSql.execute("use db1")
        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())

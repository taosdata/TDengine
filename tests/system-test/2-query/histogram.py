from datetime import datetime
import re
import json

from dataclasses import dataclass
from typing import Any

from util.log import tdLog
from util.sql import tdSql
from util.cases import tdCases
from util.dnodes import tdDnodes
from util.constant import *
from util.common import *

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
NOW = int(datetime.timestamp(datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
STBNAME = "stb1"
CTBNAME = "ct1"
NTBNAME = "nt1"
PARAINF = float("inf")


@dataclass
class Hsgschema:

    func_type           : str           = "SELECT"
    from_clause         : str           = f"{STBNAME}"
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


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def __create_hsg(self, sma:Hsgschema):
        return  f"""{sma.histogram_flag}({sma.col}, '{sma.bin_type}', '{sma.bin_desc}', {sma.normalized})"""

    def __gen_sql(self, sma:Hsgschema, dbname=DBNAME):
        sql = f"{sma.func_type} {self.__create_hsg(sma)} from {dbname}.{sma.from_clause} "
        if sma.where_clause:
            sql += f" where {sma.where_clause}"
        if sma.partition_clause:
            sql += f" partition by {sma.partition_clause}"
        if sma.group_clause:
            sql += f" group by {sma.group_clause}"
            if sma.having_clause:
                sql += f" having {sma.having_clause}"
        return sql

    def __gen_no_hsg_sql(self, sma:Hsgschema, dbname=DBNAME):
        return f"{sma.func_type} {sma.col} from {dbname}.{sma.from_clause}"

    def __hsg_check(self, sma:Hsgschema, dbname=DBNAME):
        if not sma.histogram_flag:
            return False
        if not sma.col or (not isinstance(sma.col, str) and not isinstance(sma.col, int) and not isinstance(sma.col, float)):
            return False
        if tdSql.is_err_sql(self.__gen_no_hsg_sql(sma, dbname)):
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

        tdSql.execute(self.__gen_no_hsg_sql(sma, dbname))
        if tdSql.cursor.istype(0, "BINARY") or tdSql.cursor.istype(0, "NCHAR") or tdSql.cursor.istype(0, "BOOL") or tdSql.cursor.istype(0, "TIMESTAMP"):
            return False

        return True

    def hsg_check(self, sma:Hsgschema, dbname=DBNAME):
        if self.__hsg_check(sma):
            tdSql.query(self.__gen_sql(sma, dbname))
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

    def hsg_check_error(self, sma:Hsgschema, dbname=DBNAME):
            tdSql.error(self.__gen_sql(sma, dbname))

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
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[0,3,6,9]", normalized="", from_clause=NTBNAME ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[0,3,6,9]", normalized="", from_clause=CTBNAME ) )

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
        # err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": 2, "count": 10, "infinity": false}', normalized=1 , "123" ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 1, "width": float("inf"), "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin",  linear_bin='{"start": float("inf"), "width": 10, "count": 10, "infinity": false}', normalized=1 ) )
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
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": -10, "count": 4,  "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 0, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 0, "count": 4, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": -10, "width": NULL, "count": 4, "infinity": true}', normalized=1 ) )

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
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": 10001, "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": "123", "infinity": true}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": 10, "count": float("inf"), "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": float("inf"), "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": float("inf"), "factor": 10, "count": 10, "infinity": false}', normalized=1 ) )
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": float("-inf"), "factor": 10, "count": 10, "infinity": false}', normalized=1 ) )

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


        # add testcase by chr
        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[]", normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="USER_INPUT", user_input="[1,'listStr',2]", normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": %f, "width": 10000000, "count": 10000000, "infinity": false}'%PARAINF, normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 2000, "width": %f, "count": 10, "infinity": false}'%PARAINF, normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 10, "width": 10, "count": %f, "infinity": false}'%PARAINF, normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor": %f, "count": 10, "infinity": false}'%PARAINF, normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": %f, "width": %f, "count": %f, "infinity": false}'%(PARAINF,PARAINF,PARAINF), normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 10, "width": 10, "count": 0, "infinity": false}', normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 10, "width": 10, "count": -10, "infinity": false}', normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 10, "width": 10, "count": 1001, "infinity": false}', normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="linear_bin", linear_bin='{"start": 10, "width": 10, "count": 1001, "infinity": false , "linerBinNumber":5}', normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor":-100, "count": 10, "infinity": false}', normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor":0, "count": 10, "infinity": false}', normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor":1, "count": 10, "infinity": false}', normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": %f, "factor":10, "count": 10, "infinity": false}'%PARAINF, normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor":10, "count": %f, "infinity": false}'%PARAINF, normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor":10, "count": %f, "infinity": false}'%PARAINF, normalized=1 ) )

        err_sqls.append( Hsgschema( col=INT_COL, bin_type="log_bin", log_bin='{"start": 1, "factor":10, "count": %f, "infinity": false, "logBinNumber":5}', normalized=1 ) )

        err_sqls.append( Hsgschema(col={"errorColType": INT_COL}, bin_type="USER_INPUT", user_input="[0,3,6,9]", normalized=1) )

        # err_sqls.append( Hsgschema(col=INT_COL, bin_type=, user_input="[0,3,6,9]", normalized=1) )
        # err_sqls.append( Hsgschema(col=INT_COL, bin_type="USER_INPUT", user_input="[0,3,6,9]", normalized=1) )
        # err_sqls.append( Hsgschema(col=INT_COL, bin_type="USER_INPUT", user_input="[0,3,6,9]", normalized=1) )



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

    def test_histogram(self, dbname=DBNAME, ctb_num :int=20):
        err_sqls , cur_sqls = self.__hsg_querysql
        for err_sql in err_sqls:
            self.hsg_check_error(err_sql, dbname)
        for cur_sql in cur_sqls:
            self.hsg_check(cur_sql, dbname)

        tdSql.query(f"SELECT HISTOGRAM(c_int, 'USER_INPUT', '[0,3,6,9]', 0) from {dbname}.stb1 where c_int < 10 ")
        tdSql.checkData(0, 0, f'{{"lower_bin":0, "upper_bin":3, "count":{ ( ctb_num - 2 ) * 3 }}}')
        tdSql.checkData(1, 0, f'{{"lower_bin":3, "upper_bin":6, "count":{ ( ctb_num - 2 ) * 3 }}}')
        tdSql.checkData(2, 0, f'{{"lower_bin":6, "upper_bin":9, "count":{ ( ctb_num - 2 ) * 3 + 1}}}')

        tdSql.query(f"SELECT HISTOGRAM(c_int, 'USER_INPUT', '[0,3,6,9]', 0) from {dbname}.ct1 where c_int < 10")
        tdSql.checkData(0, 0, '{"lower_bin":0, "upper_bin":3, "count":0}')
        tdSql.checkData(1, 0, '{"lower_bin":3, "upper_bin":6, "count":0}')
        tdSql.checkData(2, 0, '{"lower_bin":6, "upper_bin":9, "count":1}')

        tdSql.query(f"SELECT HISTOGRAM(c_int, 'USER_INPUT', '[0,3,6,9]', 0) from {dbname}.nt1 where c_int < 10")
        tdSql.checkData(0, 0, '{"lower_bin":0, "upper_bin":3, "count":3}')
        tdSql.checkData(1, 0, '{"lower_bin":3, "upper_bin":6, "count":3}')
        tdSql.checkData(2, 0, '{"lower_bin":6, "upper_bin":9, "count":3}')
        
        #error sql 
        # tdSql.error(f"SELECT HISTOGRAM(c_int, 'linear_bin', '{"start": -200, "width": 100, "count": 20, "infinity": false}', 0 ,1 ) from {dbname}.nt1 where c_int < 10")

        # tdSql.error(f"SELECT HISTOGRAM(c_int, 'linear_bin', '{"start": -200, "width": 100, "count": -10, "infinity": false}', 0 ) from {dbname}.nt1 where c_int < 10")

        # tdSql.error(f"SELECT HISTOGRAM(c_int, 'linear_bin', '{"start": -200, "width": 100, "count": 1001, "infinity": false}', 0 ) from {dbname}.nt1 where c_int < 10")

        tdSql.error(f"SELECT HISTOGRAM(c_int, 'log_bin', '[0,3,6,9]', 0 ,1 ) from {dbname}.nt1 where c_int < 10")

        tdSql.error(f"SELECT HISTOGRAM(c_int, 'USER_INPUT', '[0,3,6,9]', 0 ,1 ) from {dbname}.nt1 where c_int < 10")
        tdSql.error(f"SELECT HISTOGRAM(c_int, 'USER_INPUT', '[0,3,6,9]' ) from {dbname}.nt1 where c_int < 10")
        tdSql.error(f"SELECT HISTOGRAM('123', 'USER_INPUT', '[0,3,6,9]', 0 ) from {dbname}.nt1 where c_int < 10")
        tdSql.error(f"SELECT HISTOGRAM('123', 123 ,123 ,123 ) from {dbname}.nt1 where c_int < 10")
        tdSql.error(f"SELECT HISTOGRAM(123, '123' ,'123' ,'123' ) from {dbname}.nt1 where c_int < 10")

        #  if (4 != numOfParams) 
        # tdSql.error("select HISTOGRAM(c_int, \"linear_bin\", \"{\"start\": -200, \"width\": 100, \"count\": 20, \"infinity\": false}\", 1 , InvalidNumber) from db.stb1 where c_int < 10")
        tdSql.error('SELECT HISTOGRAM(c_int, "linear_bin", \'{"start": -200, "width": 100, "count": 20, "infinity": false}\', 0 ,InvalidNumber ) from %s.stb1 '%dbname)
        tdSql.error('SELECT HISTOGRAM(c_int, "linear_bin", \'{"start": -200, "width": 100, "count": 20, "infinity": false}\' ) from %s.stb1 '%dbname)
        tdSql.error('SELECT HISTOGRAM(c_int, 54321, "[0,3,6,9]", 1 ) from %s.stb1 '%dbname) 
        tdSql.error('SELECT HISTOGRAM(c_int, "USER_INPUT", 54321, 0  ) from %s.stb1'%dbname) 
        tdSql.error('SELECT HISTOGRAM(c_int, "USER_INPUT", "[0,3,6,9]", InvalidNumber ) from %s.stb1'%dbname) 
        tdSql.error('SELECT HISTOGRAM(c_int, "USER_INPUT", "[0,3,6,9]", -100 ) from %s.stb1'%dbname) 
        tdSql.error('SELECT HISTOGRAM(c_int, c_int, "[0,3,6,9]", 1 ) from %s.stb1'%dbname) 
        tdSql.error('SELECT HISTOGRAM(c_int, "USER_INPUT", c_int, 1 ) from %s.stb1'%dbname) 
        tdSql.error('SELECT HISTOGRAM(c_int, "USER_INPUT", "[0,3,6,9]", c_int ) from %s.stb1'%dbname) 
        tdSql.query('SELECT HISTOGRAM(123, "USER_INPUT", "[0,3,6,9]", 0 ) from %s.stb1'%dbname) 
        tdSql.error('SELECT HISTOGRAM(c_binary, "USER_INPUT", "[0,3,6,9]", 0 ) from %s.stb1'%dbname) 
        tdSql.error('SELECT HISTOGRAM("c_binary", "USER_INPUT", "[0,3,6,9]", 0 ) from %s.stb1'%dbname) 
        tdSql.query('SELECT HISTOGRAM(123, "linear_bin", \'{"start": 1, "width": 10, "count": 20, "infinity": false}\',0 ) from %s.stb1 '%dbname)
        tdSql.error('SELECT HISTOGRAM(c_binary, "linear_bin", \'{"start": 1, "width": 10, "count": 20, "infinity": false}\',0 ) from %s.stb1 '%dbname)
        tdSql.error('SELECT HISTOGRAM("c_binary", "linear_bin", \'{"start": 1, "width": 10, "count": 20, "infinity": false}\',0 ) from %s.stb1 '%dbname)

        tdSql.error('SELECT HISTOGRAM(c_int, "c_int", \'{"start": 1, "width": 10, "count": 20, "infinity": false}\',0 ) from %s.stb1 '%dbname)
        tdSql.error('SELECT HISTOGRAM(c_int, "linear_bin", c_int,0 ) from %s.stb1 '%dbname)
        tdSql.error('SELECT HISTOGRAM(c_int, "linear_bin", \'{"start": 1, "width": 10, "count": 20, "infinity": false}\',c_int ) from %s.stb1 '%dbname)


    def all_test(self, dbname=DBNAME):
        self.test_histogram(dbname)

    def __create_tb(self, stb=STBNAME, ctb_num=20, ntbnum=1, dbname=DBNAME):
        tdLog.printNoPrefix("==========step: create table")
        create_stb_sql = f'''create table {dbname}.{stb}(
                {PRIMARY_COL} timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags ({INT_TAG} int)
            '''
        tdSql.execute(create_stb_sql)

        for i in range(ntbnum):
            create_ntb_sql = f'''create table {dbname}.nt{i+1}(
                    {PRIMARY_COL} timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                    {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                    {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                    {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                    {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                )
                '''
            tdSql.execute(create_ntb_sql)

        for i in range(ctb_num):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.{stb} tags ( {i+1} )')

    def __insert_data(self, rows, ctb_num=20, dbname=DBNAME, star_time=NOW):
        tdLog.printNoPrefix("==========step: start inser data into tables now.....")
        # from ...pytest.util.common import DataSet
        data = DataSet()
        data.get_order_set(rows, bint_step=2)

        null_data = '''null, null, null, null, null, null, null, null, null, null, null, null, null, null'''
        # zero_data = "0, 0, 0, 0, 0, 0, 0, 'binary_0', 'nchar_0', 0, 0, 0, 0, 0"s

        for i in range(rows):
            row_data = f'''
                {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                {data.bool_data[i]}, '{data.vchar_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {data.utint_data[i]},
                {data.usint_data[i]}, {data.uint_data[i]}, {data.ubint_data[i]}
            '''
            neg_row_data = f'''
                {-1 * data.int_data[i]}, {-1 * data.bint_data[i]}, {-1 * data.sint_data[i]}, {-1 * data.tint_data[i]}, {-1 * data.float_data[i]}, {-1 * data.double_data[i]},
                {data.bool_data[i]}, '{data.vchar_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {1 * data.utint_data[i]},
                {1 * data.usint_data[i]}, {1 * data.uint_data[i]}, {1 * data.ubint_data[i]}
            '''

            for j in range(ctb_num):
                if j == 2:
                    tdSql.execute( f"insert into {dbname}.ct{j+1} values ( {star_time - j * i * TIME_STEP}, {neg_row_data} )" )
                else:
                    tdSql.execute( f"insert into {dbname}.ct{j+1} values ( {star_time - j * i * TIME_STEP}, {row_data} )" )

            tdSql.execute(
                f"insert into {dbname}.nt1 values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )")

        tdSql.execute(
            f"insert into {dbname}.ct2 values ( {NOW + int(TIME_STEP * 0.6)}, {null_data} )")
        tdSql.execute(
            f"insert into {dbname}.ct2 values ( {NOW - int(TIME_STEP * 0.6 * rows * ctb_num)}, {null_data} )")
        tdSql.execute(
            f"insert into {dbname}.ct2 values ( {NOW - int(TIME_STEP * 0.29 * rows * ctb_num) }, {null_data} )")

        tdSql.execute(
            f"insert into {dbname}.ct4 values ( {NOW + int(TIME_STEP * 0.8)}, {null_data} )")
        tdSql.execute(
            f"insert into {dbname}.ct4 values ( {NOW - int(TIME_STEP * 0.8 * rows * ctb_num)}, {null_data} )")
        tdSql.execute(
            f"insert into {dbname}.ct4 values ( {NOW - int(TIME_STEP * 0.39 * rows * ctb_num)}, {null_data} )")

        tdSql.execute(
            f"insert into {dbname}.{NTBNAME} values ( {NOW + int(TIME_STEP * 1.2)}, {null_data} )")
        tdSql.execute(
            f"insert into {dbname}.{NTBNAME} values ( {NOW - (self.rows + 1) * int(TIME_STEP * 1.2)}, {null_data} )")
        tdSql.execute(
            f"insert into {dbname}.{NTBNAME} values ( {NOW - self.rows * int(TIME_STEP * 0.59)}, {null_data} )")

    def run(self):
        self.rows = 10

        tdLog.printNoPrefix("==========step0:all check")

        tdLog.printNoPrefix("==========step1:create table in normal database")
        tdSql.prepare()
        self.__create_tb()
        self.__insert_data(self.rows)
        self.all_test()

        tdLog.printNoPrefix("==========step2:create table in normal database db1")
        tdSql.prepare(dbname="db1", **{"vgroups":2})
        self.__create_tb(dbname="db1")
        self.__insert_data(self.rows, dbname="db1")
        self.all_test(dbname="db1")

        tdSql.execute("flush database db")
        tdSql.execute("flush database db1")


        tdLog.printNoPrefix("==========step3:after wal, all check again ")
        self.all_test(dbname="db")
        self.all_test(dbname="db1")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())

/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "planTestUtil.h"
#include "planner.h"

using namespace std;

class PlanStmtTest : public PlannerTestBase {
 public:
  void buildParam(TAOS_MULTI_BIND* pBindParams, int32_t index, void* pVal, int32_t type, int32_t bytes = 0) {
    TAOS_MULTI_BIND* pBindParam = pBindParams + index;
    pBindParam->buffer_type = type;
    pBindParam->num = 1;
    pBindParam->buffer_length = bytes > 0 ? bytes : tDataTypes[type].bytes;
    pBindParam->buffer = taosMemoryCalloc(1, pBindParam->buffer_length);
    pBindParam->length = (int32_t*)taosMemoryCalloc(1, sizeof(int32_t));
    pBindParam->is_null = (char*)taosMemoryCalloc(1, sizeof(char));
    *(pBindParam->length) = bytes > 0 ? bytes : tDataTypes[type].bytes;
    *(pBindParam->is_null) = 0;

    switch (type) {
      case TSDB_DATA_TYPE_BOOL:
        *((bool*)pBindParam->buffer) = *(bool*)pVal;
        break;
      case TSDB_DATA_TYPE_TINYINT:
        *((int8_t*)pBindParam->buffer) = *(int64_t*)pVal;
        break;
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_FLOAT:
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_JSON:
      case TSDB_DATA_TYPE_VARBINARY:
      case TSDB_DATA_TYPE_DECIMAL:
      case TSDB_DATA_TYPE_BLOB:
      case TSDB_DATA_TYPE_MEDIUMBLOB:
      default:
        break;
    }
  }
};

TEST_F(PlanStmtTest, stmt) {
  useDb("root", "test");

  prepare("SELECT * FROM t1 WHERE c1 = ?");
}

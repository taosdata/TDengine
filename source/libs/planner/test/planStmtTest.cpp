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
  TAOS_MULTI_BIND* createBindParams(int32_t nParams) {
    return (TAOS_MULTI_BIND*)taosMemoryCalloc(nParams, sizeof(TAOS_MULTI_BIND));
  }

  void destoryBindParams(TAOS_MULTI_BIND* pParams, int32_t nParams) {
    for (int32_t i = 0; i < nParams; ++i) {
      TAOS_MULTI_BIND* pParam = pParams + i;
      taosMemoryFree(pParam->buffer);
      taosMemoryFree(pParam->length);
      taosMemoryFree(pParam->is_null);
    }
    taosMemoryFree(pParams);
  }

  TAOS_MULTI_BIND* buildIntegerParam(TAOS_MULTI_BIND* pBindParams, int32_t index, int64_t val, int32_t type) {
    TAOS_MULTI_BIND* pBindParam = initParam(pBindParams, index, type, 0);

    switch (type) {
      case TSDB_DATA_TYPE_BOOL:
        *((bool*)pBindParam->buffer) = val;
        break;
      case TSDB_DATA_TYPE_TINYINT:
        *((int8_t*)pBindParam->buffer) = val;
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        *((int16_t*)pBindParam->buffer) = val;
        break;
      case TSDB_DATA_TYPE_INT:
        *((int32_t*)pBindParam->buffer) = val;
        break;
      case TSDB_DATA_TYPE_BIGINT:
        *((int64_t*)pBindParam->buffer) = val;
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
        *((int64_t*)pBindParam->buffer) = val;
        break;
      default:
        break;
    }

    return pBindParam;
  }

  TAOS_MULTI_BIND* buildUIntegerParam(TAOS_MULTI_BIND* pBindParams, int32_t index, uint64_t val, int32_t type) {
    TAOS_MULTI_BIND* pBindParam = initParam(pBindParams, index, type, 0);

    switch (type) {
      case TSDB_DATA_TYPE_UTINYINT:
        *((uint8_t*)pBindParam->buffer) = val;
        break;
      case TSDB_DATA_TYPE_USMALLINT:
        *((uint16_t*)pBindParam->buffer) = val;
        break;
      case TSDB_DATA_TYPE_UINT:
        *((uint32_t*)pBindParam->buffer) = val;
        break;
      case TSDB_DATA_TYPE_UBIGINT:
        *((uint64_t*)pBindParam->buffer) = val;
        break;
      default:
        break;
    }
    return pBindParam;
  }

  TAOS_MULTI_BIND* buildDoubleParam(TAOS_MULTI_BIND* pBindParams, int32_t index, double val, int32_t type) {
    TAOS_MULTI_BIND* pBindParam = initParam(pBindParams, index, type, 0);

    switch (type) {
      case TSDB_DATA_TYPE_FLOAT:
        *((float*)pBindParam->buffer) = val;
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        *((double*)pBindParam->buffer) = val;
        break;
      default:
        break;
    }
    return pBindParam;
  }

  TAOS_MULTI_BIND* buildStringParam(TAOS_MULTI_BIND* pBindParams, int32_t index, const char* pVal, int32_t type,
                                    int32_t bytes) {
    TAOS_MULTI_BIND* pBindParam = initParam(pBindParams, index, type, bytes);

    switch (type) {
      case TSDB_DATA_TYPE_VARCHAR:
      case TSDB_DATA_TYPE_VARBINARY:
      case TSDB_DATA_TYPE_GEOMETRY:
        strncpy((char*)pBindParam->buffer, pVal, bytes);
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_NCHAR:
      default:
        break;
    }
    return pBindParam;
  }

 private:
  TAOS_MULTI_BIND* initParam(TAOS_MULTI_BIND* pBindParams, int32_t index, int32_t type, int32_t bytes) {
    TAOS_MULTI_BIND* pBindParam = pBindParams + index;
    pBindParam->buffer_type = type;
    pBindParam->num = 1;
    pBindParam->buffer_length = bytes > 0 ? bytes : tDataTypes[type].bytes;
    pBindParam->buffer = taosMemoryCalloc(1, pBindParam->buffer_length);
    pBindParam->length = (int32_t*)taosMemoryCalloc(1, sizeof(int32_t));
    pBindParam->is_null = (char*)taosMemoryCalloc(1, sizeof(char));
    *(pBindParam->length) = bytes > 0 ? bytes : tDataTypes[type].bytes;
    *(pBindParam->is_null) = 0;
    return pBindParam;
  }
};

TEST_F(PlanStmtTest, basic) {
  useDb("root", "test");

  prepare("SELECT * FROM t1 WHERE c1 = ?");
  TAOS_MULTI_BIND* pBindParams = buildIntegerParam(createBindParams(1), 0, 10, TSDB_DATA_TYPE_INT);
  bindParams(pBindParams, 0);
  exec();
  destoryBindParams(pBindParams, 1);

  {
    prepare("SELECT * FROM t1 WHERE c1 = ? AND c2 = ?");
    TAOS_MULTI_BIND* pBindParams = createBindParams(2);
    buildIntegerParam(pBindParams, 0, 10, TSDB_DATA_TYPE_INT);
    buildStringParam(pBindParams, 1, "abc", TSDB_DATA_TYPE_VARCHAR, strlen("abc"));
    bindParams(pBindParams, -1);
    exec();
    destoryBindParams(pBindParams, 2);
  }

  {
    prepare("SELECT MAX(?), MAX(?) FROM t1");
    TAOS_MULTI_BIND* pBindParams = createBindParams(2);
    buildIntegerParam(pBindParams, 0, 10, TSDB_DATA_TYPE_TINYINT);
    buildIntegerParam(pBindParams, 1, 20, TSDB_DATA_TYPE_INT);
    bindParams(pBindParams, -1);
    exec();
    destoryBindParams(pBindParams, 2);
  }
}

TEST_F(PlanStmtTest, multiExec) {
  useDb("root", "test");

  prepare("SELECT * FROM t1 WHERE c1 = ?");
  TAOS_MULTI_BIND* pBindParams = buildIntegerParam(createBindParams(1), 0, 10, TSDB_DATA_TYPE_INT);
  bindParams(pBindParams, 0);
  exec();
  destoryBindParams(pBindParams, 1);
  pBindParams = buildIntegerParam(createBindParams(1), 0, 20, TSDB_DATA_TYPE_INT);
  bindParams(pBindParams, 0);
  exec();
  destoryBindParams(pBindParams, 1);
  pBindParams = buildIntegerParam(createBindParams(1), 0, 30, TSDB_DATA_TYPE_INT);
  bindParams(pBindParams, 0);
  exec();
  destoryBindParams(pBindParams, 1);
}

#ifndef TBASE_TESTCOMMON_H_H
#define TBASE_TESTCOMMON_H_H

#include <stdbool.h>
#include <inttypes.h>

#include "taos.h"

typedef struct {
    int32_t     numOfCols;
    int32_t     numOfRows;
    void*   pVal;
} ResultInfo;

int32_t executeSQL(TAOS *conn, char *sql, ResultInfo* pRes);
void    createEnvironment(TAOS *conn, int32_t count, int32_t insertTbl, int32_t pointsPerTbl, int64_t timeDelta);
void    displayData(void* result, int32_t num_fields, TAOS_FIELD* fields, char* temp, ResultInfo* pRes);
void    setResultInfo(ResultInfo* pRes, int32_t col, int32_t row);
TAOS*   connectdb();

void sqlParseTestImpl(TAOS *conn, char *sql, bool boolFlag);

#define SQL_PARSE_CMD_SUCCESS(s)       sqlParseTestImpl(conn, s, true);
#define SQL_PARSE_CMD_FAILED(s)        sqlParseTestImpl(conn, s, false)

#define SUCCESS_SQL(conn, sql, res)      executeSQL(conn, sql, res);
#define NO_VALID_SUCCESS_SQL(conn, sql)  SUCCESS_SQL(conn, sql, NULL)

#define SET_RES_VAL(res, idx, type, v) do {             \
    tVariant* r = &res.pVal[idx];                       \
    r->nType = type;                                    \
    switch(type) {                                      \
        case TSDB_DATA_TYPE_BIGINT:                     \
        case TSDB_DATA_TYPE_INT:                        \
        case TSDB_DATA_TYPE_TIMESTAMP:                  \
        case TSDB_DATA_TYPE_BOOL:   r->i64 = v;break;\
        case TSDB_DATA_TYPE_DOUBLE:                     \
        case TSDB_DATA_TYPE_FLOAT: r->dKey = v;break;   \
        case TSDB_DATA_TYPE_BINARY: break;              \
        default:                                        \
            assert(0);                                  \
    };                                                  \
} while(0);

#endif //TBASE_TESTCOMMON_H_H

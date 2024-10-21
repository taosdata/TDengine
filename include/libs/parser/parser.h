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

#ifndef _TD_PARSER_H_
#define _TD_PARSER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "query.h"
#include "querynodes.h"
#include "insertnodes.h"
#include "catalog.h"

typedef struct SStmtCallback {
  TAOS_STMT* pStmt;
  int32_t (*getTbNameFn)(TAOS_STMT*, char**);
  int32_t (*setInfoFn)(TAOS_STMT*, STableMeta*, void*, SName*, bool, SHashObj*, SHashObj*, const char*);
  int32_t (*getExecInfoFn)(TAOS_STMT*, SHashObj**, SHashObj**);
} SStmtCallback;

typedef enum {
  PARSE_SQL_RES_QUERY = 1,
  PARSE_SQL_RES_SCHEMA,
} SParseResType;

typedef struct SParseSchemaRes {
  int8_t        precision;
  int32_t       numOfCols;
  SSchema*      pSchema;
} SParseSchemaRes;

typedef struct SParseQueryRes {
  SNode*              pQuery;
  SCatalogReq*        pCatalogReq;
  SMetaData           meta;
} SParseQueryRes;

typedef struct SParseSqlRes {
  SParseResType resType;
  union {
    SParseSchemaRes schemaRes;
    SParseQueryRes  queryRes;
  };
} SParseSqlRes;

typedef int32_t (*parseSqlFn)(void*, const char*, const char*, bool, const char*, SParseSqlRes*);

typedef struct SParseCsvCxt {
  TdFilePtr   fp;           // last parsed file
  int32_t     tableNo;      // last parsed table
  SName       tableName;    // last parsed table
  const char* pLastSqlPos;  // the location of the last parsed sql
} SParseCsvCxt;

typedef struct SParseContext {
  uint64_t         requestId;
  int64_t          requestRid;
  int32_t          acctId;
  const char      *db;
  bool             topicQuery;
  void*            pTransporter;
  SEpSet           mgmtEpSet;
  const char*      pSql;    // sql string
  size_t           sqlLen;  // length of the sql string
  char*            pMsg;    // extended error message if exists to help identifying the problem in sql statement.
  int32_t          msgLen;  // max length of the msg
  struct SCatalog* pCatalog;
  SStmtCallback*   pStmtCb;
  const char*      pUser;
  const char*      pEffectiveUser;
  bool             parseOnly;
  bool             isSuperUser;
  bool             enableSysInfo;
  bool             async;
  bool             hasInvisibleCol;
  bool             isView;
  bool             isAudit;
  bool             nodeOffline;
  const char*      svrVer;
  SArray*          pTableMetaPos;    // sql table pos => catalog data pos
  SArray*          pTableVgroupPos;  // sql table pos => catalog data pos
  int64_t          allocatorId;
  parseSqlFn       parseSqlFp;
  void*            parseSqlParam;
  int8_t           biMode;
  SArray*          pSubMetaList;
  TAOS_STMT       *stmt;
  uint8_t          prepare:1; // for prepare only
} SParseContext;

int32_t qParseSql(SParseContext* pCxt, SQuery** pQuery);
bool    qIsInsertValuesSql(const char* pStr, size_t length);

// for async mode
int32_t qParseSqlSyntax(SParseContext* pCxt, SQuery** pQuery, struct SCatalogReq* pCatalogReq);
int32_t qAnalyseSqlSemantic(SParseContext* pCxt, const struct SCatalogReq* pCatalogReq,
                            const struct SMetaData* pMetaData, SQuery* pQuery);
int32_t qContinueParseSql(SParseContext* pCxt, struct SCatalogReq* pCatalogReq, const struct SMetaData* pMetaData,
                          SQuery* pQuery);
int32_t qContinueParsePostQuery(SParseContext* pCxt, SQuery* pQuery, SSDataBlock* pBlock);

void qDestroyParseContext(SParseContext* pCxt);

void qDestroyQuery(SQuery* pQueryNode);

int32_t qExtractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema);
int32_t qSetSTableIdForRsma(SNode* pStmt, int64_t uid);
void    qCleanupKeywordsTable();

int32_t     qAppendStmtTableOutput(SQuery* pQuery, SHashObj* pAllVgHash, STableColsData* pTbData, STableDataCxt* pTbCtx, SStbInterlaceInfo* pBuildInfo);
int32_t     qBuildStmtFinOutput(SQuery* pQuery, SHashObj* pAllVgHash, SArray* pVgDataBlocks);
//int32_t     qBuildStmtOutputFromTbList(SQuery* pQuery, SHashObj* pVgHash, SArray* pBlockList, STableDataCxt* pTbCtx, int32_t tbNum);
int32_t     qBuildStmtOutput(SQuery* pQuery, int rebuild, SHashObj* pVgHash, SHashObj* pBlockHash);
int32_t qResetStmtColumns(SArray* pCols, bool deepClear);
int32_t     qResetStmtDataBlock(STableDataCxt* block, bool keepBuf);
int32_t     qCloneStmtDataBlock(STableDataCxt** pDst, STableDataCxt* pSrc, bool reset);
int32_t     qRebuildStmtDataBlock(STableDataCxt** pDst, STableDataCxt* pSrc, uint64_t uid, uint64_t suid, int32_t vgId,
                                  bool rebuildCreateTb);
void        qDestroyStmtDataBlock(STableDataCxt* pBlock);
STableMeta* qGetTableMetaInDataBlock(STableDataCxt* pDataBlock);
int32_t     qCloneCurrentTbData(STableDataCxt* pDataBlock, SSubmitTbData** pData);

int32_t qStmtBindParams(SQuery* pQuery, TAOS_MULTI_BIND* pParams, int32_t colIdx);
int32_t qStmtParseQuerySql(SParseContext* pCxt, SQuery* pQuery);
int32_t qBindStmtStbColsValue(void* pBlock, SArray* pCols, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen, STSchema** pTSchema, SBindInfo* pBindInfos);
int32_t qBindStmtColsValue(void* pBlock, SArray* pCols, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen);
int32_t qBindStmtSingleColValue(void* pBlock, SArray* pCols, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen, int32_t colIdx,
                                int32_t rowNum);
int32_t qBuildStmtColFields(void* pDataBlock, int32_t* fieldNum, TAOS_FIELD_E** fields);
int32_t qBuildStmtTagFields(void* pBlock, void* boundTags, int32_t* fieldNum, TAOS_FIELD_E** fields);
int32_t qBindStmtTagsValue(void* pBlock, void* boundTags, int64_t suid, const char* sTableName, char* tName,
                           TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen);
void    destroyBoundColumnInfo(void* pBoundInfo);
int32_t qCreateSName(SName* pName, const char* pTableName, int32_t acctId, char* dbName, char* msgBuf,
                     int32_t msgBufLen);

void qDestroyBoundColInfo(void* pInfo);

SQuery*        smlInitHandle();
int32_t        smlBuildRow(STableDataCxt* pTableCxt);
int32_t        smlBuildCol(STableDataCxt* pTableCxt, SSchema* schema, void* kv, int32_t index);
STableDataCxt* smlInitTableDataCtx(SQuery* query, STableMeta* pTableMeta);

void    clearColValArraySml(SArray* pCols);
int32_t smlBindData(SQuery* handle, bool dataFormat, SArray* tags, SArray* colsSchema, SArray* cols,
                    STableMeta* pTableMeta, char* tableName, const char* sTableName, int32_t sTableNameLen, int32_t ttl,
                    char* msgBuf, int32_t msgBufLen);
int32_t smlBuildOutput(SQuery* handle, SHashObj* pVgHash);
int     rawBlockBindData(SQuery *query, STableMeta* pTableMeta, void* data, SVCreateTbReq** pCreateTb, TAOS_FIELD *fields,
                         int numFields, bool needChangeLength, char* errstr, int32_t errstrLen);

int32_t rewriteToVnodeModifyOpStmt(SQuery* pQuery, SArray* pBufArray);
SArray* serializeVgroupsCreateTableBatch(SHashObj* pVgroupHashmap);
SArray* serializeVgroupsDropTableBatch(SHashObj* pVgroupHashmap);
void    destoryCatalogReq(SCatalogReq *pCatalogReq);
bool    isPrimaryKeyImpl(SNode* pExpr);
int32_t insAppendStmtTableDataCxt(SHashObj* pAllVgHash, STableColsData* pTbData, STableDataCxt* pTbCtx, SStbInterlaceInfo* pBuildInfo);

void stmt_current_db(TAOS_STMT *stmt, char *db, size_t len);

#define UNESCAPE_ID_TOO_LONG          (const char*)-1
#define UNESCAPE_ID_EMPTY             (const char*)-2
#define UNESCAPE_ID_OK                (const char*)0

const char* unescape_id(char *dst, size_t len, const char * const s, const size_t n);

// NOTE: for the purpose to determine, by simply token scanning,
//       if the sql statement `seems` `insert` or not
//       and how many possible question-marks or parameter-placeholders are there
void qScanSql(const char* pStr, size_t length, uint8_t *is_insert, size_t *questions);

typedef struct tsdb_value_s                 tsdb_value_t;
typedef struct parse_result_s               parse_result_t;
typedef struct parse_result_meta_s          parse_result_meta_t;

struct parse_result_meta_s {
  int32_t                      nr_tags;
  int32_t                      nr_cols;

  uint8_t                      is_insert:1;
  uint8_t                      tbname_required:1;
  uint8_t                      has_using_clause:1;
};

struct parse_result_s {
  char     current_db[TSDB_DB_NAME_LEN]; // NOTE: TSDB_DB_NAME_LEN null-terminator-inclusive

  SNode   *pRootNode;
  SArray  *pPlaceholderValues;
  int16_t  placeholderNo;
  char     err_msg[4096];

  parse_result_meta_t         meta;

  char                       *sql;
  size_t                      nr_sql;
  size_t                      cap_sql;
};

typedef struct mbs_cache_s             mbs_cache_t;
struct mbs_cache_s {
  TAOS_MULTI_BIND  *mbs;
  size_t            cap_mbs;
  char             *buf;
  size_t            cap_buf;
};

void mbs_cache_reset(mbs_cache_t *mbs_cache);
void mbs_cache_release(mbs_cache_t *mbs_cache);

typedef struct SStmtAPI2 {
  parse_result_t    parse_result;

  int               questions; // # of question-marks found by `qScanSql`

  int               nr_tags;   // # of tags
  int               nr_cols;   // # of cols

  TAOS_FIELD_E     *params;        // local cache for param-meta-info
  size_t            cap_params;

  TAOS_MULTI_BIND  *mbs_from_app;
  int               nr_mbs_from_app;

  TAOS_MULTI_BIND  *mbs_cache;     // used during batch submit
  size_t            cap_mbs;

  char             *conv_buf;      // used during param conversion
  size_t            cap_conv_buf;

  mbs_cache_t       mbs_tags;
  mbs_cache_t       mbs_cols;

  TAOS_RES         *res_from_taos_query;

  taos_stmt_prepare2_option_e         options;


  SNode            *pTagCond;
  STableMeta       *pTableMeta;

  uint8_t           scanned:1;         // set after `qScanSql`
  uint8_t           is_insert:1;
  uint8_t           tbname_required:1;

  // NOTE: only valid when tbname_required is set
  // NOTE: bypass tag-related routines when it's set
  uint8_t           without_using_clause:1;

  uint8_t           prepared:1;        // set when taos_stmt_prepare2 succeed

  uint8_t           use_res_from_taos_query:1;
} SStmtAPI2;


void parse_result_reset(parse_result_t *result);
void parse_result_release(parse_result_t *result);

#define parse_result_set_errmsg(result, err, fmt, ...)                         \
  (                                                                            \
  snprintf(result->err_msg, sizeof(result->err_msg),                           \
      "%s[%d]:%s():[0x%08x]%s:" fmt "",                                        \
      __FILE__, __LINE__, __func__,                                            \
      err, tstrerror(err),                                                     \
      ##__VA_ARGS__),                                                          \
  err                                                                          \
  )

void parse_result_get_first_table_name(parse_result_t *parse_result, db_table_t *tbl);
void parse_result_get_first_using_table_name(parse_result_t *parse_result, db_table_t *using_tbl);
void parse_result_create_sname(parse_result_t *parse_result, SName* dst, db_table_t *src, int32_t acctId, const char* dbName);

typedef struct desc_params_ctx_s            desc_params_ctx_t;
struct desc_params_ctx_s {
  SRequestConnInfo   connInfo;
  SCatalog          *pCatalog;
  const char        *current_user;
  int32_t            acctId;
  const char        *current_db;

  SStmtAPI2         *api2;
};

typedef struct eval_env_s           eval_env_t;
struct eval_env_s {
  TAOS_MULTI_BIND           *mbs;
  size_t                     nr_mbs;

  const char                *current_db;
  int64_t                    epoch_nano;
};

int32_t parse_result_describe_params(desc_params_ctx_t *ctx);
int32_t parse_result_get_target_table(parse_result_t *parse_result, int row, const char **tbname, eval_env_t *env);
void parse_result_normalize_db_table(db_table_t *db_tbl, const char *current_db);

int32_t qParsePure(TAOS_STMT *stmt, const char *sql, size_t len, parse_result_t *result);

int32_t tsdb_value_from_TAOS_MULTI_BIND(tsdb_value_t *v, TAOS_MULTI_BIND *mb, int row);
int tsdb_value_type(const tsdb_value_t *v);
int32_t tsdb_value_buffer(const tsdb_value_t *v, const char **data, int32_t *len);
int tsdb_value_as_epoch_nano(tsdb_value_t *v, int64_t epoch_nano);
typedef enum {
  PARSE_RESULT_TEMPLATIFY_SQL_OK,
  PARSE_RESULT_TEMPLATIFY_SQL_OOM,
  PARSE_RESULT_TEMPLATIFY_SQL_INTERNAL_ERROR,
} parse_result_templatify_sql_e;
parse_result_templatify_sql_e parse_result_templatify_sql(parse_result_t *parse_result);

typedef struct parse_result_visit_ctx_s          parse_result_visit_ctx_t;
struct parse_result_visit_ctx_s {
  eval_env_t               *env;
  int32_t (*on_tbname)(const char *tbname, size_t len, int row, void *arg, int *abortion);
  int32_t (*on_tag_val)(const value_t *val, int start, int row, int col, void *arg, int *abortion);
  int32_t (*on_col_val)(const value_t *val, int start, int row, int col, void *arg, int *abortion);
  void                     *arg;
};

const tsdb_value_t* value_tsdb_value(const value_t *value);
void tsdb_dump(const tsdb_value_t *v);

int32_t parse_result_visit(parse_result_t *parse_result, int row, int rows, parse_result_visit_ctx_t *ctx);
int32_t parse_result_dump_vals(parse_result_t *parse_result, eval_env_t *env);

void dump_SName_x(const char *file, int line, const char *func, SName *name);

#define dump_SName(name) dump_SName_x(__FILE__, __LINE__, __func__, name)


#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_H_*/

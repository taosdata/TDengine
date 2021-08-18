#ifndef TDENGINE_QTABLEUTIL_H
#define TDENGINE_QTABLEUTIL_H

#include "tsdb.h"  //todo tsdb should not be here
#include "qSqlparser.h"
#include "qFilter.h"

typedef struct SFieldInfo {
  int16_t      numOfOutput;   // number of column in result
  TAOS_FIELD*  final;
  SArray      *internalField; // SArray<SInternalField>
} SFieldInfo;

typedef struct SCond {
  uint64_t uid;
  int32_t  len;  // length of tag query condition data
  char *   cond;
} SCond;

typedef struct STblCond {
  uint64_t uid;
  int16_t  idx;  //table index
  int32_t  len;  // length of tag query condition data
  char *   cond;
} STblCond;


typedef struct SJoinNode {
  uint64_t uid;
  int16_t  tagColId;
  SArray*  tsJoin;
  SArray*  tagJoin;
} SJoinNode;

typedef struct SJoinInfo {
  bool       hasJoin;
  SJoinNode *joinTables[TSDB_MAX_JOIN_TABLE_NUM];
} SJoinInfo;

typedef struct STagCond {
  // relation between tbname list and query condition, including : TK_AND or TK_OR
  int16_t relType;

  // tbname query condition, only support tbname query condition on one table
  SCond tbnameCond;

  // join condition, only support two tables join currently
  SJoinInfo joinInfo;

  // for different table, the query condition must be seperated
  SArray *pCond;
} STagCond;

typedef struct SGroupbyExpr {
  int16_t tableIndex;
  SArray* columnInfo;  // SArray<SColIndex>, group by columns information
  int16_t numOfGroupCols;  // todo remove it
  int16_t orderIndex;  // order by column index
  int16_t orderType;   // order by type: asc/desc
} SGroupbyExpr;

typedef struct STableComInfo {
  uint8_t numOfTags;
  uint8_t precision;
  int16_t numOfColumns;
  int32_t rowSize;
} STableComInfo;

typedef struct STableMeta {
  int32_t        vgId;
  STableId       id;
  int8_t         tableType;
  char           sTableName[TSDB_TABLE_FNAME_LEN];  // super table name
  uint64_t       suid;       // super table id
  int16_t        sversion;
  int16_t        tversion;
  STableComInfo  tableInfo;
  SSchema        schema[];  // if the table is TSDB_CHILD_TABLE, schema is acquired by super table meta info
} STableMeta;

typedef struct STableMetaInfo {
  STableMeta    *pTableMeta;      // table meta, cached in client side and acquired by name
  uint32_t      tableMetaSize;
  size_t        tableMetaCapacity; 
  SVgroupsInfo  *vgroupList;
  SArray        *pVgroupTables;   // SArray<SVgroupTableInfo>

  /*
   * 1. keep the vgroup index during the multi-vnode super table projection query
   * 2. keep the vgroup index for multi-vnode insertion
   */
  int32_t       vgroupIndex;
  SName         name;
  char          aliasName[TSDB_TABLE_NAME_LEN];    // alias name of table specified in query sql
  SArray       *tagColList;                        // SArray<SColumn*>, involved tag columns
} STableMetaInfo;

struct   SQInfo;      // global merge operator
struct   SQueryAttr;     // query object

typedef struct STableFilter {
  uint64_t uid;
  SFilterInfo info;
} STableFilter;

typedef struct SQueryInfo {
  int16_t          command;       // the command may be different for each subclause, so keep it seperately.
  uint32_t         type;          // query/insert type
  STimeWindow      window;        // the whole query time window

  SInterval        interval;      // tumble time window
  SSessionWindow   sessionWindow; // session time window

  SGroupbyExpr     groupbyExpr;   // groupby tags info
  SArray *         colList;       // SArray<SColumn*>
  SFieldInfo       fieldsInfo;
  SArray *         exprList;      // SArray<SExprInfo*>
  SArray *         exprList1;     // final exprlist in case of arithmetic expression exists
  SLimitVal        limit;
  SLimitVal        slimit;
  STagCond         tagCond;

  SArray *         colCond;

  SOrderVal        order;
  int16_t          numOfTables;
  int16_t          curTableIdx;
  STableMetaInfo **pTableMetaInfo;
  struct STSBuf   *tsBuf;

  int16_t          fillType;      // final result fill type
  int64_t *        fillVal;       // default value for fill
  int32_t          numOfFillVal;  // fill value size

  char *           msg;           // pointer to the pCmd->payload to keep error message temporarily
  int64_t          clauseLimit;   // limit for current sub clause

  int64_t          prjOffset;     // offset value in the original sql expression, only applied at client side
  int64_t          vgroupLimit;    // table limit in case of super table projection query + global order + limit

  int32_t          udColumnId;    // current user-defined constant output field column id, monotonically decreases from TSDB_UD_COLUMN_INDEX
  bool             distinct;   // distinct tag or not
  bool             onlyHasTagCond;
  int32_t          round;         // 0/1/....
  int32_t          bufLen;
  char*            buf;

  bool               udfCopy;
  SArray            *pUdfInfo;

  struct SQInfo     *pQInfo;      // global merge operator
  struct SQueryAttr *pQueryAttr;     // query object

  struct SQueryInfo *sibling;     // sibling
  SArray            *pUpstream;   // SArray<struct SQueryInfo>
  struct SQueryInfo *pDownstream;
  int32_t            havingFieldNum;
  bool               stableQuery;
  bool               groupbyColumn;
  bool               simpleAgg;
  bool               arithmeticOnAgg;
  bool               projectionQuery;
  bool               hasFilter;
  bool               onlyTagQuery;
  bool               orderProjectQuery;
  bool               stateWindow;
  bool               globalMerge;
} SQueryInfo;

/**
 * get the number of tags of this table
 * @param pTableMeta
 * @return
 */
int32_t tscGetNumOfTags(const STableMeta* pTableMeta);

/**
 * get the number of columns of this table
 * @param pTableMeta
 * @return
 */
int32_t tscGetNumOfColumns(const STableMeta* pTableMeta);

/**
 * get the basic info of this table
 * @param pTableMeta
 * @return
 */
STableComInfo tscGetTableInfo(const STableMeta* pTableMeta);

/**
 * get the schema
 * @param pTableMeta
 * @return
 */
SSchema* tscGetTableSchema(const STableMeta* pTableMeta);

/**
 * get the tag schema
 * @param pMeta
 * @return
 */
SSchema *tscGetTableTagSchema(const STableMeta *pMeta);

/**
 * get the column schema according to the column index
 * @param pMeta
 * @param colIndex
 * @return
 */
SSchema *tscGetTableColumnSchema(const STableMeta *pMeta, int32_t colIndex);

/**
 * get the column schema according to the column id
 * @param pTableMeta
 * @param colId
 * @return
 */
SSchema* tscGetColumnSchemaById(STableMeta* pTableMeta, int16_t colId);

/**
 * create the table meta from the msg
 * @param pTableMetaMsg
 * @param size size of the table meta
 * @return
 */
STableMeta* tscCreateTableMetaFromMsg(STableMetaMsg* pTableMetaMsg);

#endif  // TDENGINE_QTABLEUTIL_H

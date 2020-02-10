/************************************
 * For internal usage
 ************************************/

#include <pthread.h>

#include "tsdb.h"

// Initially, there are 4 tables
#define TSDB_INIT_NUMBER_OF_SUPER_TABLE 4

typedef enum : uint8_t {
  TSDB_SUPER_TABLE,  // super table
  TSDB_NTABLE,       // table not created from super table
  TSDB_STABLE        // table created from super table
} TSDB_TABLE_TYPE;

typedef struct STable {
  tsdb_id_t       tableId;
  int64_t         uid;
  char *          tableName;
  TSDB_TABLE_TYPE type;

  int64_t createdTime;

  // super table UID
  tsdb_id_t superTableId;

  // Schema for this table
  // For TSDB_SUPER_TABLE, it is the schema including tags
  // For TSDB_NTABLE, it is only the schema, not including tags
  // For TSDB_STABLE, it is NULL
  SVSchema *pSchema;

  // Tag value for this table
  // For TSDB_SUPER_TABLE and TSDB_NTABLE, it is NULL
  // For TSDB_STABLE, it is the tag value string
  char *pTagVal;

  // Object content;
  // For TSDB_SUPER_TABLE, it is the index of tables created from it
  // For TSDB_STABLE and TSDB_NTABLE, it is the cache data
  union {
    void *pData;
    void *pIndex;
  } content;

  // A handle to deal with event
  void *eventHandle;

  // A handle to deal with stream
  void *streamHandle;

} STable;

typedef struct {
  int32_t numOfTables;       // Number of tables not including TSDB_SUPER_TABLE (#TSDB_NTABLE + #TSDB_STABLE)
  int32_t numOfSuperTables;  // Number of super tables (#TSDB_SUPER_TABLE)
  // An array of tables (TSDB_NTABLE and TSDB_STABLE) in this TSDB repository
  STable **pTables;

  // A map of tableName->tableId
  // TODO: May use hash table
  void *pNameTableMap;
} SMetaHandle;

// ---- Operation on STable
#define TSDB_TABLE_ID(pTable) ((pTable)->tableId)
#define TSDB_TABLE_UID(pTable) ((pTable)->uid)
#define TSDB_TABLE_NAME(pTable) ((pTable)->tableName)
#define TSDB_TABLE_TYPE(pTable) ((pTable)->type)
#define TSDB_TABLE_SUPER_TABLE_UID(pTable) ((pTable)->stableUid)
#define TSDB_TABLE_IS_SUPER_TABLE(pTable) (TSDB_TABLE_TYPE(pTable) == TSDB_SUPER_TABLE)
#define TSDB_TABLE_TAG_VALUE(pTable) ((pTable)->pTagVal)
#define TSDB_TABLE_CACHE_DATA(pTable) ((pTable)->content.pData)
#define TSDB_SUPER_TABLE_INDEX(pTable) ((pTable)->content.pIndex)

SVSchema *tsdbGetTableSchema(STable *pTable);

// ---- Operation on SMetaHandle
#define TSDB_NUM_OF_TABLES(pHandle) ((pHandle)->numOfTables)
#define TSDB_NUM_OF_SUPER_TABLES(pHandle) ((pHandle)->numOfSuperTables)
#define TSDB_TABLE_OF_ID(pHandle, id) ((pHandle)->pTables)[id]
#define TSDB_GET_TABLE_OF_NAME(pHandle, name) /* TODO */
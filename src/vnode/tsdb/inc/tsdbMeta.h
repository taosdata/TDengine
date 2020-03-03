/************************************
 * For internal usage
 ************************************/

#include <pthread.h>

// #include "taosdef.h"

// Initially, there are 4 tables
#define TSDB_INIT_NUMBER_OF_SUPER_TABLE 4

typedef enum {
  TSDB_SUPER_TABLE,  // super table
  TSDB_NTABLE,       // table not created from super table
  TSDB_STABLE        // table created from super table
} TSDB_TABLE_TYPE;

typedef struct STable {
  int32_t         tableId;
  int64_t         uid;
  char *          tableName;
  TSDB_TABLE_TYPE type;

  int64_t createdTime;

  // super table UID
  int32_t superTableId;

  // Schema for this table
  // For TSDB_SUPER_TABLE, it is the schema including tags
  // For TSDB_NTABLE, it is only the schema, not including tags
  // For TSDB_STABLE, it is NULL
  SSchema *pSchema;

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

  struct STable *next;

} STable;

typedef struct {
  int32_t          maxTables;
  int32_t          numOfSuperTables;  // Number of super tables (#TSDB_SUPER_TABLE)
  STable **        tables;            // array of normal tables
  STable *         stables;           // linked list of super tables
  void *           tableMap;          // table map of name ==> table
} STsdbMeta;

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

SSchema *tsdbGetTableSchema(STable *pTable);

// ---- Operation on SMetaHandle
#define TSDB_NUM_OF_TABLES(pHandle) ((pHandle)->numOfTables)
#define TSDB_NUM_OF_SUPER_TABLES(pHandle) ((pHandle)->numOfSuperTables)
#define TSDB_TABLE_OF_ID(pHandle, id) ((pHandle)->pTables)[id]
#define TSDB_GET_TABLE_OF_NAME(pHandle, name) /* TODO */

// Create a new meta handle with configuration
STsdbMeta *tsdbCreateMeta(int32_t maxTables);
int32_t    tsdbFreeMeta(STsdbMeta *pMeta);

// Recover the meta handle from the file
STsdbMeta *tsdbOpenMetaHandle(char *tsdbDir);

int32_t tsdbCreateTableImpl(STsdbMeta *pHandle, STableCfg *pCfg);

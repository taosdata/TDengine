/************************************
 * For internal usage
 ************************************/

#include "tsdb.h"

typedef enum {
  TSDB_TABLE_NORMAL,
  TSDB_TABLE_STABLE,
  TSDB_TABLE_SUPER
} TSDB_TABLE_TYPE;

// Below is the struct definition of super table
// TODO: May merge all table definitions
typedef struct _super_table {
  int64_t uid;
  char *tableName;

  int64_t createdTime;

  // TODO: try to merge the two schema into one
  SSchema *pSchema;
  SSchema *pTagSchema;

  // A index created for all tables created using this super table
  SSkipList * pIndex;
} SSTable;

// Normal table struct definition, table not
// created from super table
typedef struct SNTable
{
  tsdb_id_t tableId;
  int64_t uid;
  char *tableName;
  
  int64_t createdTime;
  SSchema *pSchema
} SNTable;

// Table created from super table
typedef struct STable {
  tsdb_id_t tableId;
  int64_t uid;
  char *tableName;

  int64_t createdTime;

  // super table UID
  int64_t stableUid;

  // Tag values for this table
  char *pTagVal;

  // Cached data
  SSkipList *pData;
} STable;

#define TSDB_GET_TABLE_ID(pTable) (((STable *)pTable)->tid).tableId
#define TSDB_GET_TABLE_UID(pTable) (((STable *)pTable)->tid).uid

#define TSDB_IS_SUPER_TABLE(pTable)

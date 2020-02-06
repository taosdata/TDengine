/************************************
 * For internal usage
 ************************************/

#include "tsdb.h"

typedef struct STable
{
  STableId tid;
  char *tableName;
} STable;

#define TSDB_GET_TABLE_ID(pTable) (((STable *)pTable)->tid).tableId
#define TSDB_GET_TABLE_UID(pTable) (((STable *)pTable)->tid).uid

#define TSDB_IS_SUPER_TABLE(pTable)

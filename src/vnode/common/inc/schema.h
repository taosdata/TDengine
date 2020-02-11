#if !defined(_TD_SCHEMA_H_)
#define _TD_SCHEMA_H_

#include <stdint.h>

#include "type.h"

// Column definition
// TODO: if we need to align the structure
typedef struct {
  td_datatype_t type;     // Column type
  int32_t       colId;    // column ID
  int32_t       bytes;    // column bytes
  char *        colName;  // the column name
} SColumn;

// Schema definition
typedef struct {
  int32_t  version;  // schema version, it is used to change the schema
  int32_t  numOfCols;
  int32_t  numOfTags;
  int32_t  colIdCounter;
  SColumn *columns;
} SSchema;

/* Inline schema definition
 * +---------+---------+---------+-----+---------+-----------+-----+-----------+
 * | int32_t |         |         |     |         |           |     |           |
 * +---------+---------+---------+-----+---------+-----------+-----+-----------+
 * |   len   | SSchema | SColumn | ... | SColumn | col1_name | ... | colN_name |
 * +---------+---------+---------+-----+---------+-----------+-----+-----------+
 */
typedef char *SISchema

// ---- operation on SColumn
#define TD_COLUMN_TYPE(pCol) ((pCol)->type)
#define TD_COLUMN_ID(pCol) ((pCol)->colId)
#define TD_COLUMN_BYTES(pCol) ((pCol)->bytes)
#define TD_COLUMN_NAME(pCol) ((pCol)->colName)

// ---- operation on SSchema
#define TD_SCHEMA_VERSION(pSchema) ((pSchema)->version)
#define TD_SCHEMA_NCOLS(pSchema) ((pSchema)->numOfCols)
#define TD_SCHEMA_NTAGS(pSchema) ((pSchema)->numOfTags)
#define TD_SCHEMA_TOTAL_COLS(pSchema) (TD_SCHEMA_NCOLS(pSchema) + TD_SCHEMA_NTAGS(pSchema))
#define TD_SCHEMA_NEXT_COLID(pSchema) ((pSchema)->colIdCounter++)
#define TD_SCHEMA_COLS(pSchema) ((pSchema)->columns)
#define TD_SCHEMA_TAGS(pSchema) (TD_SCHEMA_COLS(pSchema) + TD_SCHEMA_NCOLS(pSchema))
#define TD_SCHEMA_COLUMN_AT(pSchema, idx) TD_SCHEMA_COLS(pSchema)[idx]
#define TD_SCHEMA_TAG_AT(pSchema, idx) TD_SCHEMA_TAGS(pSchema)[idx]

// ---- operation on SISchema
#define TD_ISCHEMA_LEN(pISchema) *((int32_t *)(pISchema))
#define TD_ISCHEMA_SCHEMA(pISchema) ((SSchema *)((pISchema) + sizeof(int32_t)))

#endif  // _TD_SCHEMA_H_

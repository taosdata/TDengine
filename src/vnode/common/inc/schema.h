#if !defined(_TD_SCHEMA_H_)
#define _TD_SCHEMA_H_

#include <stdint.h>
#include <string.h>

#include "type.h"

// Column definition
// TODO: if we need to align the structure
typedef struct {
  td_datatype_t type;     // Column type
  int32_t       colId;    // column ID
  int32_t       bytes;    // column bytes
  int32_t       offset;   // point offset in a row data
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
typedef char *SISchema;

// TODO: decide if the space is allowed
#define TD_ISCHEMA_HEADER_SIZE sizeof(int32_t) + sizeof(SSchema)

// ---- operations on SColumn
#define TD_COLUMN_TYPE(pCol) ((pCol)->type)     // column type
#define TD_COLUMN_ID(pCol) ((pCol)->colId)      // column ID
#define TD_COLUMN_BYTES(pCol) ((pCol)->bytes)   // column bytes
#define TD_COLUMN_OFFSET(pCol) ((pCol)->offset)   // column bytes
#define TD_COLUMN_NAME(pCol) ((pCol)->colName)  // column name
#define TD_COLUMN_INLINE_SIZE(pCol) (sizeof(SColumn) + TD_COLUMN_NAME(pCol) + 1)

// ---- operations on SSchema
#define TD_SCHEMA_VERSION(pSchema) ((pSchema)->version)      // schema version
#define TD_SCHEMA_NCOLS(pSchema) ((pSchema)->numOfCols)      // schema number of columns
#define TD_SCHEMA_NTAGS(pSchema) ((pSchema)->numOfTags)      // schema number of tags
#define TD_SCHEMA_TOTAL_COLS(pSchema) (TD_SCHEMA_NCOLS(pSchema) + TD_SCHEMA_NTAGS(pSchema)) // schema total number of SColumns (#columns + #tags)
#define TD_SCHEMA_NEXT_COLID(pSchema) ((pSchema)->colIdCounter++)
#define TD_SCHEMA_COLS(pSchema) ((pSchema)->columns)
#define TD_SCHEMA_TAGS(pSchema) (TD_SCHEMA_COLS(pSchema) + TD_SCHEMA_NCOLS(pSchema))
#define TD_SCHEMA_COLUMN_AT(pSchema, idx) (TD_SCHEMA_COLS(pSchema) + idx)
#define TD_SCHEMA_TAG_AT(pSchema, idx) (TD_SCHEMA_TAGS(pSchema) + idx)

// ---- operations on SISchema
#define TD_ISCHEMA_LEN(pISchema) *((int32_t *)(pISchema))
#define TD_ISCHEMA_SCHEMA(pISchema) ((SSchema *)((pISchema) + sizeof(int32_t)))
#define TD_ISCHEMA_COL_NAMES(pISchema) ((pISchema) + TD_ISCHEMA_HEADER_SIZE + sizeof(SColumn) * TD_SCHEMA_TOTAL_COLS(TD_ISCHEMA_SCHEMA(pISchema)))

// ----
/* Convert a schema structure to an inline schema structure
 */
SISchema tdConvertSchemaToInline(SSchema *pSchema);
int32_t tdGetColumnIdxByName(SSchema *pSchema, char *colName);
int32_t tdGetColumnIdxById(SSchema *pSchema, int32_t colId);
SSchema *tdDupSchema(SSchema *pSchema);

// ---- TODO: operations to modify schema

#endif  // _TD_SCHEMA_H_

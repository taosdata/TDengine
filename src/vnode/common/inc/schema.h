#ifndef _TD_SCHEMA_H_
#define _TD_SCHEMA_H_

#include <stdint.h>
#include <string.h>

#include "type.h"

#ifdef __cplusplus
extern "C" {
#endif

// ---- Column definition and operations
typedef struct {
  int8_t  type;    // Column type
  int16_t colId;   // column ID
  int16_t bytes;   // column bytes
  int32_t offset;  // point offset in a row data
} SColumn;

#define colType(col) ((col)->type)
#define colColId(col) ((col)->colId)
#define colBytes(col) ((col)->bytes)
#define colOffset(col) ((col)->offset)

#define colSetType(col, t) (colType(col) = (t))
#define colSetColId(col, id) (colColId(col) = (id))
#define colSetBytes(col, b) (colBytes(col) = (b))
#define colSetOffset(col, o) (colOffset(col) = (o))

SColumn *tdNewCol(int8_t type, int16_t colId, int16_t bytes);
void tdFreeCol(SColumn *pCol);
void tdColCpy(SColumn *dst, SColumn *src);

// ---- Schema definition and operations
typedef struct {
  int32_t  numOfCols;
  int32_t  padding;   // TODO: replace the padding for useful variable
  SColumn  columns[];
} SSchema;

#define schemaNCols(s) ((s)->numOfCols)
#define schemaColAt(s, i) ((s)->columns + i)

SSchema *tdNewSchema(int32_t nCols);
SSchema *tdDupSchema(SSchema *pSchema);
void tdFreeSchema(SSchema *pSchema);
void tdUpdateSchema(SSchema *pSchema);
int32_t tdMaxRowDataBytes(SSchema *pSchema);

// ---- Inline schema definition and operations

/* Inline schema definition
 * +---------+---------+---------+-----+---------+-----------+-----+-----------+
 * | int32_t |         |         |     |         |           |     |           |
 * +---------+---------+---------+-----+---------+-----------+-----+-----------+
 * |   len   | SSchema | SColumn | ... | SColumn | col1_name | ... | colN_name |
 * +---------+---------+---------+-----+---------+-----------+-----+-----------+
 */
typedef char *SISchema;

// TODO: add operations on SISchema

#ifdef __cplusplus
}
#endif

#endif  // _TD_SCHEMA_H_

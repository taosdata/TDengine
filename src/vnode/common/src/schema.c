#include <stdlib.h>

#include "schema.h"

static size_t tdGetEstimatedISchemaLen(SSchema *pSchema) {
  size_t colNameLen = 0;
  for (size_t i = 0; i < TD_SCHEMA_NCOLS(pSchema); i++) {
    colNameLen += (strlen(TD_COLUMN_NAME(TD_SCHEMA_COLUMN_AT(pSchema, i))) + 1);
  }

  for (size_t i = 0; i < TD_SCHEMA_NCOLS(pSchema); i++) {
    colNameLen += (strlen(TD_COLUMN_NAME(TD_SCHEMA_COLUMN_AT(pSchema, i))) + 1);
  }

  return TD_ISCHEMA_HEADER_SIZE + (size_t)TD_SCHEMA_TOTAL_COLS(pSchema) + colNameLen;
}

static void tdUpdateColumnOffsets(SSchema *pSchema) {
  int32_t offset = 0;
  for (size_t i = 0; i < TD_SCHEMA_NCOLS(pSchema); i++)
  {
    SColumn *pCol = TD_SCHEMA_COLUMN_AT(pSchema, i);
    TD_COLUMN_OFFSET(pCol) = offset;
    offset += rowDataLen[TD_COLUMN_TYPE(pCol)];
  }

  offset = 0;
  for (size_t i = 0; i < TD_SCHEMA_NTAGS(pSchema); i++) {
    SColumn *pCol = TD_SCHEMA_TAG_AT(pSchema, i);
    TD_COLUMN_OFFSET(pCol) = offset;
    offset += rowDataLen[TD_COLUMN_TYPE(pCol)];
  }
}

SISchema tdConvertSchemaToInline(SSchema *pSchema) {
  size_t  len = tdGetEstimatedISchemaLen(pSchema);
  int32_t totalCols = TD_SCHEMA_TOTAL_COLS(pSchema);
  // TODO: if use pISchema is reasonable?
  SISchema pISchema = malloc(len);
  if (pSchema == NULL) {
    // TODO: add error handling
    return NULL;
  }

  TD_ISCHEMA_LEN(pISchema) = (int32_t)len;
  memcpy((void *)TD_ISCHEMA_SCHEMA(pISchema), (void *)pSchema, sizeof(SSchema));
  TD_SCHEMA_COLS(TD_ISCHEMA_SCHEMA(pISchema)) = (SColumn *)(pISchema + TD_ISCHEMA_HEADER_SIZE);
  memcpy((void *)TD_SCHEMA_COLS(TD_ISCHEMA_SCHEMA(pISchema)), (void *)TD_SCHEMA_COLS(pSchema),
         sizeof(SColumn) * totalCols);

  char *pName = TD_ISCHEMA_COL_NAMES(pISchema);
  for (int32_t i = 0; i < totalCols; i++) {
    SColumn *pCol = TD_SCHEMA_COLUMN_AT(TD_ISCHEMA_SCHEMA(pISchema), i);
    char *   colName = TD_COLUMN_NAME(TD_SCHEMA_COLUMN_AT(pSchema, i), i);

    TD_COLUMN_NAME(pCol) = pName;

    size_t tlen = strlen(colName) + 1;
    memcpy((void *)pName, (void *)colName, tlen);
    pName += tlen;
  }

  return pISchema;
}

int32_t tdGetColumnIdxByName(SSchema *pSchema, char *colName) {
  for (int32_t i = 0; i < TD_SCHEMA_TOTAL_COLS(pSchema); i++) {
    SColumn *pCol = TD_SCHEMA_COLUMN_AT(pSchema, i);
    if (strcmp(colName, TD_COLUMN_NAME(pCol)) == 0) {
      return i;
    }
  }

  return -1;
}

int32_t tdGetColumnIdxById(SSchema *pSchema, int32_t colId) {
  for (int32_t i = 0; i < TD_SCHEMA_TOTAL_COLS(pSchema); i++) {
    SColumn *pCol = TD_SCHEMA_COLUMN_AT(pSchema, i);
    if (TD_COLUMN_ID(pCol) == colId) {
      return i;
    }
  }
  return -1;
}
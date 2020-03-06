#include <stdlib.h>

#include "schema.h"

const int32_t rowDataLen[] = {
    sizeof(int8_t),   // TD_DATATYPE_BOOL,
    sizeof(int8_t),   // TD_DATATYPE_TINYINT,
    sizeof(int16_t),  // TD_DATATYPE_SMALLINT,
    sizeof(int32_t),  // TD_DATATYPE_INT,
    sizeof(int64_t),  // TD_DATATYPE_BIGINT,
    sizeof(float),    // TD_DATATYPE_FLOAT,
    sizeof(double),   // TD_DATATYPE_DOUBLE,
    sizeof(int32_t),  // TD_DATATYPE_VARCHAR,
    sizeof(int32_t),  // TD_DATATYPE_NCHAR,
    sizeof(int32_t)   // TD_DATATYPE_BINARY
};

/**
 * Create a new SColumn object
 * ASSUMPTIONS: VALID PARAMETERS
 * 
 * @param type column type
 * @param colId column ID
 * @param bytes maximum bytes the col taken
 * 
 * @return a SColumn object on success
 *         NULL for failure
 */
SColumn *tdNewCol(int8_t type, int16_t colId, int16_t bytes) {
  SColumn *pCol = (SColumn *)calloc(1, sizeof(SColumn));
  if (pCol == NULL) return NULL;

  colSetType(pCol, type);
  colSetColId(pCol, colId);
  switch (type) {
    case TD_DATATYPE_VARCHAR:
    case TD_DATATYPE_NCHAR:
    case TD_DATATYPE_BINARY:
      colSetBytes(pCol, bytes);
      break;
    default:
      colSetBytes(pCol, rowDataLen[type]);
      break;
  }

  return pCol;
}

/**
 * Free a SColumn object CREATED with tdNewCol
 */
void tdFreeCol(SColumn *pCol) {
  if (pCol) free(pCol);
}

void tdColCpy(SColumn *dst, SColumn *src) { memcpy((void *)dst, (void *)src, sizeof(SColumn)); }

/**
 * Create a SSchema object with nCols columns
 * ASSUMPTIONS: VALID PARAMETERS
 *
 * @param nCols number of columns the schema has
 *
 * @return a SSchema object for success
 *         NULL for failure
 */
SSchema *tdNewSchema(int32_t nCols) {
  int32_t  size = sizeof(SSchema) + sizeof(SColumn) * nCols;

  SSchema *pSchema = (SSchema *)calloc(1, size);
  if (pSchema == NULL) return NULL;
  pSchema->numOfCols = nCols;

  return pSchema;
}

/**
 * Free the SSchema object created by tdNewSchema or tdDupSchema
 */
void tdFreeSchema(SSchema *pSchema) {
  if (pSchema == NULL) free(pSchema);
}

SSchema *tdDupSchema(SSchema *pSchema) {
  SSchema *tSchema = tdNewSchema(schemaNCols(pSchema));
  if (tSchema == NULL) return NULL;

  int32_t size = sizeof(SSchema) + sizeof(SColumn) * schemaNCols(pSchema);
  memcpy((void *)tSchema, (void *)pSchema, size);

  return tSchema;
}

/**
 * Function to update each columns's offset field in the schema.
 * ASSUMPTIONS: VALID PARAMETERS
 */
void tdUpdateSchema(SSchema *pSchema) {
  SColumn *pCol = NULL;
  int32_t offset = 0;
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    pCol = schemaColAt(pSchema, i);
    colSetOffset(pCol, offset);
    offset += rowDataLen[pCol->type];
  }
}

/**
 * Get the maximum size of a row data with the schema
 */
int32_t tdMaxRowDataBytes(SSchema *pSchema) {
  int32_t  size = 0;
  SColumn *pCol = NULL;
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    pCol = schemaColAt(pSchema, i);
    size += rowDataLen[pCol->type];

    switch (pCol->type) {
      case TD_DATATYPE_VARCHAR:
        size += (pCol->bytes + 1); // TODO: remove literal here
        break;
      case TD_DATATYPE_NCHAR:
        size += (pCol->bytes + 4); // TODO: check and remove literal here
        break;
      case TD_DATATYPE_BINARY:
        size += pCol->bytes;
        break;

      default:
        break;
    }
  }

  return size;
}
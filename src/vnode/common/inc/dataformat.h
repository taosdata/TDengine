#if !defined(_TD_DATA_FORMAT_H_)
#define _TD_DATA_FORMAT_H_

#include <stdint.h>

#include "type.h"
#include "schema.h"

// ----------------- Data row structure

/* A data row, the format of it is like below:
 * +---------+---------------------------------+
 * | int32_t |                                 |
 * +---------+---------------------------------+
 * |   len   |               data              |
 * +---------+---------------------------------+
 */
typedef char* SDataRow;

/* Data rows definition, the format of it is like below:
 * +---------+---------+-----------------------+--------+-----------------------+
 * | int32_t | int32_t |                       |        |                       |
 * +---------+---------+-----------------------+--------+-----------------------+
 * |   len   |  nrows  |        SDataRow       |  ....  |        SDataRow       |
 * +---------+---------+-----------------------+--------+-----------------------+
 */
typedef char * SDataRows;

/* Data column definition
 * +---------+---------+-----------------------+
 * | int32_t | int32_t |                       |
 * +---------+---------+-----------------------+
 * |   len   | npoints |          data         |
 * +---------+---------+-----------------------+
 */
typedef char * SDataCol;

/* Data columns definition
 * +---------+---------+-----------------------+--------+-----------------------+
 * | int32_t | int32_t |                       |        |                       |
 * +---------+---------+-----------------------+--------+-----------------------+
 * |   len   | npoints |        SDataCol       |  ....  |        SDataCol       |
 * +---------+---------+-----------------------+--------+-----------------------+
 */
typedef char * SDataCols;

// ----------------- Data column structure

// ---- operation on SDataRow;
#define TD_DATAROW_LEN(pDataRow) (*(int32_t *)(pDataRow))
#define TD_DATAROW_DATA(pDataRow) ((pDataRow) + sizeof(int32_t))

// ---- operation on SDataRows
#define TD_DATAROWS_LEN(pDataRows) (*(int32_t *)(pDataRows))
#define TD_DATAROWS_ROWS(pDataRows) (*(int32_t *)(pDataRows + sizeof(int32_t)))
#define TD_NEXT_DATAROW(pDataRow)  ((pDataRow) + TD_DATAROW_LEN(pDataRow))

// ---- operation on SDataCol
#define TD_DATACOL_LEN(pDataCol) (*(int32_t *)(pDataCol))
#define TD_DATACOL_NPOINTS(pDataCol) (*(int32_t *)(pDataCol + sizeof(int32_t)))

// ---- operation on SDataCols
#define TD_DATACOLS_LEN(pDataCols) (*(int32_t *)(pDataCols))
#define TD_DATACOLS_NPOINTS(pDataCols) (*(int32_t *)(pDataCols + sizeof(int32_t)))

#endif // _TD_DATA_FORMAT_H_

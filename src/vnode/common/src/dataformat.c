#include <stdlib.h>

#include "dataformat.h"

int32_t tdGetMaxDataRowSize(SSchema *pSchema) {
    int32_t nbytes = 0;

    for (int32_t i = 0; i < TD_SCHEMA_NCOLS(pSchema); i++)
    {
        SColumn *pCol = TD_SCHEMA_COLUMN_AT(pSchema, i);
        td_datatype_t type = TD_COLUMN_TYPE(pCol);

        nbytes += rowDataLen[type];

        switch (type)
        {
        case TD_DATATYPE_VARCHAR:
            nbytes += TD_COLUMN_BYTES(pCol);
            break;
        case TD_DATATYPE_NCHAR:
            nbytes += 4 * TD_COLUMN_BYTES(pCol);
            break;
        case TD_DATATYPE_BINARY:
            nbytes += TD_COLUMN_BYTES(pCol);
            break;
        }
    }

    nbytes += TD_DATA_ROW_HEADER_SIZE;

    return nbytes;
}
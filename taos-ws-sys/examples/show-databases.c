#include "taosws.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
  char* dsn = getenv("TAOS_DSN");
  if (dsn == NULL) {
    dsn = "ws://localhost:6041";
  }
  WS_TAOS *taos = ws_connect_with_dsn(dsn);
  int32_t code = ws_connect_errno(taos);
  if (code != 0) {
    const char *errstr = ws_connect_errstr(taos);
    dprintf(2, "Error [%6x]: %s", code, errstr);
    ws_close(taos);
    return 0;
  }

  WS_RS *rs = ws_query(taos, "show databases");
  code = ws_query_errno(rs);
  if (code != 0) {
    const char *errstr = ws_connect_errstr(taos);
    dprintf(2, "Error [%6x]: %s", code, errstr);
    ws_free_result(rs);
    ws_close(taos);
    return 0;
  }

  int precision = ws_result_precision(rs);
  int cols = ws_num_of_fields(rs);
  const struct WS_FIELD_V2 *fields = ws_fetch_fields_v2(rs);
  for (int col = 0; col < cols; col++) {
    const struct WS_FIELD_V2 *field = &fields[col];
    dprintf(2, "column %d: name: %s, length: %d, type: %d\n", col, field->name,
           field->bytes, field->type);
  }

  for (int col = 0; col < cols; col++) {
    if (col == 0) {
      printf("%s", fields[col].name);
    } else {
      printf(",%s", fields[col].name);
    }
  }
  printf("\n");

  while (true) {
    int rows = 0;
    const void *data = NULL;
    code = ws_fetch_block(rs, &data, &rows);

    if (rows == 0)
      break;
    uint8_t ty;
    uint32_t len;
    char tmp[4096];

    for (int row = 0; row < rows; row++) {

      for (int col = 0; col < cols; col++) {
        if (col != 0)
          printf(",");
        const void *value = ws_get_value_in_block(rs, row, col, &ty, &len);
        if (value == NULL) {
          printf(" NULL ");
          continue;
        }
        // printf("%d", ty);
        switch (ty) {
        case TSDB_DATA_TYPE_NULL:
          printf(" NULL ");
          break;
        case TSDB_DATA_TYPE_BOOL:
          if (*(bool*)(value)) {
            printf(" true  ");
          } else {
            printf(" false ");
          }
          break;
        case TSDB_DATA_TYPE_TINYINT:
          printf(" %d ", *(int8_t*)value);
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          printf(" %d ", *(int16_t*)value);
          break;
        case TSDB_DATA_TYPE_INT:
          printf(" %d ", *(int32_t*)value);
          break;
        case TSDB_DATA_TYPE_BIGINT:
          printf(" %ld ", *(int64_t*)value);
          break;
        case TSDB_DATA_TYPE_UTINYINT:
          printf(" %d ", *(uint8_t*)value);
          break;
        case TSDB_DATA_TYPE_USMALLINT:
          printf(" %d ", *(uint16_t*)value);
          break;
        case TSDB_DATA_TYPE_UINT:
          printf(" %d ", *(uint32_t*)value);
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          printf(" %ld ", *(uint64_t*)value);
          break;
        case TSDB_DATA_TYPE_FLOAT:
          printf(" %f ", *(float*)value);
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          printf(" %lf ", *(double*)value);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          const char* ts = ws_timestamp_to_rfc3339(*(int64_t*)value, precision, true);
          printf("\"%s\"", ts);
          break;
        case TSDB_DATA_TYPE_VARCHAR:
          memset(tmp, 0, 4096);
          memcpy(tmp, value, len);
          printf("\"%s\"", (char*)tmp);
          break;
        case TSDB_DATA_TYPE_JSON:
          memset(tmp, 0, 4096);
          memcpy(tmp, value, len);
          printf("'%s'", (char*)tmp);
          break;
        case 10:
          break;
        default:
          printf(" ");
        }
      }
      printf("\n");
    }
  }
}

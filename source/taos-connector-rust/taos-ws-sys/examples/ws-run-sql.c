#include "taosws.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <argp.h>

int main(int argc, char *argv[])
{
  if ((argc < 2) || (strlen(argv[1]) == 0))
  {
    printf("please input a SQL command to query\n");
    exit(1);
  }
  char *dsn = getenv("TAOS_DSN");
  if (dsn == NULL)
  {
    dsn = "ws://localhost:6041";
  }
  ws_enable_log("debug");
  WS_TAOS *taos = ws_connect(dsn);
  if (taos == NULL)
  {
    int code = ws_errno(NULL);
    const char *errstr = ws_errstr(NULL);
    dprintf(2, "Error [%6x]: %s", code, errstr);
    return 0;
  }

  WS_RES *rs = ws_query(taos, argv[1]);
  int code = ws_errno(rs);
  if (code != 0)
  {
    const char *errstr = ws_errstr(taos);
    dprintf(2, "Error [%6x]: %s", code, errstr);
    ws_free_result(rs);
    ws_close(taos);
    return 0;
  }

  int precision = ws_result_precision(rs);
  int cols = ws_field_count(rs);
  const struct WS_FIELD_V2 *fields = ws_fetch_fields_v2(rs);
  for (int col = 0; col < cols; col++)
  {
    const struct WS_FIELD_V2 *field = &fields[col];
    dprintf(2, "column %d: name: %s, length: %d, type: %d\n", col, field->name,
            field->bytes, field->type);
  }

  for (int col = 0; col < cols; col++)
  {
    if (col == 0)
    {
      printf("%s", fields[col].name);
    }
    else
    {
      printf(",%s", fields[col].name);
    }
  }
  printf("\n");

  while (true)
  {
    int rows = 0;
    const void *data = NULL;
    code = ws_fetch_raw_block(rs, &data, &rows);

    if (rows == 0)
      break;
    uint8_t ty;
    uint32_t len;
    char tmp[4096];

    for (int row = 0; row < rows; row++)
    {

      for (int col = 0; col < cols; col++)
      {
        if (col != 0)
          printf(",");
        const void *value = ws_get_value_in_block(rs, row, col, &ty, &len);
        if (value == NULL)
        {
          printf(" NULL ");
          continue;
        }
        // printf("%d", ty);
        switch (ty)
        {
        case TSDB_DATA_TYPE_NULL:
          printf(" NULL ");
          break;
        case TSDB_DATA_TYPE_BOOL:
          if (*(bool *)(value))
          {
            printf(" true  ");
          }
          else
          {
            printf(" false ");
          }
          break;
        case TSDB_DATA_TYPE_TINYINT:
          printf(" %d ", *(int8_t *)value);
          break;
        case TSDB_DATA_TYPE_SMALLINT:
          printf(" %d ", *(int16_t *)value);
          break;
        case TSDB_DATA_TYPE_INT:
          printf(" %d ", *(int32_t *)value);
          break;
        case TSDB_DATA_TYPE_BIGINT:
          printf(" %lld ", *(int64_t *)value);
          break;
        case TSDB_DATA_TYPE_UTINYINT:
          printf(" %d ", *(uint8_t *)value);
          break;
        case TSDB_DATA_TYPE_USMALLINT:
          printf(" %d ", *(uint16_t *)value);
          break;
        case TSDB_DATA_TYPE_UINT:
          printf(" %d ", *(uint32_t *)value);
          break;
        case TSDB_DATA_TYPE_UBIGINT:
          printf(" %lld ", *(uint64_t *)value);
          break;
        case TSDB_DATA_TYPE_FLOAT:
          printf(" %f ", *(float *)value);
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          printf(" %lf ", *(double *)value);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          memset(tmp, 0, 4096);
          ws_timestamp_to_rfc3339((uint8_t *)tmp, *(int64_t *)value, precision, true);
          printf("\"%s\"", (char *)tmp);
          break;
        case TSDB_DATA_TYPE_VARCHAR:
          memset(tmp, 0, 4096);
          memcpy(tmp, value, len);
          printf("\"%s\"", (char *)tmp);
          break;
        case TSDB_DATA_TYPE_NCHAR:
          memset(tmp, 0, 4096);
          memcpy(tmp, value, len);
          printf("\"%s\"", (char *)tmp);
          break;
        case TSDB_DATA_TYPE_JSON:
          memset(tmp, 0, 4096);
          memcpy(tmp, value, len);
          printf("'%s'", (char *)tmp);
          break;
        default:
          printf(" ");
        }
      }
      printf("\n");
    }
  }
}

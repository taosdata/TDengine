// compile with:
// gcc -o query_example query_example.c -ltaos
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
#include <assert.h>

typedef int16_t VarDataLenT;  // maxVarDataLen: 32767

#define TSDB_NCHAR_SIZE sizeof(int32_t)
#define VARSTR_HEADER_SIZE sizeof(VarDataLenT)

#define GET_FLOAT_VAL(x) (*(float *)(x))
#define GET_DOUBLE_VAL(x) (*(double *)(x))
#define SET_FLOAT_VAL(x, y) \
  { (*(float *)(x)) = (float)(y); }
#define SET_DOUBLE_VAL(x, y) \
  { (*(double *)(x)) = (double)(y); }
#define SET_FLOAT_PTR(x, y) \
  { (*(float *)(x)) = (*(float *)(y)); }
#define SET_DOUBLE_PTR(x, y) \
  { (*(double *)(x)) = (*(double *)(y)); }


#define varDataLen(v) ((VarDataLenT *)(v))[0]
#define varDataTLen(v) (sizeof(VarDataLenT) + varDataLen(v))
#define varDataVal(v) ((void *)((char *)v + VARSTR_HEADER_SIZE))
#define varDataCopy(dst, v) memcpy((dst), (void *)(v), varDataTLen(v))
#define varDataLenByData(v) (*(VarDataLenT *)(((char *)(v)) - VARSTR_HEADER_SIZE))
#define varDataSetLen(v, _len) (((VarDataLenT *)(v))[0] = (VarDataLenT)(_len))
#define IS_VAR_DATA_TYPE(t) (((t) == TSDB_DATA_TYPE_BINARY) || ((t) == TSDB_DATA_TYPE_NCHAR))

#define varDataNetLen(v) (htons(((VarDataLenT *)(v))[0]))
#define varDataNetTLen(v) (sizeof(VarDataLenT) + varDataNetLen(v))

int printRow(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields) {
  int  len = 0;
  char split = ' ';

  for (int i = 0; i < num_fields; ++i) {
    if (i > 0) {
      str[len++] = split;
    }

    if (row[i] == NULL) {
      len += sprintf(str + len, "%s", "NULL");
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_TINYINT:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UTINYINT:
        len += sprintf(str + len, "%u", *((uint8_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_SMALLINT:
        len += sprintf(str + len, "%d", *((int16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_USMALLINT:
        len += sprintf(str + len, "%u", *((uint16_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_INT:
        len += sprintf(str + len, "%d", *((int32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UINT:
        len += sprintf(str + len, "%u", *((uint32_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BIGINT:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_UBIGINT:
        len += sprintf(str + len, "%" PRIu64, *((uint64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_FLOAT: {
        float fv = 0;
        fv = GET_FLOAT_VAL(row[i]);
        len += sprintf(str + len, "%f", fv);
      } break;

      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = 0;
        dv = GET_DOUBLE_VAL(row[i]);
        len += sprintf(str + len, "%lf", dv);
      } break;

      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_NCHAR: {
        int32_t charLen = varDataLen((char *)row[i] - VARSTR_HEADER_SIZE);
        if (fields[i].type == TSDB_DATA_TYPE_BINARY) {
          assert(charLen <= fields[i].bytes && charLen >= 0);
        } else {
          assert(charLen <= fields[i].bytes * TSDB_NCHAR_SIZE && charLen >= 0);
        }
        // copy content
        memcpy(str + len, row[i], charLen);
        len += charLen;
      } break;

      case TSDB_DATA_TYPE_TIMESTAMP:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;

      case TSDB_DATA_TYPE_BOOL:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
      default:
        break;
    }
  }

  return len;
}

static int printResult(TAOS_RES *res) {
  TAOS_ROW    row = NULL;
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);
  int         nRows = 0;

  while ((row = taos_fetch_row(res))) {
    char temp[256] = {0};
    printRow(temp, row, fields, num_fields);
    puts(temp);
    nRows++;
  }
}

int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "power", 6030);
  if (taos == NULL) {
    puts("failed to connect to server");
    exit(EXIT_FAILURE);
  }
  TAOS_RES *res = taos_query(taos, "SELECT * FROM meters LIMIT 2");
  if (taos_errno(res) != 0) {
    printf("failed to exeuce taos_query. error: %s\n", taos_errstr(res));
    exit(EXIT_FAILURE);
  }
  printResult(res);
  taos_free_result(res);
  taos_close(taos);
  taos_cleanup();
}
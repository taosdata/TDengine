// compile with:
// gcc -o query_example query_example.c -ltaos
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

typedef uint16_t VarDataLenT;

#define TSDB_NCHAR_SIZE sizeof(int32_t)
#define VARSTR_HEADER_SIZE sizeof(VarDataLenT)

#define GET_FLOAT_VAL(x) (*(float *)(x))
#define GET_DOUBLE_VAL(x) (*(double *)(x))

#define varDataLen(v) ((VarDataLenT *)(v))[0]

int printRow(char *str, TAOS_ROW row, TAOS_FIELD *fields, int numFields) {
  int  len = 0;
  char split = ' ';

  for (int i = 0; i < numFields; ++i) {
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
      case TSDB_DATA_TYPE_VARBINARY:
      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_GEOMETRY: {
        int32_t charLen = varDataLen((char *)row[i] - VARSTR_HEADER_SIZE);
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

/**
 * @brief print column name and values of each row
 *
 * @param res
 * @return int
 */
static int printResult(TAOS_RES *res) {
  int         numFields = taos_num_fields(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);
  char        header[256] = {0};
  int len = 0;
  for (int i = 0; i < numFields; ++i) {
    len += sprintf(header + len, "%s ", fields[i].name);
  }
  puts(header);

  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(res))) {
    char temp[256] = {0};
    printRow(temp, row, fields, numFields);
    puts(temp);
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
    printf("failed to execute taos_query. error: %s\n", taos_errstr(res));
    exit(EXIT_FAILURE);
  }
  printResult(res);
  taos_free_result(res);
  taos_close(taos);
  taos_cleanup();
}

// output:
// ts current voltage phase location groupid 
// 1648432611249 10.300000 219 0.310000 California.SanFrancisco 2
// 1648432611749 12.600000 218 0.330000 California.SanFrancisco 2
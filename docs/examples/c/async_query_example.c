// compile with:
// gcc -o async_query_example async_query_example.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
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

void printHeader(TAOS_RES *res) {
  int         numFields = taos_num_fields(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);
  char        header[256] = {0};
  int         len = 0;
  for (int i = 0; i < numFields; ++i) {
    len += sprintf(header + len, "%s ", fields[i].name);
  }
  puts(header);
}

// ANCHOR: demo

/**
 * @brief call back function of taos_fetch_row_a
 *
 * @param param : the third parameter you passed to taos_fetch_row_a
 * @param res : pointer of TAOS_RES
 * @param numOfRow : number of rows fetched in this batch. will be 0 if there is no more data.
 * @return void*
 */
void *fetch_row_callback(void *param, TAOS_RES *res, int numOfRow) {
  printf("numOfRow = %d \n", numOfRow);
  int         numFields = taos_num_fields(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);
  TAOS       *_taos = (TAOS *)param;
  if (numOfRow > 0) {
    for (int i = 0; i < numOfRow; ++i) {
      TAOS_ROW row = taos_fetch_row(res);
      char     temp[256] = {0};
      printRow(temp, row, fields, numFields);
      puts(temp);
    }
    taos_fetch_rows_a(res, fetch_row_callback, _taos);
  } else {
    printf("no more data, close the connection.\n");
    taos_free_result(res);
    taos_close(_taos);
    taos_cleanup();
  }
}

/**
 * @brief callback function of taos_query_a
 *
 * @param param: the fourth parameter you passed to taos_query_a
 * @param res : the result set
 * @param code : status code
 * @return void*
 */
void *select_callback(void *param, TAOS_RES *res, int code) {
  printf("query callback ...\n");
  TAOS *_taos = (TAOS *)param;
  if (code == 0 && res) {
    printHeader(res);
    taos_fetch_rows_a(res, fetch_row_callback, _taos);
  } else {
    printf("failed to execute taos_query. error: %s\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(_taos);
    taos_cleanup();
    exit(EXIT_FAILURE);
  }
}

int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "power", 6030);
  if (taos == NULL) {
    puts("failed to connect to server");
    exit(EXIT_FAILURE);
  }
  // param one is the connection returned by taos_connect.
  // param two is the SQL to execute.
  // param three is the callback function.
  // param four can be any pointer. It will be passed to your callback function as the first parameter. we use taos
  // here, because we want to close it after getting data.
  taos_query_a(taos, "SELECT * FROM meters", select_callback, taos);
  sleep(1);
}

// output:
// query callback ...
// ts current voltage phase location groupid
// numOfRow = 8
// 1538548685500 11.800000 221 0.280000 california.losangeles 2
// 1538548696600 13.400000 223 0.290000 california.losangeles 2
// 1538548685000 10.800000 223 0.290000 california.losangeles 3
// 1538548686500 11.500000 221 0.350000 california.losangeles 3
// 1538548685000 10.300000 219 0.310000 california.sanfrancisco 2
// 1538548695000 12.600000 218 0.330000 california.sanfrancisco 2
// 1538548696800 12.300000 221 0.310000 california.sanfrancisco 2
// 1538548696650 10.300000 218 0.250000 california.sanfrancisco 3
// numOfRow = 0
// no more data, close the connection.
// ANCHOR_END: demo
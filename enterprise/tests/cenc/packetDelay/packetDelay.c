#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <sys/time.h>
#include "taos.h"
#include "libmseed.h"


#define  MAX_TSQL_LEN  65535


int nTotalRows = 0;
int nTotalSamples = 0;


static signed char index_64[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55,
    56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
    13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32,
    33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1};

#define  CHAR64(c)     (((c) < 0 || (c) > 127) ? -1 : index_64[(c)])


typedef struct callback_params_s {
  TAOS  *res_taos;
  void  *data;
} callback_params_t;


// base64 decode
unsigned char *base64_decode(const char *value, int inlen, int *outlen) {
  int            c1, c2, c3, c4;
  unsigned char *result = (unsigned char *)taosMemoryMalloc((size_t)(inlen * 3) / 4 + 1);
  unsigned char *out = result;

  *outlen = 0;

  while (1) {
    if (value[0] == 0) {
      *out = '\0';
      return result;
    }

    // skip \r\n
    if (value[0] == '\n' || value[0] == '\r') {
      value += 1;
      continue;
    }

    c1 = value[0];
    if (CHAR64(c1) == -1) goto base64_decode_error;
    c2 = value[1];
    if (CHAR64(c2) == -1) goto base64_decode_error;
    c3 = value[2];
    if ((c3 != '=') && (CHAR64(c3) == -1)) goto base64_decode_error;
    c4 = value[3];
    if ((c4 != '=') && (CHAR64(c4) == -1)) goto base64_decode_error;

    value += 4;
    *out++ = (unsigned char)((CHAR64(c1) << 2) | (CHAR64(c2) >> 4));
    *outlen += 1;
    if (c3 != '=') {
      *out++ = (unsigned char)(((CHAR64(c2) << 4) & 0xf0) | (CHAR64(c3) >> 2));
      *outlen += 1;
      if (c4 != '=') {
        *out++ = (unsigned char)(((CHAR64(c3) << 6) & 0xc0) | CHAR64(c4));
        *outlen += 1;
      }
    }
  }

base64_decode_error:
  taosMemoryFree(result);
  result = 0;
  *outlen = 0;

  return result;
}


int check_and_free_res(TAOS_RES **res, const char *cmd) {
    int code = 0;

    if (*res == NULL) {
        fprintf(stderr, "NULL res\r\n");
        code = -1;
    } else {
        if (taos_errno(*res) != 0) {
            fprintf(stderr, "failed to execute: \"%s\", reason: %s\r\n", cmd, taos_errstr(*res));
            code = -2;
        }

        taos_free_result(*res);
        *res = NULL;
    }

    return code;
}


void cenc_calc_delay(MS3TraceList *mstl, callback_params_t *param)
{
  int                    np;
  char                  *cp;
  int64_t                start_time, now;
  int64_t                numsamples;
  int                    samprate;
  MS3TraceID            *id;
  MS3TraceSeg           *seg;
  nstime_t               dtime;
  char                   sid[LM_SIDLEN];
  char                   net[LM_SIDLEN], stat[LM_SIDLEN], loc[LM_SIDLEN], chan[LM_SIDLEN];
  callback_params_t     *p;
  TAOS_RES              *res = NULL;
  char                  *stb_name;
  char                   cmd[MAX_TSQL_LEN];
  struct timeval         tv;

  p = (callback_params_t *) param;
  if (p == NULL) {
    return;
  }

  stb_name = (char *) p->data;

  numsamples = 0;

  id = mstl->traces;

  while (id) {
    seg = id->first;

    memset(sid, 0, LM_SIDLEN);
    memcpy(sid, id->sid, strlen(id->sid));

    start_time = (int64_t) round(id->earliest * 0.001 * 0.001);

    memset(net, 0, LM_SIDLEN);
    memset(stat, 0, LM_SIDLEN);
    memset(loc, 0, LM_SIDLEN);
    memset(chan, 0, LM_SIDLEN);

    if (ms_sid2nslc(sid, net, stat, loc, chan)) {
      fprintf(stderr, "failed to parse sid: %s\r\n", sid);
      break;
    }

    np = snprintf(cmd, sizeof(cmd),
                  "insert into %s_%s_%s_%s using %s tags ('%s', '%s', '%s', '%s') values ",
                  net, stat, loc, chan, stb_name, net, stat, loc, chan);
    if (np <= 0) {
      fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
      break;
    }

    while (seg) {
      if (seg->numsamples <= 0) {
        seg = seg->next;
        continue;
      }

      samprate = (int) seg->samprate;
      dtime = (int) round(1000.0 / seg->samprate);

      numsamples += seg->numsamples;
      nTotalSamples += seg->numsamples;

      seg = seg->next;
    }

    if (numsamples > 0) {
      gettimeofday(&tv, NULL);
      now = tv.tv_sec * 1000 + tv.tv_usec / 1000;

      cp = cmd + np;
      np = snprintf(cp, cmd + MAX_TSQL_LEN - cp,
                    "(%ld, %d, %ld, %ld, %ld, %d)",
                    now, (int) ((now - start_time) / 1000), start_time, start_time + numsamples * dtime, numsamples, samprate);

      cp[np] = ';';
      cp[++np] = '\0';

      res = taos_query(p->res_taos, cmd);
      check_and_free_res(&res, cmd);
    }

    id = id->next;
  }
}


void subscribe_callback(TAOS_SUB *tsub, TAOS_RES *res, void *param, int code)
{
  MS3TraceList      *mstl              = NULL;
  TAOS_FIELD        *fields            = NULL;
  unsigned char     *p;
  int                records           = 0;
  int                nRows             = 0;
  int                i, len, nfields;
  uint32_t           flags             = 0;
  TAOS_ROW           row               = NULL;

  /* Set bit flags to validate CRC and unpack data samples */
  flags |= MSF_VALIDATECRC;
  flags |= MSF_UNPACKDATA;

  while ((row = taos_fetch_row(res))) {
    fields = taos_fetch_fields(res);
    nfields = taos_num_fields(res);

    nRows++;

    /* TODO: no iteration or free to improve performance */
    for (i = 0; i < nfields; i++) {
      if (strncasecmp(fields[i].name, "content", sizeof("content") - 1) == 0 &&
          fields[i].type == TSDB_DATA_TYPE_BINARY)
      {
        p = base64_decode((const char *) row[i], strlen((char *) row[i]), &len);
        if (p == NULL) {
          fprintf(stderr, "base64_decode error\r\n");
          continue;
        }

        mstl = mstl3_init(NULL);
        if (mstl == NULL) {
          fprintf(stderr, "error allocating MS3TraceList\r\n");
          continue;
        }

        records = mstl3_readbuffer(&mstl, (char *) p, len, 0, flags, NULL, 0);
        if (records < 0) {
          fprintf(stderr, "mstl3_readbuffer error\r\n");
          continue;
        }

        cenc_calc_delay(mstl, param);

        taosMemoryFree(p);
        mstl3_free(&mstl, 0);
      }
    }
  }

  nTotalRows += nRows;
  fprintf(stderr, "end time: %ld\r\n", time(NULL));
  fprintf(stderr, "%d rows consumed.\r\n", nRows);
}


int main(int argc, char *argv[]) {
  int                 np;
  const char         *host      = "localhost";
  const char         *user      = "root";
  const char         *passwd    = "taosdata";
  const char         *port      = "6030";
  const char         *sql       = "select * from ps;";
  const char         *topic     = "packet";
  const char         *delay     = "delay";
  const char         *stb_name  = "ms";
  TAOS               *taos      = NULL;
  TAOS               *res_taos  = NULL;
  TAOS_RES           *res       = NULL;
  TAOS_SUB           *tsub      = NULL;
  char                cmd[MAX_TSQL_LEN];
  int                 i;
  int                 async     = 1;
  int                 restart   = 0;
  int                 keep      = 1;
  callback_params_t   params;

  for (i = 1; i < argc; i++) {
    if (strncmp(argv[i], "-h=", 3) == 0) {
      host = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-u=", 3) == 0) {
      user = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-p=", 3) == 0) {
      passwd = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-P=", 3) == 0) {
      port = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-t=", 3) == 0) {
      topic = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-d=", 3) == 0) {
      delay = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-s=", 3) == 0) {
      stb_name = argv[i] + 3;
      continue;
    }

    if (strcmp(argv[i], "-help") == 0) {
      fprintf(stderr,
              "Usage: %s[ -h=host -u=user -p=password -P=port "
              "-t=topic -d=result_db_name -s=result_stb_name "
              "-sync -restart -nokeep -help]\r\n", argv[0]);

      exit(0);
    }

    if (strcmp(argv[i], "-sync") == 0) {
      async = 0;
      continue;
    }

    if (strcmp(argv[i], "-restart") == 0) {
      restart = 1;
      continue;
    }

    if (strcmp(argv[i], "-nokeep") == 0) {
      keep = 0;
      continue;
    }
  }

  fprintf(stderr, "################################################################\r\n");
  fprintf(stderr, "# Server:                          %s\r\n", host);
  fprintf(stderr, "# User:                            %s\r\n", user);
  fprintf(stderr, "# Port:                            %s\r\n", port);
  fprintf(stderr, "# Topic:                           %s\r\n", topic);
  fprintf(stderr, "# Delay Database Name:             %s\r\n", delay);
  fprintf(stderr, "# Delay Super Table Name:          %s\r\n", stb_name);
  fprintf(stderr, "# Async:                           %d\r\n", async);
  fprintf(stderr, "# Restart:                         %d\r\n", restart);
  fprintf(stderr, "# Keep:                            %d\r\n", keep);
  fprintf(stderr, "################################################################\r\n");

  usleep(500000);

  // init TAOS
  taos_init();

  taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    fprintf(stderr, "failed to connect to db, reason:%s\r\n", taos_errstr(taos));
    goto failed;
  }

  // create topic
  np = snprintf(cmd, sizeof(cmd), "create topic if not exists %s partitions 4;", topic);
  if (np <= 0) {
    fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
    goto failed;
  }

  cmd[np] = '\0';

  res = taos_query(taos, cmd);
  if (check_and_free_res(&res, cmd) != 0) {
    goto failed;
  }

  res_taos = taos_connect(host, user, passwd, "", 0);
  if (res_taos == NULL) {
    fprintf(stderr, "failed to connect to delay database, reason:%s\r\n", taos_errstr(taos));
    goto failed;
  }

  // create databse for delay
  np = snprintf(cmd, sizeof(cmd), "create database if not exists %s;", delay);
  if (np <= 0) {
    fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
    goto failed;
  }

  cmd[np] = '\0';

  res = taos_query(taos, cmd);
  if (check_and_free_res(&res, cmd) != 0) {
    goto failed;
  }

  params.res_taos = res_taos;
  params.data = (void *) stb_name;

  taos_select_db(taos, topic);
  taos_select_db(res_taos, delay);

  // create super table for delay
  np = snprintf(cmd, sizeof(cmd),
                "create stable if not exists %s (ingesttime timestamp, delay int, starttime timestamp, endtime timestamp, npts int, samprate int) "
                "tags (network binary(20), station binary(20), location binary(20), channel binary(20));",
                stb_name);

  if (np <= 0) {
    fprintf(stderr, "fnprintf error cmd: %s\r\n", cmd);
    goto failed;
  }

  cmd[np] = '\0';

  res = taos_query(res_taos, cmd);
  if (check_and_free_res(&res, cmd) != 0) {
    goto failed;
  }

  fprintf(stderr, "start time: %ld\r\n", time(NULL));

  if (async) {
    // create an asynchronized subscription, the callback function will be called every 1s
    tsub = taos_subscribe(taos, restart, topic, sql, subscribe_callback, &params, 1000);
  } else {
    // create an synchronized subscription, need to call 'taos_consume' manually
    tsub = taos_subscribe(taos, restart, topic, sql, NULL, NULL, 0);
  }

  if (tsub == NULL) {
    fprintf(stderr, "failed to create subscription.\r\n");
    taos_close(taos);
    exit(0);
  }

  if (async) {
    getchar();
  } else while (1) {
    TAOS_RES *res = taos_consume(tsub);
    if (res == NULL) {
      fprintf(stderr, "failed to consume data.");
      break;
    } else {
      getchar();
    }
  }

  fprintf(stderr, "total samples consumed: %d\r\n", nTotalSamples);
  fprintf(stderr, "total rows consumed: %d\r\n", nTotalRows);
  taos_unsubscribe(tsub, keep);

failed:
  if (taos) {
    taos_close(taos);
  }

  if (res_taos) {
    taos_close(res_taos);
  }

  return 0;
}

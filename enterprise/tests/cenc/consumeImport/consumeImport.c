#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <signal.h>
#include "taos.h"
#include "libmseed.h"


#define  MAX_TSQL_LEN  1048576
#define  SEG_TSQL_LEN  65536
#define  TQ_CHAN_NUM   10
#define  MAX_DB_ROWS   32767


int nTotalRows = 0;
time_t nTotalTime = 0;
int nTotalSamples = 0;


pthread_mutex_t  mutex;
pthread_mutex_t  mutex_trows;
pthread_mutex_t  mutex_tsamps;
pthread_mutex_t  mutex_ttime;


int                 run       = 1;
int                 async     = 0;
int                 restart   = 0;
int                 keep      = 1;
const char         *host      = "localhost";
const char         *user      = "root";
const char         *passwd    = "taosdata";
const char         *port      = "6030";
const char         *topic     = "packet";
const char         *result    = "detail";
const char         *stb_name  = "ms";


static signed char index_64[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55,
    56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
    13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32,
    33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1};

#define  CHAR64(c)     (((c) < 0 || (c) > 127) ? -1 : index_64[(c)])


typedef struct callback_params_s {
  int        index;
  TAOS      *taos;
  TAOS      *res_taos;
  TAOS_SUB  *tsub;
  char       cmd[MAX_TSQL_LEN];
  int        rows;
  off_t      offset;
  void      *data;
} callback_params_t;


// base64 decode
unsigned char *base64_decode(const char *value, int inlen, int *outlen) {
  int            c1, c2, c3, c4;
  unsigned char *result = (unsigned char *)malloc((size_t)(inlen * 3) / 4 + 1);
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
      } else {
        *out = '\0';
        return result;
      }
    } else {
      *out = '\0';
      return result;
    }
  }

base64_decode_error:
  free(result);
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


void cenc_import_detail(MS3TraceList *mstl, callback_params_t *param)
{
  int                    i, np, rows;
  int                    index;
  char                  *cp;
  int64_t                start_time;
  int32_t               *samples;
  int                    numsamples;
  const char            *prefix = "insert into";
  const int              prefix_len = sizeof("insert into") - 1;
  MS3TraceID            *id;
  MS3TraceSeg           *seg;
  nstime_t               dtime;
  char                   sid[LM_SIDLEN];
  char                   net[LM_SIDLEN], stat[LM_SIDLEN], loc[LM_SIDLEN], chan[LM_SIDLEN];
  callback_params_t     *p;
  TAOS_RES              *res = NULL;
  char                  *stb_name;
  char                   cmd[SEG_TSQL_LEN];
  time_t                 now;
  int64_t                ts;
  //struct timeval         s_time, e_time;

  p = (callback_params_t *) param;
  if (p == NULL) {
    return;
  }

  index = p->index;
  stb_name = (char *) p->data;

  rows = 0;
  numsamples = 0;

  id = mstl->traces;

  while (id) {
    now = time(NULL);
    ts = (int64_t) (id->earliest * 0.001 * 0.001 * 0.001);

    if ((ts >= now && (ts - now > 315360000)) || (ts < now && (now - ts) > 315360000)) {
      fprintf(stderr, "sub(%d): sid(%s), invalid start time: %ld\r\n", index, id->sid, id->earliest);
      id = id->next;
      continue;
    }

    seg = id->first;
    start_time = (int64_t) round(id->earliest * 0.001 * 0.001);

    memset(sid, 0, LM_SIDLEN);
    memcpy(sid, id->sid, strlen(id->sid));

    memset(net, 0, LM_SIDLEN);
    memset(stat, 0, LM_SIDLEN);
    memset(loc, 0, LM_SIDLEN);
    memset(chan, 0, LM_SIDLEN);
    if (ms_sid2nslc(sid, net, stat, loc, chan)) {
      fprintf(stderr, "sub(%d): failed to parse sid: %s\r\n", index, sid);
      break;
    }

    np = 0;

    if (p->offset == 0) {
      memcpy(cmd, prefix, prefix_len);
      np += prefix_len;
    }

    np += snprintf(cmd + np, sizeof(cmd) - np,
                  " %s_%s_%s_%s using %s tags ('%s', '%s', '%s', '%s') values ",
                  net, stat, loc, chan, stb_name, net, stat, loc, chan);

    if (np <= 0) {
      fprintf(stderr, "sub(%d): fnprintf error cmd: %s\r\n", index, cmd);
      break;
    }

    cmd[np] = '\0';
    cp = cmd + np;

    while (seg) {
      if (seg->numsamples <= 0) {
        seg = seg->next;
        continue;
      }

      samples = (int32_t *) seg->datasamples;
      dtime = (int) round(1000.0 / seg->samprate);

      for (i = 0; i < seg->numsamples; i++) {
        if (i != seg->numsamples - 1) {
          np = snprintf(cp, cmd + SEG_TSQL_LEN - cp, "(%ld, %d) ", start_time + i * dtime, samples[i]);
        } else {
          np = snprintf(cp, cmd + SEG_TSQL_LEN - cp, "(%ld, %d)", start_time + i * dtime, samples[i]);
        }

        if (np <= 0) {
          fprintf(stderr, "sub(%d): fprintf error cmd: %s\r\n", index, cmd);
          return;
        }

        cp += np;
        rows++;
      }

      numsamples += seg->numsamples;

      pthread_mutex_lock(&mutex_tsamps);
      nTotalSamples += seg->numsamples;
      pthread_mutex_unlock(&mutex_tsamps);

      seg = seg->next;
    }

    if (strlen(cmd) > (MAX_TSQL_LEN - p->offset - 1024) || (p->rows + rows) >= MAX_DB_ROWS) {
      p->cmd[p->offset++] = ';';
      p->cmd[p->offset] = '\0';

      p->offset = 0;
      p->rows = 0;

      //gettimeofday(&s_time, NULL);
      res = taos_query(p->res_taos, p->cmd);
      //gettimeofday(&e_time, NULL);
      //printf("insert delta time=%ld\r\n", e_time.tv_sec * 1000000 + e_time.tv_usec - s_time.tv_sec * 1000000 - s_time.tv_usec);
      check_and_free_res(&res, p->cmd);

      if (memcmp(cmd, prefix, prefix_len)) {
        memcpy(p->cmd, prefix, prefix_len);
        p->offset += prefix_len;
      }
    }

    memmove(p->cmd + p->offset, cmd, strlen(cmd));
    p->offset += strlen(cmd);
    p->rows += rows;

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
  struct timeval     start_time, end_time;

  /* Set bit flags to validate CRC and unpack data samples */
  //flags |= MSF_VALIDATECRC;
  flags |= MSF_UNPACKDATA;

  gettimeofday(&start_time, NULL);

  while ((row = taos_fetch_row(res)) && run) {
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
          //fprintf(stderr, "mstl3_readbuffer error, p = %s, len = %d\r\n", (char *) row[i], len);

          mstl3_free(&mstl, 0);
          free(p);

          continue;
        }

        cenc_import_detail(mstl, param);

        mstl3_free(&mstl, 0);
        free(p);
      }
    }
  }

  if (nRows != 0) {
    pthread_mutex_lock(&mutex_trows);
    nTotalRows += nRows;
    pthread_mutex_unlock(&mutex_trows);

    gettimeofday(&end_time, NULL);
    pthread_mutex_lock(&mutex_ttime);
    nTotalTime += (end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec);
    pthread_mutex_unlock(&mutex_ttime);

    fprintf(stderr, "%d rows consumed.\r\n", nRows);
  }
}


void subscribe_routine_init(callback_params_t *params, const int index)
{
  if (params) {
    params->index = index;
    params->rows = 0;
    params->offset = 0;
    params->taos = NULL;
    params->res_taos = NULL;
    params->tsub = NULL;
  }
}


void *subscribe_routine(void *arg)
{
  int                 np;
  int                 index;
  char                sql[SEG_TSQL_LEN];
  char                cmd[SEG_TSQL_LEN];
  char                topic_name[SEG_TSQL_LEN];
  TAOS_RES           *res;
  callback_params_t  *pps;

  if (arg == NULL) {
    return NULL;
  }

  pps = (callback_params_t *) arg;

  index = pps->index;

  fprintf(stdout, "sub(%d) created\r\n", index);

  // init TAOS
  taos_init();

  pps->taos = taos_connect(host, user, passwd, "", 0);
  if (pps->taos == NULL) {
    fprintf(stderr, "sub(%d): failed to connect to db\r\n", index);
    goto failed;
  }

  // create topic
  np = snprintf(cmd, sizeof(cmd), "create topic if not exists %s partitions %d;", topic, TQ_CHAN_NUM);
  if (np <= 0) {
    fprintf(stderr, "sub(%d): fnprintf error cmd: %s\r\n", index, cmd);
    goto failed;
  }

  cmd[np] = '\0';

  pthread_mutex_lock(&mutex);
  res = taos_query(pps->taos, cmd);
  pthread_mutex_unlock(&mutex);
  if (check_and_free_res(&res, cmd) != 0) {
    goto failed;
  }

  pps->res_taos = taos_connect(host, user, passwd, "", 0);
  if (pps->res_taos == NULL) {
    fprintf(stderr, "sub(%d): failed to connect to result database\r\n", index);
    goto failed;
  }

  // create databse for result
  np = snprintf(cmd, sizeof(cmd), "create database if not exists %s;", result);
  if (np <= 0) {
    fprintf(stderr, "sub(%d): fnprintf error cmd: %s\r\n", index, cmd);
    goto failed;
  }

  cmd[np] = '\0';

  pthread_mutex_lock(&mutex);
  res = taos_query(pps->taos, cmd);
  pthread_mutex_unlock(&mutex);
  if (check_and_free_res(&res, cmd) != 0) {
    goto failed;
  }

  pps->data = (void *) stb_name;

  taos_select_db(pps->taos, topic);
  taos_select_db(pps->res_taos, result);

  // create super table for result
  np = snprintf(cmd, sizeof(cmd),
                "create stable if not exists %s (ts timestamp, data int) "
                "tags (network binary(20), station binary(20), location binary(20), channel binary(20));",
                stb_name);

  if (np <= 0) {
    fprintf(stderr, "sub(%d): fnprintf error cmd: %s\r\n", index, cmd);
    goto failed;
  }

  cmd[np] = '\0';

  pthread_mutex_lock(&mutex);
  res = taos_query(pps->res_taos, cmd);
  pthread_mutex_unlock(&mutex);
  if (check_and_free_res(&res, cmd) != 0) {
    goto failed;
  }

  memset(sql, 0, SEG_TSQL_LEN);
  memset(topic_name, 0, SEG_TSQL_LEN);

  snprintf(sql, SEG_TSQL_LEN, "select * from p%d;", index);
  snprintf(topic_name, SEG_TSQL_LEN, "%s%d;", topic, index);

  if (async) {
    // create an asynchronized subscription, the callback function will be called every 1s
    pps->tsub = taos_subscribe(pps->taos, restart, topic, sql, subscribe_callback, pps, 1000);
  } else {
    // create an synchronized subscription, need to call 'taos_consume' manually
    pps->tsub = taos_subscribe(pps->taos, restart, topic, sql, NULL, NULL, 0);
  }

  if (pps->tsub == NULL) {
    fprintf(stderr, "sub(%d): failed to create subscription.\r\n", index);
    goto failed;
  }

  if (async) {
    return NULL;
  } else {
    while (run) {
      res = taos_consume(pps->tsub);
      if (res == NULL) {
        fprintf(stderr, "sub(%d): failed to consume data.\r\n", index);
        break;
      } else {
        subscribe_callback(pps->tsub, res, pps, 0);
      }
    }
  }

  fprintf(stdout, "sub(%d) quit\r\n", index);

  if (pps->offset > 0) {
    pps->cmd[pps->offset++] = ';';
    pps->cmd[pps->offset] = '\0';
    pps->offset = 0;

    res = taos_query(pps->res_taos, pps->cmd);
    check_and_free_res(&res, pps->cmd);
  }

failed:
  if (pps->taos) {
    taos_close(pps->taos);
    pps->taos = NULL;
  }

  if (pps->res_taos) {
    taos_close(pps->res_taos);
    pps->res_taos = NULL;
  }

  return NULL;
}


void subscribe_routine_finalize(callback_params_t *params)
{
  if (params) {
    if (params->taos) {
      taos_close(params->taos);
    }

    if (params->res_taos) {
      taos_close(params->res_taos);
    }

    if (params->tsub) {
      taos_unsubscribe(params->tsub, keep);
    }
  }
}


pthread_t           t[TQ_CHAN_NUM];
callback_params_t   pp[TQ_CHAN_NUM];


void handler(int sig) {
    run = 0;
}


int main(int argc, char **argv)
{
  int                 i;
  struct sigaction    act;

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

    if (strncmp(argv[i], "-f=", 3) == 0) {
      result = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-d=", 3) == 0) {
      result = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-s=", 3) == 0) {
      stb_name = argv[i] + 3;
      continue;
    }

    if (strcmp(argv[i], "-help") == 0) {
      fprintf(stderr,
              "Usage: %s [ -h=host -u=user -p=password -P=port "
              "-t=topic -d=result_db_name -s=result_stb_name "
              "-async -restart -nokeep -help]\r\n", argv[0]);

      exit(0);
    }

    if (strcmp(argv[i], "-async") == 0) {
      async = 1;
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
  fprintf(stderr, "# Result Database Name:            %s\r\n", result);
  fprintf(stderr, "# Result Super Table Name:         %s\r\n", stb_name);
  fprintf(stderr, "# Async:                           %d\r\n", async);
  fprintf(stderr, "# Restart:                         %d\r\n", restart);
  fprintf(stderr, "# Keep:                            %d\r\n", keep);
  fprintf(stderr, "################################################################\r\n");

  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  sigaction(SIGINT, &act, 0);

  usleep(500000);

  pthread_mutex_init(&mutex, NULL);
  pthread_mutex_init(&mutex_trows, NULL);
  pthread_mutex_init(&mutex_tsamps, NULL);
  pthread_mutex_init(&mutex_ttime, NULL);

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    subscribe_routine_init(&pp[i], i + 1);
    pthread_create(&t[i], NULL, subscribe_routine, (void *) &pp[i]);
  }

  if (async) {
    getchar();
  }

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    pthread_join(t[i], NULL);
    subscribe_routine_finalize(&pp[i]);
  }


  fprintf(stdout, "total samples consumed: %d\r\n", nTotalSamples);
  fprintf(stdout, "total rows consumed: %d\r\n", nTotalRows);
  fprintf(stdout, "total time consumed: %ld\r\n", nTotalTime / TQ_CHAN_NUM);

  return 0;
}

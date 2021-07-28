#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <signal.h>
#include <errno.h>
#include "taos.h"
#include "base64.h"
#include "libmseed.h"


#define  MAX_TSQL_LEN  1048576
#define  SEG_TSQL_LEN  65536
#define  TQ_CHAN_NUM   16
#define  MAX_DB_ROWS   32767


int64_t nTotalRows = 0;
time_t nTotalTime = 0;
int64_t nTotalSamples = 0;


pthread_mutex_t     mutex;
pthread_spinlock_t  lock_trows;
pthread_spinlock_t  lock_tsamps;
pthread_spinlock_t  lock_ttime;


int                 run        = 1;
int                 quit       = 0;
int                 async      = 0;
int                 restart    = 0;
int                 keep       = 1;
int                 seed_only  = 1;
const char         *src_host   = "localhost";
const char         *dst_host   = "localhost";
const char         *src_user   = "root";
const char         *dst_user   = "root";
const char         *src_passwd = "taosdata";
const char         *dst_passwd = "taosdata";
const char         *src_port   = "6030";
const char         *dst_port   = "6030";
const char         *topic      = "packet";
const char         *result     = "detail";
const char         *stb_name   = "ms";
const char         *retry      = "0";


#if defined(CONSUME_DEBUG)
typedef struct timestamp_s
{
  struct timeval  stime;
  struct timeval  etime;
  int64_t         start;
  int64_t         end;
  int64_t         delta;
} timestamp_t;


#define  F_DATA_TAB_MOD  5000
#define  hash(key, c)    ((uint64_t) (key) * 31 + (c))


typedef struct f_data_s f_data_t;

struct f_data_s
{
  char             sid[LM_SIDLEN];
  int64_t          count1;
  int64_t          ts;
  int64_t          count2;
  int64_t          count3;
  f_data_t        *next;
};


pthread_spinlock_t  lock_f_data1;
pthread_spinlock_t  lock_f_data2;
pthread_spinlock_t  lock_f_data3;


uint64_t hash_key(const char *data, size_t len)
{
  uint64_t  i, key;

  key = 0;

  for (i = 0; i < len; i++) {
    key = hash(key, data[i]);
  }

  return key;
}


f_data_t **get_hash_item(const char *sid, f_data_t **htb)
{
  int          index;
  uint64_t     hash;
  size_t       slen, dlen;
  f_data_t   **t;

  hash = hash_key(sid, strlen(sid));
  index = hash % F_DATA_TAB_MOD;

  for (t = &htb[index]; *t; t = &(*t)->next) {
    slen = strlen(sid);
    dlen = strlen((*t)->sid);
    if (slen == dlen && strncasecmp((*t)->sid, sid, slen) == 0) {
      break;
    }
  }

  if (*t == NULL) {
    *t = (f_data_t *) malloc(sizeof(f_data_t));
    if (*t) {
      memset(*t, 0, sizeof(f_data_t));
      strncpy((*t)->sid, sid, strlen(sid));
    }
  }

  return t;
}


void free_hash_table(f_data_t **htb)
{
  int         i;
  f_data_t   *l;
  f_data_t  **t;

  for (i = 0; i < F_DATA_TAB_MOD; i++) {
    if (htb[i]) {
      for (t = &htb[i]; *t; /* void */) {
        l = (*t)->next;
        free(*t);
        *t = NULL;
        t = &l;
      }
    }
  }
}

void *fiter_statistics_routine(void *arg)
{
  int               i;
  time_t            start, end;
  struct timeval    now;
  f_data_t        **t;
  f_data_t        **htb;

  htb = (f_data_t **) arg;

  start = time(NULL);

  while (run) {
    end = time(NULL);

    if (end - start >= 1800) {
      start = time(NULL);

      fprintf(stderr, "============== start ==============\r\n");

      for (i = 0; i < F_DATA_TAB_MOD && run; i++) {
        if (htb[i] && run) {
          gettimeofday(&now, NULL);

          for (t = &htb[i]; *t; t = &(*t)->next) {
            fprintf(stderr, "filtered sid: %s, c1: %12"PRId64", ts: %"PRId64", c2: %12"PRId64", c3: %12"PRId64", now: %"PRId64"\r\n",
                    (*t)->sid, (*t)->count1, (*t)->ts, (*t)->count2, (*t)->count3, now.tv_sec * 1000000 + now.tv_usec);
          }
        }
      }

      fprintf(stderr, "=============== end ===============\r\n");
    } else {
      usleep(100000);
    }
  }

  return NULL;
}
#endif


typedef struct callback_params_s
{
  int              index;
  TAOS            *taos;
  TAOS            *res_taos;
  TAOS_SUB        *tsub;
  char             cmd[MAX_TSQL_LEN];
  int              rows;
  int              ready;
  int              run; /* only for consumer */
  int              retry;
  off_t            offset;
  void            *data;
  pthread_mutex_t  mutex;
  pthread_cond_t   cond;
#if defined(CONSUME_DEBUG)
  timestamp_t      proc;
  timestamp_t      write;
  f_data_t       **htb;
#endif
} callback_params_t;


int check_and_free_res(TAOS_RES **res, const char *cmd, callback_params_t *par)
{
    int code = 0;

    if (*res == NULL) {
        if (par) {
            fprintf(stderr, "thread(%d): NULL res\r\n", par->index);
        } else {
            fprintf(stderr, "NULL res\r\n");
        }
        code = -1;
    } else {
        code = taos_errno(*res);
        if (code != 0) {
            if (par) {
                fprintf(stderr, "thread(%d): failed to execute: \"%s\", reason: %s\r\n",
                        par->index, cmd, taos_errstr(*res));
            } else {
                fprintf(stderr, "failed to execute: \"%s\", reason: %s\r\n", cmd, taos_errstr(*res));
            }
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
  char                  *stb_name;
  char                   cmd[SEG_TSQL_LEN];
  time_t                 now;
  int64_t                ts;
#if defined(CONSUME_DEBUG)
  f_data_t             **t;
#endif

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

    if ((ts > now && (ts - now > 3600)) || (ts < now && (now - ts) > 3600)) {
#if defined(CONSUME_DEBUG)
      pthread_spin_lock(&lock_f_data1);

      t = get_hash_item(id->sid, p->htb);
      if (*t) {
        (*t)->count1++;
        (*t)->ts = (int64_t) (id->earliest * 0.001);
      } else {
        fprintf(stderr, "sub(%d): sid(%s), get_hash_item error, ts: %"PRId64"\r\n", index, id->sid, (int64_t) (id->earliest * 0.001));
      }

      pthread_spin_unlock(&lock_f_data1);
#endif
      //fprintf(stderr, "sub(%d): sid(%s), invalid start time: %"PRId64"\r\n", index, id->sid, id->earliest);
      id = id->next;
      continue;
    }

    seg = id->first;

    while (seg && seed_only) {
      if ((int) seg->samprate == 100) {
        break;
      }
#if defined(CONSUME_DEBUG)
      else {
        pthread_spin_lock(&lock_f_data2);

        t = get_hash_item(id->sid, p->htb);
        if (*t) {
          (*t)->count2++;
        } else {
          fprintf(stderr, "sub(%d): sid(%s), seed only get_hash_item error\r\n", index, id->sid);
        }

        pthread_spin_unlock(&lock_f_data2);
      }
#endif

      seg = seg->next;
    }

    if (seg == NULL) {
      id = id->next;
      continue;
    }

    start_time = (int64_t) round(id->earliest * 0.001);

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
#if defined(CONSUME_DEBUG)
        pthread_spin_lock(&lock_f_data3);

        t = get_hash_item(id->sid, p->htb);
        if (*t) {
          (*t)->count3++;
        } else {
          fprintf(stderr, "sub(%d): sid(%s), numsamples get_hash_item error\r\n", index, id->sid);
        }

        pthread_spin_unlock(&lock_f_data3);
#endif
        seg = seg->next;
        continue;
      }

      samples = (int32_t *) seg->datasamples;
      dtime = (int) round(1000000.0 / seg->samprate);

      for (i = 0; i < seg->numsamples; i++) {
        if (i != seg->numsamples - 1) {
          np = snprintf(cp, cmd + SEG_TSQL_LEN - cp, "(%"PRId64", %d) ", start_time + i * dtime, samples[i]);
        } else {
          np = snprintf(cp, cmd + SEG_TSQL_LEN - cp, "(%"PRId64", %d)", start_time + i * dtime, samples[i]);
        }

        if (np <= 0) {
          fprintf(stderr, "sub(%d): fprintf error cmd: %s\r\n", index, cmd);
          return;
        }

        cp += np;
        rows++;
      }

      numsamples += seg->numsamples;

      pthread_spin_lock(&lock_tsamps);
      nTotalSamples += seg->numsamples;
      pthread_spin_unlock(&lock_tsamps);

      seg = seg->next;
    }

    if (numsamples == 0) {
      id = id->next;
      continue;
    }

#if defined(CONSUME_DEBUG)
      gettimeofday(&p->proc.etime, NULL);
      p->proc.end = p->proc.etime.tv_sec * 1000000 + p->proc.etime.tv_usec;
      p->proc.delta += p->proc.end - p->proc.start;
#endif

    if (strlen(cmd) > (MAX_TSQL_LEN - p->offset - 1024) || (p->rows + rows) >= MAX_DB_ROWS) {
      p->cmd[p->offset++] = ';';
      p->cmd[p->offset] = '\0';

#if defined(CONSUME_DEBUG)
      if (p->proc.delta > 1000000) {
        fprintf(stderr, "sub(%d) proc data, delta: %"PRId64", row(s): %d\r\n", index, p->proc.delta, p->rows);
      }

      p->proc.delta = 0;

      gettimeofday(&p->write.stime, NULL);
      p->write.start = p->write.stime.tv_sec * 1000000 + p->write.stime.tv_usec;
#endif

      pthread_mutex_lock(&p->mutex);

      p->ready = 1;
      pthread_cond_signal(&p->cond);

      while (p->ready) {
        pthread_cond_wait(&p->cond, &p->mutex);
      }

      p->offset = 0;

      if (memcmp(cmd, prefix, prefix_len)) {
        memcpy(p->cmd, prefix, prefix_len);
        p->offset += prefix_len;
      }

      pthread_mutex_unlock(&p->mutex);

#if defined(CONSUME_DEBUG)
      gettimeofday(&p->write.etime, NULL);
      p->write.end = p->write.etime.tv_sec * 1000000 + p->write.etime.tv_usec;
      if (p->write.end - p->write.start > 1000000) {
          fprintf(stderr, "sub(%d) write data, start: %"PRId64", end: %"PRId64", delta: %"PRId64", row(s): %d\r\n",
                  index, p->write.start, p->write.end, p->write.end - p->write.start, p->rows);
      }
#endif

      p->rows = 0;
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
  int64_t            stime, etime;
  callback_params_t *par;

  /* Set bit flags to validate CRC and unpack data samples */
  //flags |= MSF_VALIDATECRC;
  flags |= MSF_UNPACKDATA;
  par = (callback_params_t *) param;

  gettimeofday(&start_time, NULL);

  while ((row = taos_fetch_row(res)) && run) {
    fields = taos_fetch_fields(res);
    nfields = taos_num_fields(res);

    nRows++;

#if defined(CONSUME_DEBUG)
    gettimeofday(&par->proc.stime, NULL);
    par->proc.start = par->proc.stime.tv_sec * 1000000 + par->proc.stime.tv_usec;
#endif

    /* TODO: no iteration or free to improve performance */
    for (i = 0; i < nfields; i++) {
      if (strncasecmp(fields[i].name, "content", sizeof("content") - 1) == 0 &&
          fields[i].type == TSDB_DATA_TYPE_BINARY)
      {
        p = base64_decode((const char *) row[i], strlen((char *) row[i]), &len);
        if (p == NULL) {
          fprintf(stderr, "sub(%d): base64_decode error\r\n", par->index);
          continue;
        }

        mstl = mstl3_init(NULL);
        if (mstl == NULL) {
          fprintf(stderr, "sub(%d): error allocating MS3TraceList\r\n", par->index);
          free(p);
          continue;
        }

        records = mstl3_readbuffer(&mstl, (char *) p, len, 0, flags, NULL, 0);
        if (records < 0) {
          //fprintf(stderr, "sub(%d): mstl3_readbuffer error, p = %s, len = %d\r\n", par->index, (char *) row[i], len);

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
    pthread_spin_lock(&lock_trows);
    nTotalRows += nRows;
    pthread_spin_unlock(&lock_trows);

    gettimeofday(&end_time, NULL);
    pthread_spin_lock(&lock_ttime);
    etime = end_time.tv_sec * 1000000 + end_time.tv_usec;
    stime = start_time.tv_sec * 1000000 + start_time.tv_usec;
    nTotalTime += etime - stime;
    pthread_spin_unlock(&lock_ttime);

    fprintf(stdout, "sub(%d): %d rows consumed, now: %"PRId64"\r\n", par->index, nRows, etime);
  }
}


void subscribe_routine_init(callback_params_t *params, const int index,
#if defined(CONSUME_DEBUG)
  f_data_t **f_data,
#endif
  const int retry)
{
  if (params) {
    params->index = index;
    params->rows = 0;
    params->offset = 0;
    params->taos = NULL;
    params->res_taos = NULL;
    params->tsub = NULL;
    params->retry = retry;
    params->run = 1;
    pthread_cond_init(&params->cond, NULL);
    pthread_mutex_init(&params->mutex, NULL);
#if defined(CONSUME_DEBUG)
    params->htb = f_data;
    memset(&params->proc, 0, sizeof(timestamp_t));
    memset(&params->write, 0, sizeof(timestamp_t));
#endif
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

  pps->taos = taos_connect(src_host, src_user, src_passwd, "", (int) strtol(src_port, NULL, 10));
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
  if (check_and_free_res(&res, cmd, pps) != 0) {
    goto failed;
  }

  pps->data = (void *) stb_name;

  taos_select_db(pps->taos, topic);

  memset(sql, 0, SEG_TSQL_LEN);
  memset(topic_name, 0, SEG_TSQL_LEN);

  snprintf(sql, SEG_TSQL_LEN, "select * from p%d;", index);
  snprintf(topic_name, SEG_TSQL_LEN, "%s%d;", topic, index);

  if (async) {
    // create an asynchronized subscription, the callback function will be called every 1s
    pps->tsub = taos_subscribe(pps->taos, restart, topic_name, sql, subscribe_callback, pps, 1000);
  } else {
    // create an synchronized subscription, need to call 'taos_consume' manually
    pps->tsub = taos_subscribe(pps->taos, restart, topic_name, sql, NULL, NULL, 50);
  }

  if (pps->tsub == NULL) {
    fprintf(stderr, "sub(%d): failed to create subscription\r\n", index);
    goto failed;
  }

  if (async) {
    return NULL;
  } else {
    while (run) {
      res = taos_consume(pps->tsub);
      if (res == NULL) {
        fprintf(stderr, "sub(%d): failed to consume data\r\n", index);
        run = 0;
        break;
      } else {
        subscribe_callback(pps->tsub, res, pps, 0);
      }
    }
  }

  pthread_mutex_lock(&pps->mutex);

  pps->ready = 1;
  pps->run = 0; /* tell consumer to quit */

  if (pps->offset > 0) {
    pps->cmd[pps->offset++] = ';';
    pps->cmd[pps->offset] = '\0';
  }

  pthread_cond_signal(&pps->cond);

  while (pps->ready == 1) {
    pthread_cond_wait(&pps->cond, &pps->mutex);
  }

  pps->offset = 0;

  pthread_mutex_unlock(&pps->mutex);

  fprintf(stdout, "sub(%d) quit\r\n", index);

failed:
  if (pps->taos) {
    taos_close(pps->taos);
    pps->taos = NULL;
  }

  return NULL;
}


void *writedb_routine(void *arg)
{
  int                 retry;
  int                 np;
  int                 index;
  char                cmd[SEG_TSQL_LEN];
  TAOS_RES           *res;
  callback_params_t  *pps;

  if (arg == NULL) {
    return NULL;
  }

  pps = (callback_params_t *) arg;

  index = pps->index;

  fprintf(stdout, "wdb(%d) created\r\n", index);

  // init TAOS
  taos_init();

  pps->res_taos = taos_connect(dst_host, dst_user, dst_passwd, "", (int) strtol(dst_port, NULL, 10));
  if (pps->res_taos == NULL) {
    fprintf(stderr, "wdb(%d): failed to connect to result database\r\n", index);
    goto failed;
  }

  // create databse for result
  np = snprintf(cmd, sizeof(cmd), "create database if not exists %s keep 365000 precision 'us';", result);
  if (np <= 0) {
    fprintf(stderr, "wdb(%d): fnprintf error cmd: %s\r\n", index, cmd);
    goto failed;
  }

  cmd[np] = '\0';

  pthread_mutex_lock(&mutex);
  res = taos_query(pps->res_taos, cmd);
  pthread_mutex_unlock(&mutex);
  if (check_and_free_res(&res, cmd, pps) != 0) {
    goto failed;
  }

  taos_select_db(pps->res_taos, result);

  // create super table for result
  np = snprintf(cmd, sizeof(cmd),
                "create stable if not exists %s (ts timestamp, data int) "
                "tags (network binary(20), station binary(20), location binary(20), channel binary(20));",
                stb_name);

  if (np <= 0) {
    fprintf(stderr, "wdb(%d): fnprintf error cmd: %s\r\n", index, cmd);
    goto failed;
  }

  cmd[np] = '\0';

  pthread_mutex_lock(&mutex);
  res = taos_query(pps->res_taos, cmd);
  pthread_mutex_unlock(&mutex);
  if (check_and_free_res(&res, cmd, pps) != 0) {
    goto failed;
  }

  while (pps->run) {
    pthread_mutex_lock(&pps->mutex);
    while (pps->ready == 0) {
      pthread_cond_wait(&pps->cond, &pps->mutex);
    }

    if (pps->offset > 0) {
      retry = pps->retry;

      do {
        res = taos_query(pps->res_taos, pps->cmd);
        if (check_and_free_res(&res, pps->cmd, pps) != 0) {
          fprintf(stderr, "wdb(%d): times left to retry: %d\r\n", index, retry);
          retry--;
        } else {
          break;
        }
      } while (retry > 0);
    }

    pps->ready = 0;
    pthread_cond_signal(&pps->cond);

    pthread_mutex_unlock(&pps->mutex);
  }

  fprintf(stdout, "wdb(%d) quit\r\n", index);

failed:
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


pthread_t           p[TQ_CHAN_NUM];
pthread_t           c[TQ_CHAN_NUM];
callback_params_t   pp[TQ_CHAN_NUM];


void handler(int sig) {
    switch (sig) {
    case SIGINT:
        run = 0;
        quit = 1;
        break;
    default:
        break;
    }
}


int main(int argc, char **argv)
{
  int                 i;
  long                lretry = 0;
  struct timeval      now;
  struct sigaction    act;
#if defined(CONSUME_DEBUG)
  pthread_t           f;
  f_data_t          **f_data;
#endif

  for (i = 1; i < argc; i++) {
    if (strncmp(argv[i], "-h=", 3) == 0) {
      src_host = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-H=", 3) == 0) {
      dst_host = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-u=", 3) == 0) {
      src_user = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-U=", 3) == 0) {
      dst_user = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-p=", 3) == 0) {
      src_passwd = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-P=", 3) == 0) {
      dst_passwd = argv[i] + 3;
      continue;
    }


    if (strncmp(argv[i], "-S=", 3) == 0) {
      src_port = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-D=", 3) == 0) {
      dst_port = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-t=", 3) == 0) {
      topic = argv[i] + 3;
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

    if (strncmp(argv[i], "-r=", 3) == 0) {
      retry = argv[i] + 3;
      continue;
    }

    if (strcmp(argv[i], "-help") == 0) {
      fprintf(stderr,
              "Usage: %s [-h=src_host -u=src_user -p=src_password -H=dst_host -U=dst_user "
              "-P=dst_password -S=src_port -D=dst_port -t=topic -d=result_db_name -s=result_stb_name "
              "-r=retry -async -restart -nokeep -help]\r\n", argv[0]);

      exit(0);
    }

    if (strcmp(argv[i], "-all") == 0) {
      seed_only = 0;
      continue;
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

  if (retry[0] != '0') {
    lretry = strtol(retry, NULL, 10);
    if (lretry < 0 || errno == EINVAL || errno == ERANGE) {
      fprintf(stderr, "invalid parameter for option -r\r\n");
      exit(0);
    }
  }

  fprintf(stderr, "################################################################\r\n");
  fprintf(stderr, "# Src Server:                      %s\r\n", src_host);
  fprintf(stderr, "# Src User:                        %s\r\n", src_user);
  fprintf(stderr, "# Dst Server:                      %s\r\n", dst_host);
  fprintf(stderr, "# Dst User:                        %s\r\n", dst_user);
  fprintf(stderr, "# Src Port:                        %s\r\n", src_port);
  fprintf(stderr, "# Dst Port:                        %s\r\n", dst_port);
  fprintf(stderr, "# Topic:                           %s\r\n", topic);
  fprintf(stderr, "# Result Database Name:            %s\r\n", result);
  fprintf(stderr, "# Result Super Table Name:         %s\r\n", stb_name);
  fprintf(stderr, "# Retry:                           %s\r\n", retry);
  fprintf(stderr, "# Async:                           %d\r\n", async);
  fprintf(stderr, "# Restart:                         %d\r\n", restart);
  fprintf(stderr, "# Keep:                            %d\r\n", keep);
  fprintf(stderr, "################################################################\r\n");

  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  sigaction(SIGINT, &act, 0);

retry:

  usleep(500000);

#if defined(CONSUME_DEBUG)
  pthread_spin_init(&lock_f_data1, PTHREAD_PROCESS_PRIVATE);
  pthread_spin_init(&lock_f_data2, PTHREAD_PROCESS_PRIVATE);
  pthread_spin_init(&lock_f_data3, PTHREAD_PROCESS_PRIVATE);
  f_data = (f_data_t **) malloc(sizeof(f_data_t *) * F_DATA_TAB_MOD);
  if (f_data == NULL) {
    fprintf(stderr, "failed to allocate for fitered data hash table\r\n");
    exit(1);
  }

  memset(f_data, 0, sizeof(f_data_t *) * F_DATA_TAB_MOD);

  pthread_create(&f, NULL, fiter_statistics_routine, (void *) f_data);
#endif

  pthread_mutex_init(&mutex, NULL);
  pthread_spin_init(&lock_trows, PTHREAD_PROCESS_PRIVATE);
  pthread_spin_init(&lock_tsamps, PTHREAD_PROCESS_PRIVATE);
  pthread_spin_init(&lock_ttime, PTHREAD_PROCESS_PRIVATE);

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    subscribe_routine_init(&pp[i], i + 1,
#if defined(CONSUME_DEBUG)
                           f_data,
#endif
                           (int) lretry);
  }

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    pthread_create(&p[i], NULL, subscribe_routine, (void *) &pp[i]);
  }

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    pthread_create(&c[i], NULL, writedb_routine, (void *) &pp[i]);
  }

  if (async) {
    getchar();
  }

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    pthread_join(c[i], NULL);
  }

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    pthread_join(p[i], NULL);
  }

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    subscribe_routine_finalize(&pp[i]);
  }

  fprintf(stdout, "total samples consumed: %"PRId64"\r\n", nTotalSamples);
  fprintf(stdout, "total rows consumed: %"PRId64"\r\n", nTotalRows);
  fprintf(stdout, "total time consumed: %"PRId64"\r\n", nTotalTime / TQ_CHAN_NUM);

  gettimeofday(&now, NULL);
  fprintf(stderr, "retry at %"PRId64"\r\n", now.tv_sec * 1000000 + now.tv_usec);
  run = 1;

#if defined(CONSUME_DEBUG)
  pthread_join(f, NULL);

  if (f_data) {
    free_hash_table(f_data);
    free(f_data);
  }
#endif

  if (quit != 1) {
    goto retry;
  }

  return 0;
}

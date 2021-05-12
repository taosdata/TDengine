#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#include "taos.h"
#include "libmseed.h"
#include "sachdr.h"
#include "ew_bridge.h"
#include "PickData.h"
#include "FilterPicker5_Memory.h"
#include "FilterPicker5.h"


#define  MAX_TSQL_LEN  65535
#define  hash(key, c)  ((uint64_t) key * 31 + c)
#define  MEM_TAB_MOD   500
#define  TQ_CHAN_NUM   16


int64_t nTotalRows = 0;
time_t nTotalTime = 0;
int64_t nTotalSamples = 0;


pthread_mutex_t  mutex;
pthread_mutex_t  mutex_trows;
pthread_mutex_t  mutex_tsamps;
pthread_mutex_t  mutex_ttime;


int              run       = 1;
int              async     = 0;
int              restart   = 0;
int              keep      = 1;
int              seed_only = 1;
const char      *src_host  = "localhost";
const char      *dst_host  = "localhost";
const char      *src_user  = "root";
const char      *dst_user  = "root";
const char      *src_passwd= "taosdata";
const char      *dst_passwd= "taosdata";
const char      *src_port  = "6030";
const char      *dst_port  = "6030";
const char      *topic     = "packet";
const char      *fpicker   = "fpicker";
const char      *stb_name  = "ms";
const char      *channel   = NULL;
char             cchan     = '\0';


static signed char index_64[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55,
    56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12,
    13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32,
    33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1};

#define  CHAR64(c)     (((c) < 0 || (c) > 127) ? -1 : index_64[(c)])


typedef struct cenc_hsamples_s {
  nstime_t history[4096];
  int      first_call;
  int      offset;
  int      count;
} cenc_hsamples_t;


typedef struct cenc_samples_s {
  nstime_t time[4096];
  float    samples[4096];
} cenc_samples_t;


typedef struct memories_table_s memory_table_t;

struct memories_table_s {
  char                   sid[LM_SIDLEN];
  cenc_hsamples_t        history;
  FilterPicker5_Memory  *memory;
  memory_table_t        *next;
};


typedef struct callback_params_s {
  int               index;
  TAOS             *taos;
  TAOS             *res_taos;
  TAOS_SUB         *tsub;
  memory_table_t  **mtb;
  void             *data;
} callback_params_t;


uint64_t hash_key(const char *data, size_t len)
{
  uint64_t  i, key;

  key = 0;

  for (i = 0; i < len; i++) {
    key = hash(key, data[i]);
  }

  return key;
}


memory_table_t **get_memory_table(const char *sid, memory_table_t **mtb)
{
  int               index;
  uint64_t          hash;
  size_t            slen, dlen;
  memory_table_t  **t;

  hash = hash_key(sid, strlen(sid));
  index = hash % MEM_TAB_MOD;

  for (t = &mtb[index]; *t; t = &(*t)->next) {
    slen = strlen(sid);
    dlen = strlen((*t)->sid);
    if (slen == dlen && strncasecmp((*t)->sid, sid, slen) == 0) {
      break;
    }
  }

  if (*t == NULL) {
    *t = (memory_table_t *) malloc(sizeof(memory_table_t));
    if (*t) {
      memset(*t, 0, sizeof(memory_table_t));
      (*t)->history.first_call = 1;
      strncpy((*t)->sid, sid, strlen(sid));
    }
  }

  return t;
}


void free_memory_table(memory_table_t **mtb)
{
  int               i;
  memory_table_t   *l;
  memory_table_t  **t;

  for (i = 0; i < MEM_TAB_MOD; i++) {
    if (mtb[i]) {
      for (t = &mtb[i]; *t; /* void */) {
        free_FilterPicker5_Memory(&(*t)->memory);
        l = (*t)->next;
        free(*t);
        *t = NULL;
        t = &l;
      }
    }
  }
}


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


void cenc_picker_func(MS3TraceList *mstl, callback_params_t *param)
{
  int                    n, np;
  BOOLEAN_INT            useMemory            = TRUE_INT;
  double                 longTermWindow       = 10.0; 
  double                 threshold1           = 8.0;
  double                 threshold2           = 8.0;
  double                 tUpEvent             = 0.5;
  double                 filterWindow         = 4.0;
  double                 dt                   = 0.01;

  /* array of num_picks ptrs to PickData structures/objects containing returned picks */
  PickData             **pick_list            = NULL;
  int                    num_picks            = 0;
  int                    index, numsamples, hcount;
  int                    samprate;
  int                   *samples;
  long                   iFilterWindow;
  long                   ilongTermWindow;
  long                   itUpEvent;
  MS3TraceID            *id;
  MS3TraceSeg           *seg;
  nstime_t               dtime;
  char                   sid[LM_SIDLEN];
  char                   net[LM_SIDLEN], stat[LM_SIDLEN], loc[LM_SIDLEN], chan[LM_SIDLEN];
#if 0
  char                   timestr[64];
#endif
  cenc_samples_t         samps;
  int                    idx;
  callback_params_t     *p;
  PickData              *pick = NULL;
  memory_table_t       **t;
  TAOS_RES              *res = NULL;
  char                  *stb_name;
  char                  *pos;
  char                   cmd[MAX_TSQL_LEN];
  int                    ulen;
  cenc_hsamples_t       *h;
  time_t                 now;
  int64_t                ts;

  p = (callback_params_t *) param;
  if (p == NULL) {
    return;
  }

  stb_name = (char *) p->data;
  idx = p->index;

  filterWindow = 300.0 * dt;
  iFilterWindow = (long) (0.5 + filterWindow * 1000.0);
  if (iFilterWindow > 1) {
    filterWindow = (double) iFilterWindow / 1000.0;
  }

  longTermWindow = 500.0 * dt; 
  ilongTermWindow = (long) (0.5 + longTermWindow * 1000.0);
  if (ilongTermWindow > 1) {
    longTermWindow = (double) ilongTermWindow / 1000.0;
  }

  tUpEvent = 20.0 * dt;
  itUpEvent = (long) (0.5 + tUpEvent * 1000.0);
  if (itUpEvent > 1) {
    tUpEvent = (double) itUpEvent / 1000.0;
  }

  numsamples = 0;
  samprate = 0;

  id = mstl->traces;

  while (id) {
    now = time(NULL);
    ts = (int64_t) (id->earliest * 0.001 * 0.001 * 0.001);

    if ((ts >= now && (ts - now > 315360000)) || (ts < now && (now - ts) > 315360000)) {
      fprintf(stderr, "sub(%d): sid(%s), invalid start time: %ld\r\n", idx, id->sid, id->earliest);
      id = id->next;
      continue;
    }

    seg = id->first;

    while (seg && seed_only) {
      if ((int) seg->samprate == 100) {
        break;
      }

      seg = seg->next;
    }

    if (seg == NULL) {
      id = id->next;
      continue;
    }

    memset(&samps.time, 0, sizeof(samps.time));
    memset(&samps.samples, 0, sizeof(samps.samples));

    memset(sid, 0, LM_SIDLEN);
    memcpy(sid, id->sid, strlen(id->sid));

    memset(net, 0, LM_SIDLEN);
    memset(stat, 0, LM_SIDLEN);
    memset(loc, 0, LM_SIDLEN);
    memset(chan, 0, LM_SIDLEN);
    if (ms_sid2nslc(sid, net, stat, loc, chan)) {
      fprintf(stderr, "sub(%d): ms_sid2nslc() error\r\n", idx);
      id = id->next;
      continue;
    }

    if (cchan != '\0' && (chan[strlen(chan) - 1] | 0x20) != cchan) {
      id = id->next;
      continue;
    }

    samps.time[numsamples] = id->earliest;

    pthread_mutex_lock(&mutex);
    t = get_memory_table(sid, p->mtb);
    if (*t == NULL) {
      pthread_mutex_unlock(&mutex);
      fprintf(stderr, "sub(%d): get_memory_table error\r\n", idx);
      return;
    }

    pthread_mutex_unlock(&mutex);

    h = &(*t)->history;

    while (seg) {
      if (seg->numsamples <= 0) {
        seg = seg->next;
        continue;
      }

      dtime = 1000 * 1000 * 1000.0 / seg->samprate;
      samprate = (int) seg->samprate;

      samples = (int *) seg->datasamples;

      for (n = 0; n < seg->numsamples; n++) {
        samps.time[numsamples + n] = samps.time[0] + n * dtime;
        samps.samples[numsamples + n] = (float) samples[n];
#if 0
        ms_nstime2timestrz(samps.time[numsamples + n], timestr, ISOMONTHDAY, MICRO);
        fprintf(stderr, "%s %f\n", timestr, samps.samples[numsamples + n]);
#endif
      }

      numsamples += seg->numsamples;

      pthread_mutex_lock(&mutex_tsamps);
      nTotalSamples += seg->numsamples;
      pthread_mutex_unlock(&mutex_tsamps);

      seg = seg->next;
    }

    if (numsamples) {
      Pick(0.01, samps.samples,
           numsamples,
           filterWindow,   // 多少道滤波
           longTermWindow, // 长期平均值时间窗
           threshold1,     // 平均值阈值
           threshold2,     // 积分阈值
           tUpEvent,       // 积分时间窗
           &(*t)->memory,
           useMemory,
           &pick_list,
           &num_picks,
           "cenc_picker_func"
      );
    }

    pos = cmd;

    if (num_picks) {
      np = snprintf(cmd, sizeof(cmd),
                    "insert into %s_%s_%s_%s "
                    "using %s tags ('%s', '%s', '%s', '%s') values ",
                    net, stat, loc, chan, stb_name, net, stat, loc, chan);

      if (np <= 0) {
        fprintf(stderr, "sub(%d): fnprintf error for result table: %s, %s, %s, %s\r\n",
                idx, net, stat, loc, chan);

        goto failed;
      }

      pos = cmd + np;
    }

    hcount = (int) longTermWindow * samprate;

    for (n = 0; n < num_picks; n++) {
      pick = *(pick_list + n);
      index = (int) (pick->indices[0] * 0.5 + pick->indices[1] * 0.5);

      if (index < 0) {
        if (h->first_call) {
            fprintf(stderr, "sub(%d): no history data\r\n", idx);
            continue;
        } else {
          if (h->count + index < 0) {
            fprintf(stderr, "sub(%d): no enough history data\r\n", idx);
            continue;
          }

          np = snprintf(pos, cmd + MAX_TSQL_LEN - pos, "(%ld, now) ",
                        (int64_t) (h->history[h->count + index] * 0.001 * 0.001));
#if 0
      ms_nstime2timestrz(h->history[h->count + index], timestr, ISOMONTHDAY, MICRO);
      fprintf(stderr, "%s\n", timestr);
#endif
        }
      } else {
        np = snprintf(pos, cmd + MAX_TSQL_LEN - pos, "(%ld, now) ",
                      (int64_t) (samps.time[index] * 0.001 * 0.001));
#if 0
      ms_nstime2timestrz(samps.time[index], timestr, ISOMONTHDAY, MICRO);
      fprintf(stderr, "%s\n", timestr);
#endif
      }

      if (np <= 0) {
        fprintf(stderr, "sub(%d): fnprintf error for result table: %s, %s, %s, %s\r\n",
                idx, net, stat, loc, chan);

        goto failed;
      }

      pos += np;
    }

    ulen = sizeof(nstime_t);

    if (h->first_call) {
      h->first_call = 0;

      if (numsamples < hcount) {
        memcpy((void *) h->history, (void *) samps.time, numsamples * ulen);
        h->offset = numsamples;
        h->count = numsamples;
      } else {
        memcpy((void *) h->history, (void *) &samps.time[numsamples - hcount], hcount * ulen);
        h->count = hcount;
      }
    } else {
      if (numsamples + h->count < hcount) {
        memcpy((void *) &h->history[h->offset], (void *) samps.time, numsamples * ulen);
        h->offset += numsamples;
        h->count += numsamples;
      } else {
        if (numsamples >= hcount) {
          memcpy((void *) h->history, (void *) &samps.time[numsamples - hcount], hcount * ulen);
        } else {
          memmove((void *) h->history, (void *) &h->history[h->count + numsamples - hcount], (hcount - numsamples) * ulen);
          memmove((void *) &h->history[hcount - numsamples], (void *) samps.time, numsamples * ulen);
        }

        h->offset = 0;
        h->count = hcount;
      }
    }

    if (num_picks) {
      if (pos < cmd + MAX_TSQL_LEN - 1) {
        *pos++ = ';';
        *pos++ = '\0';

        res = taos_query(p->res_taos, cmd);
        check_and_free_res(&res, cmd);
      }
    }

    num_picks = 0;
    numsamples = 0;
    if (pick_list) {
      free(pick_list);
      pick_list = NULL;
    }

    id = id->next;
  }

failed:
  if (pick_list) {
    free(pick_list);
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
  int                idx;
  uint32_t           flags             = 0;
  TAOS_ROW           row               = NULL;
  struct timeval     start_time, end_time;
  callback_params_t *ps;

  /* Set bit flags to validate CRC and unpack data samples */
  //flags |= MSF_VALIDATECRC;
  flags |= MSF_UNPACKDATA;

  ps = (callback_params_t *) param;
  if (ps == NULL) {
    return;
  }

  idx = ps->index;

  gettimeofday(&start_time, NULL);

  while ((row = taos_fetch_row(res)) && run) {
    fields = taos_fetch_fields(res);
    nfields = taos_num_fields(res);

    //fprintf(stdout, "\r\ncenc_picker_func in sub(%d)\r\n", idx);

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

        cenc_picker_func(mstl, param);

        mstl3_free(&mstl, 0);
        free(p);
      }
    }
  }

  //fprintf(stdout, "\r\ncenc_picker_func in sub(%d) ok\r\n", idx);

  if (nRows != 0) {
    pthread_mutex_lock(&mutex_trows);
    nTotalRows += nRows;
    pthread_mutex_unlock(&mutex_trows);

    gettimeofday(&end_time, NULL);
    pthread_mutex_lock(&mutex_ttime);
    nTotalTime += (end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec);
    pthread_mutex_unlock(&mutex_ttime);

    fprintf(stderr, "sub(%d): %d row(s) consumed, now: %ld\r\n", idx, nRows, end_time.tv_sec * 1000000 + end_time.tv_usec);
  }
}


void subscribe_routine_init(callback_params_t *params, const int index, memory_table_t **mtb)
{
  if (params) {
    params->index = index;
    params->taos = NULL;
    params->res_taos = NULL;
    params->tsub = NULL;
    params->mtb = mtb;
  }
}


void *subscribe_routine(void *arg)
{
  int                 np;
  int                 index;
  char                sql[MAX_TSQL_LEN];
  char                cmd[MAX_TSQL_LEN];
  char                topic_name[MAX_TSQL_LEN];
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
  if (check_and_free_res(&res, cmd) != 0) {
    goto failed;
  }

  pps->res_taos = taos_connect(dst_host, dst_user, dst_passwd, "", (int) strtol(dst_port, NULL, 10));
  if (pps->res_taos == NULL) {
    fprintf(stderr, "sub(%d): failed to connect to result database\r\n", index);
    goto failed;
  }

  // create databse for result
  np = snprintf(cmd, sizeof(cmd), "create database if not exists %s;", fpicker);
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

  pps->data = (void *) stb_name;

  taos_select_db(pps->taos, topic);
  taos_select_db(pps->res_taos, fpicker);

  // create super table for fpicker
  np = snprintf(cmd, sizeof(cmd),
                "create stable if not exists %s (ts timestamp, calc_ts timestamp) "
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

  memset(sql, 0, MAX_TSQL_LEN);
  memset(topic_name, 0, MAX_TSQL_LEN);

  snprintf(sql, MAX_TSQL_LEN, "select * from p%d;", index);
  snprintf(topic_name, MAX_TSQL_LEN, "%s%d;", topic, index);

  if (async) {
    // create an asynchronized subscription, the callback function will be called every 1s
    pps->tsub = taos_subscribe(pps->taos, restart, topic_name, sql, subscribe_callback, pps, 1000);
  } else {
    // create an synchronized subscription, need to call 'taos_consume' manually
    pps->tsub = taos_subscribe(pps->taos, restart, topic_name, sql, NULL, NULL, 10);
  }

  if (pps->tsub == NULL) {
    fprintf(stderr, "sub(%d): failed to create subscription.\r\n", index);
    taos_close(pps->taos);
    exit(0);
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


void handler(int sig) {
    run = 0;
}


int main(int argc, char *argv[])
{
  int                 i;
  pthread_t           t[TQ_CHAN_NUM];
  callback_params_t   pp[TQ_CHAN_NUM];
  memory_table_t    **mtb;
  struct sigaction    act;

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

    if (strncmp(argv[i], "-f=", 3) == 0) {
      fpicker = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-s=", 3) == 0) {
      stb_name = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-c=", 3) == 0) {
      channel = argv[i] + 3;
      continue;
    }

    if (strcmp(argv[i], "-help") == 0) {
      fprintf(stderr,
              "Usage: %s [-h=src_host -u=src_user -p=src_password -H=dst_host -U=dst_user "
	      "-P=dst_password -S=src_port -D=dst_port -t=topic -f=result_db_name -s=result_stb_name "
              "-c=channel -async -restart -nokeep -help]\r\n", argv[0]);

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

  if (channel) {
    if (strlen(channel) != 1) {
      fprintf(stderr, "channel must be 'E', 'N' and 'Z' or 'e', 'n' and 'z'\r\n");
      exit(0);
    }

    cchan = (channel[0] | 0x20);
    if (cchan != 'e' && cchan != 'n' && cchan != 'z') {
      fprintf(stderr, "channel must be 'E', 'N' and 'Z' or 'e', 'n' and 'z'\r\n");
      exit(0);
    }
  }

  fprintf(stdout, "################################################################\r\n");
  fprintf(stdout, "# Src Server:                      %s\r\n", src_host);
  fprintf(stdout, "# Src User:                        %s\r\n", src_user);
  fprintf(stdout, "# Dst Server:                      %s\r\n", dst_host);
  fprintf(stdout, "# Dst User:                        %s\r\n", dst_user);
  fprintf(stdout, "# Src Port:                        %s\r\n", src_port);
  fprintf(stdout, "# Dst Port:                        %s\r\n", dst_port);
  fprintf(stdout, "# Topic:                           %s\r\n", topic);
  fprintf(stdout, "# Result Database Name:            %s\r\n", fpicker);
  fprintf(stdout, "# Result Super Table Name:         %s\r\n", stb_name);
  fprintf(stdout, "# Async:                           %d\r\n", async);
  fprintf(stdout, "# Restart:                         %d\r\n", restart);
  fprintf(stdout, "# Keep:                            %d\r\n", keep);
  fprintf(stdout, "# Channel:                         %s\r\n", channel);
  fprintf(stdout, "################################################################\r\n");

  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  sigaction(SIGINT, &act, 0);

  usleep(500000);

  pthread_mutex_init(&mutex, NULL);
  pthread_mutex_init(&mutex_trows, NULL);
  pthread_mutex_init(&mutex_tsamps, NULL);
  pthread_mutex_init(&mutex_ttime, NULL);

  mtb = (memory_table_t **) malloc(sizeof(memory_table_t *) * MEM_TAB_MOD);
  if (mtb == NULL) {
    fprintf(stderr, "failed to allocate for memories.\r\n");
    exit(1);
  }

  memset(mtb, 0, sizeof(memory_table_t *) * MEM_TAB_MOD);

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    subscribe_routine_init(&pp[i], i + 1, mtb);
    pthread_create(&t[i], NULL, subscribe_routine, (void *) &pp[i]);
  }

  if (async) {
    getchar();
  }

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    pthread_join(t[i], NULL);
    subscribe_routine_finalize(&pp[i]);
  }

  fprintf(stdout, "total samples consumed: %ld\r\n", nTotalSamples);
  fprintf(stdout, "total rows consumed: %ld\r\n", nTotalRows);
  fprintf(stdout, "total time consumed: %ld\r\n", nTotalTime / TQ_CHAN_NUM);

  if (mtb) {
    free_memory_table(mtb);
    free(mtb);
  }

  return 0;
}

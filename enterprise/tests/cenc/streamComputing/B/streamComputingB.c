#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <signal.h>
#include <pthread.h>
#include <sys/time.h>
#include "taos.h"
#include "libmseed.h"


#define  MAX_TSQL_LEN  131072
#define  SEG_TSQL_LEN  4096
#define  TQ_CHAN_NUM   16


int64_t nTotalRows = 0;
time_t nTotalTime = 0;
int64_t nTotalSamples = 0;


pthread_mutex_t  mutex;
pthread_mutex_t  mutex_trows;
pthread_mutex_t  mutex_tsamps;
pthread_mutex_t  mutex_ttime;


int                 run       = 1;
int                 do_write[TQ_CHAN_NUM];
int                 async     = 0;
int                 restart   = 0;
int                 keep      = 1;
int                 seed_only = 1;
const char         *src_host  = "localhost";
const char         *dst_host  = "localhost";
const char         *src_user  = "root";
const char         *dst_user  = "root";
const char         *src_passwd= "taosdata";
const char         *dst_passwd= "taosdata";
const char         *src_port  = "6030";
const char         *dst_port  = "6030";
const char         *topic     = "packet";
const char         *event     = "event";
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
  off_t      offset;
  int        num;
  double     sum;
  int        timer_set;
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


void cenc_sum_avg(MS3TraceList *mstl, callback_params_t *param)
{
  int                    i, np;
  int                    index;
  int64_t                now;
  double                 avg;
  int32_t               *samples;
  MS3TraceID            *id;
  MS3TraceSeg           *seg;
  char                   sid[LM_SIDLEN];
  char                   net[LM_SIDLEN], stat[LM_SIDLEN], loc[LM_SIDLEN], chan[LM_SIDLEN];
  callback_params_t     *p;
  TAOS_RES              *res = NULL;
  char                  *stb_name;
  struct timeval         tv;
  struct itimerval       itv;
  time_t                 cur;
  int64_t                ts;
  sigset_t               sigset;

  p = (callback_params_t *) param;
  if (p == NULL) {
    return;
  }

  index = p->index;
  stb_name = (char *) p->data;

  id = mstl->traces;

  while (id) {
    cur = time(NULL);
    ts = (int64_t) (id->earliest * 0.001 * 0.001 * 0.001);

    if ((ts >= cur && (ts - cur > 315360000)) || (ts < cur && (cur - ts) > 315360000)) {
      fprintf(stderr, "sub(%d): sid(%s), invalid start time: %ld\r\n", index, id->sid, id->earliest);
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

    while (seg) {
      if (seg->numsamples <= 0) {
        seg = seg->next;
        continue;
      }

      p->num += seg->numsamples;
      samples = (int32_t *) seg->datasamples;
      for (i = 0; i < seg->numsamples; i++) {
        p->sum += samples[i];
      }

      pthread_mutex_lock(&mutex_tsamps);
      nTotalSamples += seg->numsamples;
      pthread_mutex_unlock(&mutex_tsamps);

      seg = seg->next;
    }

    id = id->next;
  }

  if (p->num > 0 && p->timer_set == 0) {
    itv.it_value.tv_sec = 10;
    itv.it_value.tv_usec = 0;

    itv.it_interval.tv_sec = 0;
    itv.it_interval.tv_usec = 0;

    setitimer(ITIMER_REAL, &itv, NULL);

    p->timer_set = 1;
  }

  if (p->offset == 0) {
    np = snprintf(p->cmd, sizeof(p->cmd) - 1,
                 "insert into %s_%s_%s_%s using %s tags ('%s', '%s', '%s', '%s') values ",
                 net, stat, loc, chan, stb_name, net, stat, loc, chan);
    if (np <= 0) {
      fprintf(stderr, "sub(%d): fnprintf error cmd: %s\r\n", index, p->cmd);
      return;
    }

    p->cmd[np] = '\0';
    p->offset = np;
  }

  if (p->num >= 100) {
    itv.it_value.tv_sec = 10;
    itv.it_value.tv_usec = 0;
    itv.it_interval = itv.it_value;

    setitimer(ITIMER_REAL, &itv, NULL);

    p->timer_set = 0;

    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    sigprocmask(SIG_BLOCK, &sigset, NULL);

    gettimeofday(&tv, NULL);
    now = tv.tv_sec * 1000000 + tv.tv_usec;

    avg = p->sum / p->num;

    np = snprintf(p->cmd + p->offset, SEG_TSQL_LEN - p->offset - 2,
                  "(%ld, %ld, %f)", now, (int64_t) p->num, avg);

    if (np <= 0) {
      sigprocmask(SIG_UNBLOCK, &sigset, NULL);
      fprintf(stderr, "sub(%d): fprintf error cmd: %s\r\n", index, p->cmd);
      return;
    }

    p->offset += np;

    p->cmd[p->offset++] = ';';
    p->cmd[p->offset] = '\0';

    p->offset = 0;
    p->sum = 0;
    p->num = 0;

    res = taos_query(p->res_taos, p->cmd);
    check_and_free_res(&res, p->cmd);

    sigprocmask(SIG_UNBLOCK, &sigset, NULL);
  }
}


void subscribe_callback(TAOS_SUB *tsub, TAOS_RES *res, void *param, int code)
{
  int                np;
  MS3TraceList      *mstl              = NULL;
  TAOS_FIELD        *fields            = NULL;
  char              *cp;
  unsigned char     *p;
  int                records           = 0;
  int                nRows             = 0;
  int                i, len, nfields;
  uint32_t           flags             = 0;
  TAOS_ROW           row               = NULL;
  int64_t            now;
  double             avg;
  struct timeval     tv, start_time, end_time;
  struct itimerval   itv;  
  callback_params_t *par;

  par = (callback_params_t *) param;

  if (do_write[par->index - 1]) {
    do_write[par->index - 1] = 0;

    itv.it_value.tv_sec = 0;  
    itv.it_value.tv_usec = 0;  
    itv.it_interval = itv.it_value;

    setitimer(ITIMER_REAL, &itv, NULL);

    par->timer_set = 0;

    par = (callback_params_t *) param;

    if (par->offset) {
      gettimeofday(&tv, NULL);
      now = tv.tv_sec * 1000000 + tv.tv_usec;

      avg = par->sum / par->num;

      cp = par->cmd + par->offset;

      np = snprintf(cp, par->cmd + SEG_TSQL_LEN - cp,
                    "(%ld, %ld, %f)", now, (int64_t) par->num, avg);

      if (np <= 0) {
        fprintf(stderr, "sub(%d): fprintf error for timer write cmd: %s\r\n", par->index, par->cmd);
        return;
      }

      par->offset += np;

      par->cmd[par->offset++] = ';';
      par->cmd[par->offset] = '\0';

      par->offset = 0;
      par->sum = 0;
      par->num = 0;

      res = taos_query(par->res_taos, par->cmd);
      check_and_free_res(&res, par->cmd);
    }
  }

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

        cenc_sum_avg(mstl, param);

        mstl3_free(&mstl, 0);
        free(p);
      }
    }
  }

  pthread_mutex_lock(&mutex_trows);
  nTotalRows += nRows;
  pthread_mutex_unlock(&mutex_trows);

  gettimeofday(&end_time, NULL);
  pthread_mutex_lock(&mutex_ttime);
  nTotalTime += (end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec);
  pthread_mutex_unlock(&mutex_ttime);

  if (nRows != 0) {
    fprintf(stderr, "%d rows consumed.\r\n", nRows);
  }
}


void subscribe_routine_init(callback_params_t *params, const int index)
{
  if (params) {
    params->index = index;
    params->offset = 0;
    params->taos = NULL;
    params->res_taos = NULL;
    params->tsub = NULL;
    params->num = 0;
    params->sum = 0;
    params->timer_set = 0;
  }
}



void *subscribe_routine(void *arg)
{
  int                 np;
  int                 index;
  char                sql[SEG_TSQL_LEN];
  char                cmd[SEG_TSQL_LEN];
  char                topic_name[SEG_TSQL_LEN];
  struct timeval      tv;
  struct itimerval    itv;
  double              avg;
  int64_t             now;
  TAOS_RES           *res;
  callback_params_t  *params;

  if (arg == NULL) {
    return NULL;
  }

  params = (callback_params_t *) arg;

  index = params->index;

  fprintf(stdout, "sub(%d) created\r\n", index);

  // init TAOS
  taos_init();

  params->taos = taos_connect(src_host, src_user, src_passwd, "", (int) strtol(src_port, NULL, 10));
  if (params->taos == NULL) {
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
  res = taos_query(params->taos, cmd);
  pthread_mutex_unlock(&mutex);
  if (check_and_free_res(&res, cmd) != 0) {
    goto failed;
  }

  params->res_taos = taos_connect(dst_host, dst_user, dst_passwd, "", (int) strtol(dst_port, NULL, 10));
  if (params->res_taos == NULL) {
    fprintf(stderr, "sub(%d): failed to connect to event database\r\n", index);
    goto failed;
  }

  // create databse for event
  np = snprintf(cmd, sizeof(cmd), "create database if not exists %s precision 'us';", event);
  if (np <= 0) {
    fprintf(stderr, "sub(%d): fnprintf error cmd: %s\r\n", index, cmd);
    goto failed;
  }

  cmd[np] = '\0';

  pthread_mutex_lock(&mutex);
  res = taos_query(params->taos, cmd);
  pthread_mutex_unlock(&mutex);
  if (check_and_free_res(&res, cmd) != 0) {
    goto failed;
  }

  params->data = (void *) stb_name;

  taos_select_db(params->taos, topic);
  taos_select_db(params->res_taos, event);

  // create super table for event
  np = snprintf(cmd, sizeof(cmd),
                "create stable if not exists %s (ingesttime timestamp, sum bigint, avg double) "
                "tags (network binary(20), station binary(20), location binary(20), channel binary(20));",
                stb_name);

  if (np <= 0) {
    fprintf(stderr, "sub(%d): fnprintf error cmd: %s\r\n", index, cmd);
    goto failed;
  }

  cmd[np] = '\0';

  pthread_mutex_lock(&mutex);
  res = taos_query(params->res_taos, cmd);
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
    params->tsub = taos_subscribe(params->taos, restart, topic_name, sql, subscribe_callback, params, 1000);
  } else {
    // create an synchronized subscription, need to call 'taos_consume' manually
    params->tsub = taos_subscribe(params->taos, restart, topic_name, sql, NULL, NULL, 10);
  }

  if (params->tsub == NULL) {
    fprintf(stderr, "sub(%d): failed to create subscription.\r\n", index);
    goto failed;
  }

  if (async) {
    return NULL;
  } else {
    while (run) {
      res = taos_consume(params->tsub);
      if (res == NULL) {
        fprintf(stderr, "sub(%d): failed to consume data.\r\n", index);
        break;
      } else {
        subscribe_callback(params->tsub, res, params, 0);
      }
    }
  }

  fprintf(stdout, "sub(%d) quit\r\n", index);

  if (params->offset > 0 && (params->num > 0 && params->num < 100)) {
    itv.it_value.tv_sec = 10;
    itv.it_value.tv_usec = 0;
    itv.it_interval = itv.it_value;

    setitimer(ITIMER_REAL, &itv, NULL);

    gettimeofday(&tv, NULL);
    now = tv.tv_sec * 1000000 + tv.tv_usec;

    avg = params->sum / params->num;

    np = snprintf(params->cmd + params->offset, SEG_TSQL_LEN - params->offset - 2,
                  "(%ld, %ld, %f)", now, (int64_t) params->num, avg);

    if (np <= 0) {
      fprintf(stderr, "sub(%d): fprintf error cmd: %s\r\n", index, params->cmd);
      goto failed;
    }

    params->offset += np;

    params->cmd[params->offset++] = ';';
    params->cmd[params->offset] = '\0';
    params->offset = 0;

    res = taos_query(params->res_taos, params->cmd);
    check_and_free_res(&res, params->cmd);
  }

failed:
  if (params->taos) {
    taos_close(params->taos);
    params->taos = NULL;
  }

  if (params->res_taos) {
    taos_close(params->res_taos);
    params->res_taos = NULL;
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


void handler(int sig)
{
    int              i;
    struct itimerval tv;  

    switch(sig) {
    case SIGINT:
        tv.it_value.tv_sec = 0;  
        tv.it_value.tv_usec = 0;  
        tv.it_interval = tv.it_value;

        setitimer(ITIMER_REAL, &tv, NULL);

        run = 0;
	break;
    case SIGALRM:
        for (i = 0; i < TQ_CHAN_NUM; i++) {
            do_write[i] = 1;
        }
        break;
    default:
        break;
    }
}


int main(int argc, char *argv[])
{
  int                 i;
  pthread_t           t[TQ_CHAN_NUM];
  callback_params_t   pp[TQ_CHAN_NUM];
  struct sigaction    act, wact;

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

    if (strncmp(argv[i], "-e=", 3) == 0) {
      event = argv[i] + 3;
      continue;
    }

    if (strncmp(argv[i], "-s=", 3) == 0) {
      stb_name = argv[i] + 3;
      continue;
    }

    if (strcmp(argv[i], "-help") == 0) {
      fprintf(stderr,
              "Usage: %s [-h=src_host -u=src_user -p=src_password -H=dst_host -U=dst_user "
              "-P=dst_passwd -S=src_port -D=dst_port -t=topic -e=event_db_name -s=event_stb_name "
              "-async -restart -nokeep -help]\r\n", argv[0]);

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

  fprintf(stderr, "################################################################\r\n");
  fprintf(stderr, "# Src Server:                      %s\r\n", src_host);
  fprintf(stderr, "# Src User:                        %s\r\n", src_user);
  fprintf(stderr, "# Dst Server:                      %s\r\n", dst_host);
  fprintf(stderr, "# Dst User:                        %s\r\n", dst_user);
  fprintf(stderr, "# Src Port:                        %s\r\n", src_port);
  fprintf(stderr, "# Dst Port:                        %s\r\n", dst_port);
  fprintf(stderr, "# Topic:                           %s\r\n", topic);
  fprintf(stderr, "# Event Database Name:             %s\r\n", event);
  fprintf(stderr, "# Event Super Table Name:          %s\r\n", stb_name);
  fprintf(stderr, "# Async:                           %d\r\n", async);
  fprintf(stderr, "# Restart:                         %d\r\n", restart);
  fprintf(stderr, "# Keep:                            %d\r\n", keep);
  fprintf(stderr, "################################################################\r\n");

  usleep(500000);

  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  sigaction(SIGINT, &act, 0);

  wact.sa_handler = handler;
  sigemptyset(&wact.sa_mask);
  act.sa_flags = 0;
  sigaction(SIGALRM, &wact, NULL);

  for (i = 0; i < TQ_CHAN_NUM; i++) {
    do_write[i] = 0;
  }

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

  fprintf(stdout, "total samples consumed: %ld\r\n", nTotalSamples);
  fprintf(stdout, "total rows consumed: %ld\r\n", nTotalRows);
  fprintf(stdout, "total time consumed: %ld\r\n", nTotalTime / TQ_CHAN_NUM);

  return 0;
}

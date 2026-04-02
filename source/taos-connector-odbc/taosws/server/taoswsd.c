/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "cjson/cJSON.h"
#include "libwebsockets.h"
#include "taos.h"
#include "taoserror.h"

#include <assert.h>
#include <libgen.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <uuid/uuid.h>

#define SAFE_FREE(x) do {      \
  if (x) {                     \
    free(x);                   \
    x = NULL;                  \
  }                            \
} while (0)

#define HDR_FMT "%s[%d]:%s():"
#define HDR_VAL basename((char*)__FILE__), __LINE__, __func__

#define D(fmt, ...) lwsl_notice(HDR_FMT fmt "\n", HDR_VAL, ##__VA_ARGS__)
#define A(sm, fmt, ...) do {                               \
  if (sm) break;                                           \
  lwsl_err(HDR_FMT fmt "\n", HDR_VAL, ##__VA_ARGS__);      \
  abort();                                                 \
} while (0)

#define tws_session_append_err(session, fmt, ...) do {                         \
  char _buf[4096];                                                             \
  int _n = snprintf(_buf, sizeof(_buf), HDR_FMT fmt, HDR_VAL, ##__VA_ARGS__);  \
  if (_n<=0) break;                                                            \
  tws_session_append_err_impl(session, _buf, _n);                              \
} while (0)

#define tws_session_respond_ok(session, fmt, ...) do {                         \
  char _buf[4096];                                                             \
  int _n = snprintf(_buf, sizeof(_buf), HDR_FMT fmt, HDR_VAL, ##__VA_ARGS__);  \
  if (_n<=0) break;                                                            \
  tws_session_respond_ok_impl(session, _buf, _n);                              \
} while (0)

typedef struct tws_session_s                tws_session_t;

typedef struct tws_err_s                    tws_err_t;
struct tws_err_s {
  lws_dll2_t               node;
  char                     err[1024];
};

typedef struct tws_jio_s                    tws_jio_t;
struct tws_jio_s {
  lws_dll2_t               node;
  cJSON                   *json;
};

typedef struct tws_json_serializer_s         tws_json_serializer_t;
typedef struct tws_json_serializer_step_s    tws_json_serializer_step_t;

struct tws_json_serializer_step_s {
  lws_dll2_t               node;

  union {
    uintptr_t              str[2];      // str[0]: ptr; str[1]: len;
    cJSON                 *json;
  };

  int (*cb)(tws_json_serializer_step_t *step);

  tws_json_serializer_t    *owner;
};

struct tws_json_serializer_s {
  lws_dll2_owner_t            next_steps;   // tws_json_serializer_step_t*;

  cJSON                      *json;
  void                       *user;
  int (*cb)(tws_json_serializer_t *serializer, const char *s, size_t n, int is_number, void *user);
  tws_json_serializer_step_t* (*get)(void *user);
  void (*put)(tws_json_serializer_step_t *step, void *user);
};

typedef struct tws_io_s                     tws_io_t;
struct tws_io_s {
  lws_dll2_t               node;
  lws_dll2_owner_t         errs;
  cJSON                   *in;
  cJSON                   *out;
};

typedef struct tws_task_s                   tws_task_t;
typedef void (*task_routine)(tws_task_t *task);

typedef enum tws_task_state_e {
  TASK_STATE_NONE,
  TASK_STATE_PENDING,
  TASK_STATE_WORKING,
  TASK_STATE_WORKING_DONE,
  TASK_STATE_DONE,
} tws_task_state_t;

struct tws_task_s {
  lws_dll2_t                     node;

  tws_session_t                 *session;
  tws_io_t                      *io;

  task_routine                   on_work;
  task_routine                   on_done;
  task_routine                   on_release;

  uint64_t                       tick;

  tws_task_state_t               state;
};

typedef struct tws_queue_tasks_s            tws_queue_tasks_t;
struct tws_queue_tasks_s {
  lws_dll2_owner_t     tasks;
};

typedef struct tws_vhost_data_s             tws_vhost_data_t;
typedef struct tws_queue_workers_s          tws_queue_workers_t;
struct tws_queue_workers_s {
  tws_vhost_data_t         *vhd;

  pthread_mutex_t           mutex;
  pthread_cond_t            cond;

  pthread_t                *workers;
  size_t                    cap;
  size_t                    nr;

  tws_queue_tasks_t         pendings;
  tws_queue_tasks_t         done;

  volatile uint8_t          stop;
};

struct tws_vhost_data_s {
  struct lws_context         *context;

  tws_queue_workers_t         workers;
};

typedef struct foo_task_s              foo_task_t;
struct foo_task_s {
  tws_task_t             task;

  int                    secs_to_sleep;
  uint64_t               tick;
};

typedef struct taos_conn_task_s        taos_conn_task_t;
struct taos_conn_task_s {
  tws_task_t             task;

  TAOS                  *taos;
  int                    e;
  char                   errstr[1024];
};

typedef struct ds_stmt_s               ds_stmt_t;
struct ds_stmt_s {
  tws_session_t                  *owner;
  lws_dll2_t                      node;

  uuid_t                          uuid;

  char                           *sql;
  TAOS_STMT                      *stmt;
  TAOS_RES                       *res;
  TAOS_ROW                        block;
  int                             nr_rows_in_block;
  int                             i_row_in_block;

  int64_t                         nr_affected_rows64;
  int                             nr_fields;
  TAOS_FIELD                     *fields;

  uint8_t                         is_insert_stmt:1;
  uint8_t                         is_update_query:1;
};

typedef struct buffer_s           buffer_t;
struct buffer_s {
  lws_dll2_t             node;

  char                  *buf;
};

typedef struct tws_jout_s         tws_jout_t;
struct tws_jout_s {
  cJSON                 *current_out;
  uintptr_t              frame_chunks;
  int                    frame_end;
  char                   buf_out[LWS_PRE+4096];
  size_t                 nr_buf_out;
  const char            *s_chunk;
  size_t                 nr_chunk;

  char                   s_number[1024];  // NOTE: for cJSON_Number only, big enough or too big?

  cJSON                 *arr;             // NOTE: errs reference only
  lws_dll2_owner_t       errs;            // NOTE: holding for reference in json
};

struct tws_session_s {
  struct lws            *wsi;

  lws_dll2_owner_t       free_errs;       // tws_err_t
  lws_dll2_owner_t       ios;             // tws_io_t
  lws_dll2_owner_t       free_ios;        // tws_io_t
  lws_dll2_owner_t       free_steps;      // tws_json_serializer_step_t

  lws_dll2_owner_t       pending_jins;    // tws_jio_t
  lws_dll2_owner_t       pending_jouts;   // tws_jio_t
  lws_dll2_owner_t       free_jios;       // tws_jio_t

  tws_json_serializer_t  serializer;
  tws_jout_t             current_jout;

  tws_io_t              *current_io;

  char                  *recv_buf;
  size_t                 recv_cap;
  size_t                 recv_nr;

  char                  *ip;
  char                  *user;
  char                  *password;
  char                  *db;
  uint16_t               port;

  union {
    foo_task_t           foo_task;
    taos_conn_task_t     conn_task;
  };
  tws_task_t            *task;
  uint64_t               task_tick;
  uint64_t               foo_tick;

  TAOS                  *taos;

  lws_dll2_owner_t       stmts;
  lws_map_t             *stmts_map;

  uint8_t                init:1;
  uint8_t                terminating:1;
};

static int tws_json_serializer_push_raw_str(tws_json_serializer_t *serializer, const char *s, size_t n);
static int tws_json_serializer_push_str(tws_json_serializer_t *serializer, const char *s, size_t n);
static int tws_json_serializer_push_json(tws_json_serializer_t *serializer, cJSON *json);

static int _tws_json_serializer_release_step(lws_dll2_t *d, void *user)
{
  tws_json_serializer_t *serializer = (tws_json_serializer_t*)user;

  lws_dll2_remove(d);
  tws_json_serializer_step_t *p = lws_container_of(d, tws_json_serializer_step_t, node);
  serializer->put(p, serializer->user);

  return 0;
}

static void tws_json_serializer_release(tws_json_serializer_t *serializer)
{
  lws_dll2_foreach_safe(&serializer->next_steps, serializer, _tws_json_serializer_release_step);
}

static int _tws_json_serializer_step_on_raw_str(tws_json_serializer_step_t *step)
{
  tws_json_serializer_t    *serializer = step->owner;
  const char *s = (const char*)step->str[0];
  size_t      n = step->str[1];
  serializer->cb(serializer, s, n, 0, serializer->user);
  return 0;
}

static int _tws_json_serializer_step_on_str(tws_json_serializer_step_t *step)
{
  tws_json_serializer_t    *serializer = step->owner;
  const char *s = (const char*)step->str[0];
  size_t      n = step->str[1];

  if (n == 0) return 0;

  const char *matches = "\"\t\f\r\n\b\\";
  const char *escs[] = {
    "\\\"",
    "\\t",
    "\\f",
    "\\r",
    "\\n",
    "\\b",
    "\\\\",
  };
  for (size_t i=0; i<n; ++i) {
    const char *m = strchr(matches, s[i]);
    if (!m) continue;
    const char *esc = escs[m-matches];
    if (tws_json_serializer_push_str(serializer, s+i+1, n-i-1)) return -1;
    if (tws_json_serializer_push_raw_str(serializer, esc, strlen(esc))) return -1;
    serializer->cb(serializer, s, i, 0, serializer->user);
    return 0;
  }
  serializer->cb(serializer, s, n, 0, serializer->user);
  return 0;
}

static int _tws_json_serializer_step_on_json(tws_json_serializer_step_t *step)
{
  tws_json_serializer_t *serializer = step->owner;

  cJSON *json = (cJSON*)step->json;

  if (json->next) {
    if (tws_json_serializer_push_json(serializer, json->next)) return -1;
    if (json->next->string) {
      if (tws_json_serializer_push_raw_str(serializer, "\":", 2)) return -1;
      if (tws_json_serializer_push_str(serializer, json->next->string, strlen(json->next->string))) return -1;
      if (tws_json_serializer_push_raw_str(serializer, "\"", 1)) return -1;
    }
    if (tws_json_serializer_push_raw_str(serializer, ",", 1)) return -1;
  }

  int type = json->type & (~cJSON_IsReference) & (~cJSON_StringIsConst);

  switch (type) {
    case cJSON_False: {
      serializer->cb(serializer, "false", 5, 0, serializer->user);
    } break;
    case cJSON_True: {
      serializer->cb(serializer, "true", 4, 0, serializer->user);
    } break;
    case cJSON_NULL: {
      serializer->cb(serializer, "null", 4, 0, serializer->user);
    } break;
    case cJSON_Number: {
      char buf[1024];
      cJSON_PrintPreallocated(json, buf, sizeof(buf), 0);
      // NOTE: tricky
      serializer->cb(serializer, buf, strlen(buf), 1, serializer->user);
    } break;
    case cJSON_String: {
      if (tws_json_serializer_push_raw_str(serializer, "\"", 1)) return -1;
      const char *s = cJSON_GetStringValue(json);
      if (tws_json_serializer_push_str(serializer, s, strlen(s))) return -1;
      serializer->cb(serializer, "\"", 1, 0, serializer->user);
    } break;
    case cJSON_Array: {
      if (tws_json_serializer_push_raw_str(serializer, /*[*/ "]", 1)) return -1;
      cJSON *child = json->child;
      if (child) {
        if (tws_json_serializer_push_json(serializer, child)) return -1;
      }
      serializer->cb(serializer, "[" /*]*/, 1, 0, serializer->user);
    } break;
    case cJSON_Object: {
      if (tws_json_serializer_push_raw_str(serializer, /*{*/ "}", 1)) return -1;
      cJSON *child = json->child;
      if (child) {
        if (tws_json_serializer_push_json(serializer, child)) return -1;
        if (tws_json_serializer_push_raw_str(serializer, "\":", 2)) return -1;
        if (tws_json_serializer_push_str(serializer, child->string, strlen(child->string))) return -1;
        if (tws_json_serializer_push_raw_str(serializer, "\"", 1)) return -1;
      }
      serializer->cb(serializer, "{" /*}*/, 1, 0, serializer->user);
    } break;
    default: {
      D("unknown json type:0x%x", json->type);
      return -1;
    }
  }

  return 0;
}

static int tws_json_serializer_push_raw_str(tws_json_serializer_t *serializer, const char *s, size_t n)
{
  tws_json_serializer_step_t *step = (tws_json_serializer_step_t*)serializer->get(serializer->user);
  if (!step) return -1;

  step->owner            = serializer;
  step->str[0]           = (uintptr_t)s;
  step->str[1]           = n;
  step->cb               = _tws_json_serializer_step_on_raw_str;
  step->owner            = serializer;

  lws_dll2_add_head(&step->node, &serializer->next_steps);
  return 0;
}

static int tws_json_serializer_push_str(tws_json_serializer_t *serializer, const char *s, size_t n)
{
  tws_json_serializer_step_t *step = (tws_json_serializer_step_t*)serializer->get(serializer->user);
  if (!step) return -1;

  step->owner            = serializer;
  step->str[0]           = (uintptr_t)s;
  step->str[1]           = n;
  step->cb               = _tws_json_serializer_step_on_str;
  step->owner            = serializer;

  lws_dll2_add_head(&step->node, &serializer->next_steps);
  return 0;
}

static int tws_json_serializer_push_json(tws_json_serializer_t *serializer, cJSON *json)
{
  tws_json_serializer_step_t *step = (tws_json_serializer_step_t*)serializer->get(serializer->user);
  if (!step) return -1;

  step->owner            = serializer;
  step->json             = json;
  step->cb               = _tws_json_serializer_step_on_json;
  step->owner            = serializer;

  lws_dll2_add_head(&step->node, &serializer->next_steps);
  return 0;
}

static int tws_json_serializer_init(tws_json_serializer_t *serializer, cJSON *json,
    int (*cb)(tws_json_serializer_t *serializer, const char *s, size_t n, int is_number, void *user),
    tws_json_serializer_step_t* (*get)(void *user),
    void (*put)(tws_json_serializer_step_t *step, void *user),
    void *user)
{
  serializer->json           = json;
  serializer->user           = user;
  serializer->cb             = cb;
  serializer->get            = get;
  serializer->put            = put;

  return tws_json_serializer_push_json(serializer, json);
}

static int tws_json_serialize(tws_json_serializer_t *serializer, int *end)
{
  int r = 0;

  lws_dll2_t *p= lws_dll2_get_head(&serializer->next_steps);
  if (!p) {
    *end = 1;
    return 0;
  }

  *end = 0;

  lws_dll2_remove(p);

  tws_json_serializer_step_t *next_step = lws_container_of(p, tws_json_serializer_step_t, node);

  r = next_step->cb(next_step);

  serializer->put(next_step, serializer->user);

  return r;
}

static void tws_err_release(tws_err_t *err)
{
  (void)err;
}

static void tws_err_init(tws_err_t *err)
{
  memset(&err->node, 0, sizeof(err->node));
  err->err[0] = '\0';
}

static int _tws_io_reclaim_err(lws_dll2_t *d, void *user)
{
  tws_session_t *session = (tws_session_t*)user;

  lws_dll2_remove(d);
  tws_err_t *p = lws_container_of(d, tws_err_t, node);
  p->err[0] = '\0';
  lws_dll2_add_tail(&p->node, &session->free_errs);

  return 0;
}

static void tws_session_release_io(tws_session_t *session, tws_io_t *io)
{
  if (io->out) {
    cJSON_Delete(io->out);
    io->out = NULL;
  }
  if (io->in) {
    cJSON_Delete(io->in);
    io->in = NULL;
  }

  lws_dll2_foreach_safe(&io->errs, session, _tws_io_reclaim_err);
}

static int _tws_ios_reclaim_io(lws_dll2_t *d, void *user)
{
  tws_session_t *session = (tws_session_t*)user;

  lws_dll2_remove(d);
  tws_io_t *io = lws_container_of(d, tws_io_t, node);
  tws_session_release_io(session, io);
  lws_dll2_add_tail(&io->node, &session->free_ios);

  return 0;
}

static void tws_session_reclaim_ios(tws_session_t *session)
{
  lws_dll2_foreach_safe(&session->ios, session, _tws_ios_reclaim_io);
}

static int _tws_ios_release_free_io(lws_dll2_t *d, void *user)
{
  tws_session_t *session = (tws_session_t*)user;

  lws_dll2_remove(d);
  tws_io_t *io = lws_container_of(d, tws_io_t, node);
  tws_session_release_io(session, io);
  SAFE_FREE(io);

  return 0;
}

static void tws_session_release_free_ios(tws_session_t *session)
{
  lws_dll2_foreach_safe(&session->free_ios, session, _tws_ios_release_free_io);
}

static int _tws_steps_release_free_step(lws_dll2_t *d, void *user)
{
  tws_session_t *session = (tws_session_t*)user;
  (void)session;

  lws_dll2_remove(d);
  tws_json_serializer_step_t *step = lws_container_of(d, tws_json_serializer_step_t, node);
  SAFE_FREE(step);

  return 0;
}

static void tws_session_release_free_steps(tws_session_t *session)
{
  lws_dll2_foreach_safe(&session->free_steps, session, _tws_steps_release_free_step);
}

static int _tws_errs_release_free_err(lws_dll2_t *d, void *user)
{
  (void)user;

  lws_dll2_remove(d);
  tws_err_t *err = lws_container_of(d, tws_err_t, node);
  tws_err_release(err);
  SAFE_FREE(err);

  return 0;
}

static void tws_session_release_free_errs(tws_session_t *session)
{
  lws_dll2_foreach_safe(&session->free_errs, session, _tws_errs_release_free_err);
}

static int _gen_respond(int e, const char *errmsg, size_t len, cJSON **resp)
{
  A(errmsg[len] == '\0', "internal logic error");

  const char *s_json = NULL;
  if (e) {
    s_json = "{\"resp\":{\"err\":{}}}";
  } else {
    s_json = "{\"resp\":{\"ok\":{}}}";
  }
  cJSON *json = cJSON_ParseWithLength(s_json, strlen(s_json));
  if (!json) {
    D("internal logic error");
    return -1;
  } else {
    cJSON *resp = cJSON_GetObjectItem(json, "resp");
    cJSON *obj = cJSON_GetObjectItem(resp, e ? "err" : "ok");
    if (e) {
      cJSON_AddNumberToObject(obj, "err", e);
      if (errmsg) cJSON_AddStringToObject(obj, "errmsg", errmsg);
    } else {
      if (errmsg) cJSON_AddStringToObject(obj, "msg", errmsg);
    }
  }

  *resp = json;
  return 0;
}

static int _tws_json_serializer_output(tws_json_serializer_t *serializer, const char *s, size_t n, int is_number, void *user)
{
  (void)serializer;

  tws_session_t *session = (tws_session_t*)user;
  tws_jout_t *jout = &session->current_jout;
  if (0) fprintf(stderr, "%.*s\n", (int)n, s);
  A(jout->s_chunk == NULL, "internal logic error");
  jout->s_chunk  = s;
  jout->nr_chunk = n;

  if (is_number) {
    int nn = snprintf(jout->s_number, sizeof(jout->s_number), "%.*s", (int)n, s);
    A((size_t)nn == n, "internal logic error");
    jout->s_chunk  = jout->s_number;
    jout->nr_chunk = (size_t)nn;
  }

  return 0;
}

static tws_json_serializer_step_t* _tws_json_serializer_get_step(void *user)
{
  tws_session_t *session = (tws_session_t*)user;
  tws_json_serializer_step_t *step = NULL;

  lws_dll2_t *p = lws_dll2_get_head(&session->free_steps);
  if (!p) {
    step = (tws_json_serializer_step_t*)calloc(1, sizeof(*step));
    if (!step) {
      D("out of memory");
      return NULL;
    }
  } else {
    lws_dll2_remove(p);
    step = lws_container_of(p, tws_json_serializer_step_t, node);
  }

  return step;
}

static void _tws_json_serializer_put_step(tws_json_serializer_step_t *step, void *user)
{
  tws_session_t *session = (tws_session_t*)user;
  lws_dll2_add_tail(&step->node, &session->free_steps);
}

static int tws_session_serialize_flush_output(tws_session_t *session)
{
  struct lws *wsi    = session->wsi;
  tws_jout_t *jout   = &session->current_jout;
  char *buf_out      = jout->buf_out + LWS_PRE;

  int flags = 0;
  const int start = (jout->frame_chunks == 0);
  const int end   = !!jout->frame_end;
  flags = lws_write_ws_flags(LWS_WRITE_TEXT, start, end);
  lws_write(wsi, (unsigned char*)buf_out, jout->nr_buf_out, (enum lws_write_protocol)flags);
  jout->frame_chunks += 1;
  jout->nr_buf_out = 0;
  lws_callback_on_writable(wsi);
  return 0;
}

static int tws_session_serialize_output(tws_session_t *session)
{
  int r = 0;
  int end = 0;

  tws_jout_t *jout   = &session->current_jout;

again:

  while (jout->nr_chunk) {
    char *buf_out      = jout->buf_out + LWS_PRE;
    size_t cap_buf_out = sizeof(jout->buf_out) - LWS_PRE;
    size_t nr_buf_out  = jout->nr_buf_out;
    size_t available =  cap_buf_out - nr_buf_out;
    size_t nr_copy = jout->nr_chunk;
    if (jout->nr_chunk > available) nr_copy = available;

    memcpy(buf_out + nr_buf_out, jout->s_chunk, nr_copy);
    jout->s_chunk    += nr_copy;
    jout->nr_chunk   -= nr_copy;
    jout->nr_buf_out += nr_copy;

    if (jout->nr_chunk == 0) jout->s_chunk = NULL;

    if (jout->nr_buf_out == cap_buf_out) {
      return tws_session_serialize_flush_output(session);
    }
  }

  if (jout->frame_end) {
    r = tws_session_serialize_flush_output(session);
    A(r == 0, "internal logic error");
    tws_json_serializer_release(&session->serializer);
    return 0;
  }

  if (jout->current_out == NULL) return 0;

  A(jout->s_chunk == NULL, "internal logic error");
  A(jout->nr_chunk == 0, "internal logic error");
  A(jout->current_out, "internal logic error");

  end = 0;
  r = tws_json_serialize(&session->serializer, &end);
  A(r == 0, "internal logic error");
  jout->frame_end = !!end;
  if (jout->frame_end) {
    A(jout->s_chunk == NULL, "internal logic error");
    A(jout->nr_chunk == 0, "internal logic error");
  }

  goto again;
}

static int tws_session_serialize_json_out(tws_session_t *session, cJSON *json)
{
  int r = 0;

  tws_jout_t *jout   = &session->current_jout;

  A(jout->current_out == NULL, "internal logic error");
  A(jout->nr_buf_out == 0, "internal logic error");
  A(jout->s_chunk == NULL, "internal logic error");
  A(jout->nr_chunk == 0, "internal logic error");

  jout->current_out = json;
  jout->frame_chunks = 0;
  jout->frame_end = 0;

  r = tws_json_serializer_init(&session->serializer, json,
      _tws_json_serializer_output,
      _tws_json_serializer_get_step,
      _tws_json_serializer_put_step,
      session);
  A(r == 0, "internal logic error");

  lws_callback_on_writable(session->wsi);

  return 0;
}

static int tws_session_append_json_out(tws_session_t *session, cJSON *json);

static int tws_session_respond_json(tws_session_t *session, cJSON *json)
{
  int r = 0;

  tws_jout_t *jout   = &session->current_jout;

  if (jout->current_out) {
    r = tws_session_append_json_out(session, json);
    if (r) {
      cJSON_Delete(json);
      return -1;
    }
    return 0;
  }
  return tws_session_serialize_json_out(session, json);
}

static int tws_session_append_err_impl(tws_session_t *session, const char *errmsg, size_t len)
{
  lwsl_err("%.*s\n", (int)len, errmsg);

  if (!session->current_io) {
    D("internal logic error");
    return -1;
  }
  tws_err_t *v = NULL;
  lws_dll2_t *p = lws_dll2_get_head(&session->free_errs);
  if (!p) {
    v = (tws_err_t*)malloc(sizeof(*v));
    if (!v) {
      D("out of memory");
      return -1;
    }
    tws_err_init(v);
  } else {
    v = lws_container_of(p, tws_err_t, node);
    lws_dll2_remove(p);
  }

  snprintf(v->err, sizeof(v->err), "%.*s", (int)len, errmsg);

  lws_dll2_add_tail(&v->node, &session->current_io->errs);

  return 0;
}

static int tws_session_respond_ok_impl(tws_session_t *session, const char *msg, size_t len)
{
  int r = 0;

  A(session->current_io, "internal logic error");
  A(session->current_io->out == NULL, "internal logic error");

  cJSON *json = NULL;

  r = _gen_respond(0, msg, len, &json);
  if (r) return -1;

  return tws_session_respond_json(session, json);
}

static int _tws_errs_transfer_errmsg(lws_dll2_t *d, void *user)
{
  tws_jout_t *jout = (tws_jout_t*)user;
  cJSON *arr = jout->arr;
  tws_err_t *err = lws_container_of(d, tws_err_t, node);
  cJSON *ej = cJSON_CreateStringReference(err->err);
  if (ej) {
    cJSON_AddItemToArray(arr, ej);
  }
  lws_dll2_remove(&err->node);
  lws_dll2_add_tail(&err->node, &jout->errs);
  return 0;
}

static void tws_session_check_errs(tws_session_t *session)
{
  if (!session->current_io) return;
  if (session->current_io->errs.count) {
    A(session->current_io->out == NULL, "internal logic error");

    const char *s_json = "{\"resp\":{\"errs\":[]}}";
    cJSON *json = cJSON_ParseWithLength(s_json, strlen(s_json));
    if (!json) return;

    cJSON *resp = cJSON_GetObjectItem(json, "resp");
    cJSON *arr = cJSON_GetObjectItem(resp, "errs");

    session->current_jout.arr = arr;

    lws_dll2_foreach_safe(&session->current_io->errs, &session->current_jout, _tws_errs_transfer_errmsg);

    tws_session_respond_json(session, json);
    return;
  }

  A(session->current_io->out == NULL, "internal logic error");
}

static tws_io_t* tws_session_push_io(tws_session_t *session)
{
  tws_io_t *v = NULL;

  lws_dll2_t *p = lws_dll2_get_head(&session->free_ios);
  if (!p) {
    v = (tws_io_t*)calloc(1, sizeof(*v));
    if (!v) return NULL;
  } else {
    v = lws_container_of(p, tws_io_t, node);
    lws_dll2_remove(p);
  }

  lws_dll2_add_tail(&v->node, &session->ios);

  return v;
}

static void tws_session_pop_io(tws_session_t *session)
{
  tws_session_check_errs(session);

  tws_io_t *io = session->current_io;

  if (!io) return;

  lws_dll2_t *d = &io->node;

  lws_dll2_remove(d);
  tws_session_release_io(session, io);
  lws_dll2_add_tail(&io->node, &session->free_ios);

  session->current_io = NULL;
}

static const char* _lws_callback_reason_name(enum lws_callback_reasons reason)
{
#define CASE(x) case x: return #x
  switch (reason) {
    CASE(LWS_CALLBACK_PROTOCOL_INIT);
    CASE(LWS_CALLBACK_PROTOCOL_DESTROY);
    CASE(LWS_CALLBACK_WSI_CREATE);
    CASE(LWS_CALLBACK_WSI_DESTROY);
    CASE(LWS_CALLBACK_WSI_TX_CREDIT_GET);
    CASE(LWS_CALLBACK_OPENSSL_LOAD_EXTRA_CLIENT_VERIFY_CERTS);
    CASE(LWS_CALLBACK_OPENSSL_LOAD_EXTRA_SERVER_VERIFY_CERTS);
    CASE(LWS_CALLBACK_OPENSSL_PERFORM_CLIENT_CERT_VERIFICATION);
    CASE(LWS_CALLBACK_OPENSSL_CONTEXT_REQUIRES_PRIVATE_KEY);
    CASE(LWS_CALLBACK_SSL_INFO);
    CASE(LWS_CALLBACK_OPENSSL_PERFORM_SERVER_CERT_VERIFICATION);
    CASE(LWS_CALLBACK_SERVER_NEW_CLIENT_INSTANTIATED);
    CASE(LWS_CALLBACK_HTTP);
    CASE(LWS_CALLBACK_HTTP_BODY);
    CASE(LWS_CALLBACK_HTTP_BODY_COMPLETION);
    CASE(LWS_CALLBACK_HTTP_FILE_COMPLETION);
    CASE(LWS_CALLBACK_HTTP_WRITEABLE);
    CASE(LWS_CALLBACK_CLOSED_HTTP);
    CASE(LWS_CALLBACK_FILTER_HTTP_CONNECTION);
    CASE(LWS_CALLBACK_ADD_HEADERS);
    CASE(LWS_CALLBACK_VERIFY_BASIC_AUTHORIZATION);
    CASE(LWS_CALLBACK_CHECK_ACCESS_RIGHTS);
    CASE(LWS_CALLBACK_PROCESS_HTML);
    CASE(LWS_CALLBACK_HTTP_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_HTTP_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_HTTP_CONFIRM_UPGRADE);
    CASE(LWS_CALLBACK_ESTABLISHED_CLIENT_HTTP);
    CASE(LWS_CALLBACK_CLOSED_CLIENT_HTTP);
    CASE(LWS_CALLBACK_RECEIVE_CLIENT_HTTP_READ);
    CASE(LWS_CALLBACK_RECEIVE_CLIENT_HTTP);
    CASE(LWS_CALLBACK_COMPLETED_CLIENT_HTTP);
    CASE(LWS_CALLBACK_CLIENT_HTTP_WRITEABLE);
    CASE(LWS_CALLBACK_CLIENT_HTTP_REDIRECT);
    CASE(LWS_CALLBACK_CLIENT_HTTP_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_CLIENT_HTTP_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_ESTABLISHED);
    CASE(LWS_CALLBACK_CLOSED);
    CASE(LWS_CALLBACK_SERVER_WRITEABLE);
    CASE(LWS_CALLBACK_RECEIVE);
    CASE(LWS_CALLBACK_RECEIVE_PONG);
    CASE(LWS_CALLBACK_WS_PEER_INITIATED_CLOSE);
    CASE(LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION);
    CASE(LWS_CALLBACK_CONFIRM_EXTENSION_OKAY);
    CASE(LWS_CALLBACK_WS_SERVER_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_WS_SERVER_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_CLIENT_CONNECTION_ERROR);
    CASE(LWS_CALLBACK_CLIENT_FILTER_PRE_ESTABLISH);
    CASE(LWS_CALLBACK_CLIENT_ESTABLISHED);
    CASE(LWS_CALLBACK_CLIENT_CLOSED);
    CASE(LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER);
    CASE(LWS_CALLBACK_CLIENT_RECEIVE);
    CASE(LWS_CALLBACK_CLIENT_RECEIVE_PONG);
    CASE(LWS_CALLBACK_CLIENT_WRITEABLE);
    CASE(LWS_CALLBACK_CLIENT_CONFIRM_EXTENSION_SUPPORTED);
    CASE(LWS_CALLBACK_WS_EXT_DEFAULTS);
    CASE(LWS_CALLBACK_FILTER_NETWORK_CONNECTION);
    CASE(LWS_CALLBACK_WS_CLIENT_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_WS_CLIENT_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_GET_THREAD_ID);
    CASE(LWS_CALLBACK_ADD_POLL_FD);
    CASE(LWS_CALLBACK_DEL_POLL_FD);
    CASE(LWS_CALLBACK_CHANGE_MODE_POLL_FD);
    CASE(LWS_CALLBACK_LOCK_POLL);
    CASE(LWS_CALLBACK_UNLOCK_POLL);
    CASE(LWS_CALLBACK_CGI);
    CASE(LWS_CALLBACK_CGI_TERMINATED);
    CASE(LWS_CALLBACK_CGI_STDIN_DATA);
    CASE(LWS_CALLBACK_CGI_STDIN_COMPLETED);
    CASE(LWS_CALLBACK_CGI_PROCESS_ATTACH);
    CASE(LWS_CALLBACK_SESSION_INFO);
    CASE(LWS_CALLBACK_GS_EVENT);
    CASE(LWS_CALLBACK_HTTP_PMO);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_RX);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_RX);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_CLOSE);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_CLOSE);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_WRITEABLE);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_WRITEABLE);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_ADOPT);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_ADOPT);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_PROXY_CLI_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_PROXY_SRV_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_RX);
    CASE(LWS_CALLBACK_RAW_CLOSE);
    CASE(LWS_CALLBACK_RAW_WRITEABLE);
    CASE(LWS_CALLBACK_RAW_ADOPT);
    CASE(LWS_CALLBACK_RAW_CONNECTED);
    CASE(LWS_CALLBACK_RAW_SKT_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_SKT_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_ADOPT_FILE);
    CASE(LWS_CALLBACK_RAW_RX_FILE);
    CASE(LWS_CALLBACK_RAW_WRITEABLE_FILE);
    CASE(LWS_CALLBACK_RAW_CLOSE_FILE);
    CASE(LWS_CALLBACK_RAW_FILE_BIND_PROTOCOL);
    CASE(LWS_CALLBACK_RAW_FILE_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_TIMER);
    CASE(LWS_CALLBACK_EVENT_WAIT_CANCELLED);
    CASE(LWS_CALLBACK_CHILD_CLOSING);
    CASE(LWS_CALLBACK_CONNECTING);
    CASE(LWS_CALLBACK_VHOST_CERT_AGING);
    CASE(LWS_CALLBACK_VHOST_CERT_UPDATE);
    CASE(LWS_CALLBACK_MQTT_NEW_CLIENT_INSTANTIATED);
    CASE(LWS_CALLBACK_MQTT_IDLE);
    CASE(LWS_CALLBACK_MQTT_CLIENT_ESTABLISHED);
    CASE(LWS_CALLBACK_MQTT_SUBSCRIBED);
    CASE(LWS_CALLBACK_MQTT_CLIENT_WRITEABLE);
    CASE(LWS_CALLBACK_MQTT_CLIENT_RX);
    CASE(LWS_CALLBACK_MQTT_UNSUBSCRIBED);
    CASE(LWS_CALLBACK_MQTT_DROP_PROTOCOL);
    CASE(LWS_CALLBACK_MQTT_CLIENT_CLOSED);
    CASE(LWS_CALLBACK_MQTT_ACK);
    CASE(LWS_CALLBACK_MQTT_RESEND);
    CASE(LWS_CALLBACK_MQTT_UNSUBSCRIBE_TIMEOUT);
    CASE(LWS_CALLBACK_MQTT_SHADOW_TIMEOUT);
    CASE(LWS_CALLBACK_USER);
    default:
      return "LWS_CALLBACK_unknown";
  }
#undef CASE
}

static void ds_stmt_init(ds_stmt_t *stmt, tws_session_t *session)
{
  A(stmt->owner == NULL, "internal logic error");
  stmt->owner = session;
  uuid_generate(stmt->uuid);
}

static void ds_stmt_release(ds_stmt_t *stmt)
{
  if (!stmt) return;

  tws_session_t *session = stmt->owner;
  if (session) {
    lws_map_t *map = session->stmts_map;
    if (map) {
      struct lws_map_item *p = lws_map_item_lookup(map, (const lws_map_key_t)stmt->uuid, sizeof(stmt->uuid));
      if (p) {
        lws_map_item_destroy(p);
      }
    }
  }

  lws_dll2_remove(&stmt->node);

  if (stmt->block) stmt->block = NULL;
  if (stmt->res && !stmt->stmt) {
    D("taos_free_result(res:%p) ...", stmt->res);
    taos_free_result(stmt->res);
    D("taos_free_result(res:%p) => void", stmt->res);
    stmt->res = NULL;
  }
  if (stmt->stmt) {
    D("taos_stmt_close(stmt:%p) ...", stmt->stmt);
    int r = taos_stmt_close(stmt->stmt);
    D("taos_stmt_close(stmt:%p) => %d", stmt->stmt, r);
    stmt->stmt = NULL;
  }
  SAFE_FREE(stmt->sql);
}

static int ds_stmt_gen_query_resp(ds_stmt_t *stmt, cJSON **resp)
{
  char buf[1024]; buf[0] = '\0';
  tws_session_t *session = stmt->owner;

  const char *s_json = "{\"resp\":{\"ok\":{}}}";
  cJSON *json = cJSON_ParseWithLength(s_json, strlen(s_json));
  if (!json) {
    D("internal logic error");
    return -1;
  }

  cJSON *v_resp = cJSON_GetObjectItem(json, "resp");
  cJSON *obj = cJSON_GetObjectItem(v_resp, "ok");

  do {
    {
      uuid_unparse(stmt->uuid, buf); // FIXME: upper/lower?
      cJSON *v = cJSON_CreateString(buf);
      if (!v) {
        tws_session_append_err(session, "out of memory");
        break;
      }
      if (!cJSON_AddItemToObject(obj, "uuid", v)) {
        cJSON_Delete(v);
        tws_session_append_err(session, "out of memory");
        break;
      }
    }

    if (stmt->is_update_query) {
      snprintf(buf, sizeof(buf), "%" PRId64 "", stmt->nr_affected_rows64);
      cJSON *v = cJSON_CreateString(buf);
      if (!v) {
        tws_session_append_err(session, "out of memory");
        break;
      }
      if (!cJSON_AddItemToObject(obj, "is_update", v)) {
        cJSON_Delete(v);
        tws_session_append_err(session, "out of memory");
        break;
      }
    } else {
      // FIXME: base64(bin) or array?
      cJSON *v = cJSON_CreateArray();
      if (!v) {
        tws_session_append_err(session, "out of memory");
        break;
      }
      if (!cJSON_AddItemToObject(obj, "fields", v)) {
        cJSON_Delete(v);
        tws_session_append_err(session, "out of memory");
        break;
      }
      int i = 0;
      for (i=0; i<stmt->nr_fields; ++i) {
        TAOS_FIELD *field = stmt->fields + i;
        cJSON *v_field = cJSON_CreateArray();
        if (!v_field) {
          tws_session_append_err(session, "out of memory");
          break;
        }
        if (!cJSON_AddItemToArray(v, v_field)) {
          tws_session_append_err(session, "out of memory");
          break;
        }

        cJSON *vv = NULL;

        snprintf(buf, sizeof(buf), "%.*s", (int)sizeof(field->name), field->name);
        vv = cJSON_CreateString(buf);
        if (!vv) {
          tws_session_append_err(session, "out of memory");
          break;
        }
        if (!cJSON_AddItemToArray(v_field, vv)) {
          tws_session_append_err(session, "out of memory");
          break;
        }

        vv = cJSON_CreateNumber(field->type); // FIXME: type name?
        if (!vv) {
          tws_session_append_err(session, "out of memory");
          break;
        }
        if (!cJSON_AddItemToArray(v_field, vv)) {
          tws_session_append_err(session, "out of memory");
          break;
        }

        vv = cJSON_CreateNumber(field->bytes);
        if (!vv) {
          tws_session_append_err(session, "out of memory");
          break;
        }
        if (!cJSON_AddItemToArray(v_field, vv)) {
          tws_session_append_err(session, "out of memory");
          break;
        }
      }
      if (i < stmt->nr_fields) break;
    }

    *resp = json;
    return 0;
  } while (0);

  cJSON_Delete(json);
  return -1;
}

static int ds_stmt_query(ds_stmt_t *stmt, const char *sql)
{
  A(stmt && stmt->owner && stmt->sql == NULL, "internal logic error");

  tws_session_t *session = stmt->owner;

  if (!session->taos) {
    tws_session_append_err(session, "%s", "taos connection not ready");
    return -1;
  }
  if (stmt->stmt || stmt->res) {
    tws_session_append_err(session, "%s", "statement not closed yet");
    return -1;
  }

  stmt->sql = strdup(sql);
  if (!stmt->sql) {
    tws_session_append_err(session, "%s", "out of memory");
    return -1;
  }

  D("taos_query(sql:%s) ...", stmt->sql);
  stmt->res = taos_query(session->taos, stmt->sql);
  D("taos_query(sql:%s) => %p", stmt->sql, stmt->res);

  TAOS_RES *res = stmt->res;

  int e = taos_errno(res);
  if (e) {
    tws_session_append_err(session, "[taosc]query failure:[%d]%s", e, taos_errstr(res));
    ds_stmt_release(stmt);
    return -1;
  }

  if (!res) return 0;

  stmt->is_insert_stmt = 0;

  D("taos_is_update_query(res:%p) ...", res);
  stmt->is_update_query = !!taos_is_update_query(res);
  D("taos_is_update_query(res:%p) => %d", res, stmt->is_update_query);
  if (stmt->is_update_query) {
    D("taos_affected_rows64(res:%p) ...", res);
    stmt->nr_affected_rows64 = taos_affected_rows64(res);
    D("taos_affected_rows64(res:%p) => %" PRId64 "", res, stmt->nr_affected_rows64);
  } else {
    D("taos_field_count(res:%p) ...", res);
    stmt->nr_fields = taos_field_count(res);
    D("taos_field_count(res:%p) => %d", res, stmt->nr_fields);
    D("taos_fetch_fields(res:%p) ...", res);
    stmt->fields = taos_fetch_fields(res);
    D("taos_fetch_fields(res:%p) => %p", res, stmt->fields);
  }

  return 0;
}

static int ds_stmt_fetch_block(ds_stmt_t *stmt)
{
  A(stmt && stmt->owner && stmt->res, "internal logic error");

  int r = 0;

  tws_session_t *session = stmt->owner;

  stmt->block = NULL;
  stmt->nr_rows_in_block = 0;
  stmt->i_row_in_block = 0;

  TAOS_RES *res = stmt->res;

  D("taos_fetch_block_s(res:%p,numOfRows:%p,rows:%p) ...", res, &stmt->nr_rows_in_block, &stmt->block);
  r = taos_fetch_block_s(res, &stmt->nr_rows_in_block, &stmt->block);
  D("taos_fetch_block_s(res:%p,numOfRows:%p[%d],rows:%p[%p]) => %d", res, &stmt->nr_rows_in_block, stmt->nr_rows_in_block, &stmt->block, stmt->block, r);
  if (r) {
    tws_session_append_err(session, "[taosc]fetch block failure:[%d]%s", r, taos_errstr(res));
    return -1;
  }

  return 0;
}

static void tws_queue_tasks_append(tws_queue_tasks_t *tasks, tws_task_t *task)
{
  lws_dll2_add_tail(&task->node, &tasks->tasks);
}

static tws_task_t* tws_queue_tasks_remove(tws_queue_tasks_t *tasks)
{
  lws_dll2_t *p = lws_dll2_get_head(&tasks->tasks);
  if (!p) return NULL;
  lws_dll2_remove(p);

  return lws_container_of(p, tws_task_t, node);
}

static volatile int interrupted = 0;

static void task_on_release(tws_task_t *task)
{
  A(task->state == TASK_STATE_DONE, "internal logic error");
  tws_session_t *session = task->session;
  A(session, "internal logic error");
  A(task == session->task, "internal logic error");

  task->on_release(task);
  task->state = TASK_STATE_NONE;

  session->task = NULL;
}

static void _worker_routine(tws_queue_workers_t *workers)
{
  while (!interrupted) {
    if (workers->stop) break;

    tws_task_t *task = tws_queue_tasks_remove(&workers->pendings);
    if (!task) {
      pthread_cond_wait(&workers->cond, &workers->mutex);
      continue;
    }

    A(task->state == TASK_STATE_PENDING, "internal logic error");
    task->state = TASK_STATE_WORKING;

    pthread_mutex_unlock(&workers->mutex);

    task->on_work(task);

    pthread_mutex_lock(&workers->mutex);

    A(task->state == TASK_STATE_WORKING, "internal logic error");
    task->state = TASK_STATE_WORKING_DONE;
    tws_queue_tasks_append(&workers->done, task);

    A(workers, "internal logic error");
    A(workers->vhd, "internal logic error");
    A(workers->vhd->context, "internal logic error");

    lws_cancel_service(workers->vhd->context);
  }

  while (1) {
    tws_task_t *task = tws_queue_tasks_remove(&workers->pendings);
    if (!task) break;

    A(task->state == TASK_STATE_PENDING, "internal logic error");
    task->state = TASK_STATE_WORKING_DONE;
    tws_queue_tasks_append(&workers->done, task);
    struct lws *wsi = task->session->wsi;
    lws_callback_on_writable(wsi);
  }
}

static void* worker_routine(void *arg)
{
  tws_queue_workers_t *workers = (tws_queue_workers_t*)arg;

  pthread_mutex_lock(&workers->mutex);

  _worker_routine(workers);

  pthread_mutex_unlock(&workers->mutex);

  return NULL;
}

static void tws_queue_workers_init(tws_queue_workers_t *workers, size_t cap)
{
  int r = 0;

  if (!workers) return;

  if (pthread_mutex_init(&workers->mutex, NULL)) {
    A(0, "pthread_mutex_init failed");
  }
  if (pthread_cond_init(&workers->cond, NULL)) {
    A(0, "pthread_cond_init failed");
  }

  workers->cap = cap;
  workers->workers = (pthread_t*)calloc(workers->cap, sizeof(*workers->workers));
  A(workers->workers, "out of memory");

  for (size_t i=0; i<workers->cap; ++i) {
    r = pthread_create(workers->workers + i, NULL, worker_routine, workers);
    if (r) break;
    workers->nr += 1;
  }

  A(workers->nr, "out of memory");
}

static void tws_queue_workers_stop(tws_queue_workers_t *workers)
{
  if (!workers) return;
  if (workers->stop) return;

  workers->stop     = 1;

  for (size_t i=0; i<workers->nr; ++i) {
    pthread_cond_signal(&workers->cond);
  }

  for (size_t i=0; i<workers->nr; ++i) {
    pthread_join(workers->workers[i], NULL);
  }

  SAFE_FREE(workers->workers);
}

static int _post_task(tws_queue_workers_t *workers, tws_task_t *task)
{
  if (workers->stop) return -1;

  A(task->state == TASK_STATE_NONE, "internal logic error");
  task->state = TASK_STATE_PENDING;

  tws_queue_tasks_append(&workers->pendings, task);

  pthread_cond_signal(&workers->cond);

  return 0;
}

static int tws_queue_workers_post_task(tws_queue_workers_t *workers, tws_task_t *task)
{
  int r = 0;
  pthread_mutex_lock(&workers->mutex);
  r = _post_task(workers, task);
  pthread_mutex_unlock(&workers->mutex);

  return r ? -1 : 0;
}

static void _tws_queue_workers_wakeup_done_tasks(tws_queue_workers_t *workers)
{
  while (1) {
    tws_task_t *task = tws_queue_tasks_remove(&workers->done);
    if (!task) break;
    tws_session_t *session = task->session;

    A(task->state == TASK_STATE_WORKING_DONE, "internal logic error");
    task->state = TASK_STATE_DONE;

    A(task->io, "internal logic error");
    A(session->current_io == NULL, "internal logic error");

    session->current_io = task->io;
    task->io = NULL;

    task->on_done(task);

    tws_session_pop_io(session);

    lws_callback_on_writable(session->wsi);

    task_on_release(task);
  }
}

static void tws_queue_workers_wakeup_done_tasks(tws_queue_workers_t *workers)
{
  pthread_mutex_lock(&workers->mutex);
  _tws_queue_workers_wakeup_done_tasks(workers);
  pthread_mutex_unlock(&workers->mutex);
}

static void tws_vhost_data_init(tws_vhost_data_t *vhd, struct lws *wsi)
{
  if (!vhd) return;

  vhd->context = lws_get_context(wsi);
  vhd->workers.vhd = vhd;

  tws_queue_workers_init(&vhd->workers, 4); // TODO:

  return;
}

static void tws_vhost_data_release(tws_vhost_data_t *vhd)
{
  if (!vhd) return;

  tws_queue_workers_stop(&vhd->workers);

  return;
}

static int tws_vhost_data_post_task(tws_vhost_data_t *vhd, tws_task_t *task)
{
  int r = 0;

  tws_session_t *session = task->session;
  A(session, "internal logic error");
  A(session->current_io, "internal logic error");

  task->io = session->current_io;
  r = tws_queue_workers_post_task(&vhd->workers, task);
  if (r) {
    task->io = NULL;
  } else {
    session->current_io = NULL;
  }

  return r;
}

static void tws_vhost_data_wakeup_done_tasks(tws_vhost_data_t *vhd)
{
  tws_queue_workers_wakeup_done_tasks(&vhd->workers);
}

static int tws_session_init(tws_session_t *session)
{
  if (!session || session->init) return 0;
  A(session->wsi, "internal logic error");

  struct lws *wsi = session->wsi;

  session->init = 1;

  char buf[1024]; buf[0] = '\0'; // FIXME: big enough?

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "port", buf, sizeof(buf)-1) >=0 ) {
    int bytes = 0;
    int port = 0;
    int n = sscanf(buf, "%d%n", &port, &bytes);
    if (n == 0 || buf[bytes] || port < 0 || port > UINT16_MAX) {
      tws_session_append_err(session, "%s", "geturlarg `?port` failure");
      return -1;
    }
    session->port = (uint16_t)port;
  }

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "ip", buf, sizeof(buf)-1) >= 0 && buf[0]) {
    session->ip = strdup(buf);
    if (!session->ip) {
      tws_session_append_err(session, "%s", "out of memory");
      return -1;
    }
  }

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "user", buf, sizeof(buf)-1) >=0 && buf[0]) {
    session->user = strdup(buf);
    if (!session->user) {
      tws_session_append_err(session, "%s", "out of memory");
      return -1;
    }
  }

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "password", buf, sizeof(buf)-1) >=0 && buf[0]) {
    session->password = strdup(buf);
    if (!session->password) {
      tws_session_append_err(session, "%s", "out of memory");
      return -1;
    }
  }

  buf[0] = '\0';
  if (lws_get_urlarg_by_name_safe(wsi, "db", buf, sizeof(buf)-1) >=0 && buf[0]) {
    session->db = strdup(buf);
    if (!session->db) {
      tws_session_append_err(session, "%s", "out of memory");
      return -1;
    }
  }

	lws_map_info_t info = {0};
	session->stmts_map = lws_map_create(&info);
  if (!session->stmts_map) {
    tws_session_append_err(session, "%s", "out of memory");
    return -1;
  }

  return 0;
}

static int _tws_session_release_stmt(struct lws_dll2 *d, void *user)
{
  tws_session_t *session = (tws_session_t*)user;
  ds_stmt_t *stmt = lws_container_of(d, ds_stmt_t, node);
  A(session == stmt->owner, "internal logic error");

  lws_dll2_remove(d);
  ds_stmt_release(stmt);
  SAFE_FREE(stmt);

  return 0;
}

static void tws_session_release_stmts(tws_session_t *session)
{
  lws_dll2_foreach_safe(&session->stmts, session, _tws_session_release_stmt);
  if (session->stmts_map) {
    lws_map_destroy(&session->stmts_map);
  }
}

static void tws_session_release_taos(tws_session_t *session)
{
  if (!session) return;

  tws_session_release_stmts(session);

  if (session->taos) {
    D("taos_close(taos:%p) ...", session->taos);
    taos_close(session->taos);
    D("taos_close(taos:%p) => void", session->taos);
    session->taos = NULL;
  }
}

static void tws_jio_release(tws_jio_t *jio)
{
  if (jio->json) {
    cJSON_Delete(jio->json);
    jio->json = NULL;
  }
}

static int _tws_jios_release_jio(lws_dll2_t *d, void *user)
{
  tws_session_t *session = (tws_session_t*)user;
  (void)session;

  lws_dll2_remove(d);
  tws_jio_t *jio = lws_container_of(d, tws_jio_t, node);
  tws_jio_release(jio);
  SAFE_FREE(jio);

  return 0;
}

static void tws_session_release_jios(tws_session_t *session)
{
  lws_dll2_foreach_safe(&session->pending_jins, session, _tws_jios_release_jio);
  lws_dll2_foreach_safe(&session->pending_jouts, session, _tws_jios_release_jio);
  lws_dll2_foreach_safe(&session->free_jios, session, _tws_jios_release_jio);
}

static void tws_session_release(tws_session_t *session)
{
  if (!session) return;

  tws_session_release_taos(session);

  SAFE_FREE(session->recv_buf);
  session->recv_cap = 0;
  session->recv_nr = 0;

  SAFE_FREE(session->ip);
  SAFE_FREE(session->user);
  SAFE_FREE(session->password);
  SAFE_FREE(session->db);
  session->port = 0;

  tws_session_reclaim_ios(session);
  tws_session_release_free_ios(session);

  tws_json_serializer_release(&session->serializer);
  tws_session_release_free_steps(session);

  tws_session_release_jios(session);

  tws_session_release_free_errs(session);

  return;
}

static void taos_conn_task_on_work(tws_task_t *task)
{
  taos_conn_task_t *taos_conn_task = (taos_conn_task_t*)task;

  tws_session_t *session = task->session;

  taos_conn_task->errstr[0] = '\0';

  D("taos_connect(ip:%s,user:%s,password:%s,db:%s,port:%d) ...", session->ip, session->user, session->password, session->db, session->port);
  taos_conn_task->taos = taos_connect(session->ip, session->user, session->password, session->db, session->port);
  D("taos_connect(ip:%s,user:%s,password:%s,db:%s,port:%d) => %p", session->ip, session->user, session->password, session->db, session->port, taos_conn_task->taos);
  if (!taos_conn_task->taos) {
    taos_conn_task->e = taos_errno(NULL);
    const char *es = taos_errstr(NULL);
    snprintf(taos_conn_task->errstr, sizeof(taos_conn_task->errstr), "failure:[taosc][%d]%s", taos_conn_task->e, es);
    return;
  }

  return;
}

static void taos_conn_task_on_done(tws_task_t *task)
{
  taos_conn_task_t *taos_conn_task = (taos_conn_task_t*)task;
  tws_session_t *session = task->session;

  if (!taos_conn_task->taos) {
    tws_session_append_err(session, "%s", taos_conn_task->errstr);
    return;
  }

  session->taos = taos_conn_task->taos;
  taos_conn_task->taos = NULL;
  tws_session_respond_ok(session,
      "taos_connect success:ip:%s,user:%s,password:%s,db:%s;port,%d",
      session->ip, session->user, session->password, session->db, session->port);
}

static void taos_conn_task_on_release(tws_task_t *task)
{
  taos_conn_task_t *taos_conn_task = (taos_conn_task_t*)task;
  if (taos_conn_task->taos) {
    D("taos_close(taos:%p) ...", taos_conn_task->taos);
    A(0, "internal logic error");
    taos_close(taos_conn_task->taos);
    D("taos_close(taos:%p) => void", taos_conn_task->taos);
    taos_conn_task->taos = NULL;
  }
}

static void tws_session_open_connect(tws_session_t *session)
{
  struct lws *wsi = session->wsi;

  if (session->taos) {
    tws_session_append_err(session, "already connected");
    return;
  }

  A(session->task == NULL, "internal logic error");

  struct lws_vhost *vhost = lws_get_vhost(wsi);
  const struct lws_protocols *protocols = lws_get_protocol(wsi);
  tws_vhost_data_t *vhd = (tws_vhost_data_t*)lws_protocol_vh_priv_get(vhost, protocols);
  taos_conn_task_t *task = &session->conn_task;

  task->task.session    = session;
  task->task.on_work    = taos_conn_task_on_work;
  task->task.on_done    = taos_conn_task_on_done;
  task->task.on_release = taos_conn_task_on_release;
  task->task.tick       = ++session->task_tick;

  session->task = &task->task;

  if (tws_vhost_data_post_task(vhd, &task->task)) {
    tws_session_append_err(session, "out of memory");
    task->task.on_release(&task->task);
    task->task.state = TASK_STATE_NONE;
    session->task = NULL;
    return;
  }

  lws_rx_flow_control(wsi, 0);
}

static struct lws_context *global_context = NULL;
static tws_vhost_data_t *global_vhd = NULL;

static void tws_wsi_on_wait_cancel(struct lws *wsi)
{
  A(wsi, "internal logic error");
  struct lws_vhost *vhost = lws_get_vhost(wsi);
  A(vhost, "internal logic error");
  const struct lws_protocols *protocols = lws_get_protocol(wsi);
  A(protocols, "internal logic error");
  tws_vhost_data_t *vhd = (tws_vhost_data_t*)lws_protocol_vh_priv_get(vhost, protocols);
  A(vhd, "internal logic error");
  A(vhd == global_vhd, "internal logic error");

  tws_vhost_data_wakeup_done_tasks(global_vhd);
}

static int tws_on_protocol_init(struct lws *wsi)
{
  struct lws_vhost *vhost = lws_get_vhost(wsi);
  const struct lws_protocols *protocols = lws_get_protocol(wsi);

  tws_vhost_data_t *vhd = (tws_vhost_data_t*)lws_protocol_vh_priv_zalloc(vhost, protocols, sizeof(*vhd));
  if (!vhd) {
    D("out of memory");
    return -1;
  }

  tws_vhost_data_init(vhd, wsi);

  global_vhd = vhd;
  return 0;
}

static int tws_on_protocol_destroy(struct lws *wsi)
{
  struct lws_vhost *vhost = lws_get_vhost(wsi);
  const struct lws_protocols *protocols = lws_get_protocol(wsi);
  tws_vhost_data_t *vhd = (tws_vhost_data_t*)lws_protocol_vh_priv_get(vhost, protocols);
  tws_vhost_data_release(vhd);

  return 0;
}

static int tws_session_on_established(tws_session_t *session)
{
  struct lws *wsi = session->wsi;

  char buf[1024]; buf[0] = '\0';
  lws_hdr_copy(wsi, buf, sizeof(buf), WSI_TOKEN_GET_URI);
  if (strcmp(buf, "/taosws")) {
    tws_session_append_err(session, "URI:expected /taosws, but got ==%s==", buf);
    return -1;
  }

  if (tws_session_init(session)) return -1;

  tws_session_open_connect(session);

  return 0;
}

static cJSON* cjson_get_by_paths_v(cJSON *json, va_list ap)
{
  cJSON *v = json;

  while (1) {
    const char *s = va_arg(ap, const char*);
    if (!s) break;
    v = cJSON_GetObjectItem(v, s);
    if (!v) return NULL;
  }

  return v;
}

//static cJSON* cjson_get_by_paths(cJSON *json, /*const char* path*/ ...)
//{
//  va_list ap;
//
//  va_start(ap, json);
//  json = cjson_get_by_paths_v(json, ap);
//  va_end(ap);
//
//  return json;
//}

static int cjson_get_string_by_paths(cJSON *json, const char **val, /*const char* path*/ ...)
{
  va_list ap;

  va_start(ap, val);
  json = cjson_get_by_paths_v(json, ap);
  va_end(ap);

  if (!json) return -1;
  if (!cJSON_IsString(json)) return -1;

  *val = cJSON_GetStringValue(json);

  return 0;
}

static int cjson_get_number_by_paths(cJSON *json, double *val, /*const char* path*/ ...)
{
  va_list ap;

  va_start(ap, val);
  json = cjson_get_by_paths_v(json, ap);
  va_end(ap);

  if (!json) return -1;
  if (!cJSON_IsNumber(json)) return -1;

  *val = cJSON_GetNumberValue(json);

  return 0;
}

static int tws_session_respond_json(tws_session_t *session, cJSON *json);

static void tws_session_query(tws_session_t *session, const char *sql)
{
  int r = 0;

  ds_stmt_t *stmt = (ds_stmt_t*)calloc(1, sizeof(*stmt));
  if (!stmt) {
    tws_session_append_err(session, "out of memory");
    return;
  }

  ds_stmt_init(stmt, session);

  do {
    lws_map_t *map = session->stmts_map;
    struct lws_map_item *p = lws_map_item_lookup(map, (const lws_map_key_t)stmt->uuid, sizeof(stmt->uuid));
    if (p) {
      tws_session_append_err(session, "internal logic error");
      break;
    }

    p = lws_map_item_create(map, (const lws_map_key_t)stmt->uuid, sizeof(stmt->uuid), (const lws_map_value_t)&stmt, sizeof(stmt));
    if (!p) {
      tws_session_append_err(session, "out of memory");
      break;
    }

    if (ds_stmt_query(stmt, sql)) break;

    lws_dll2_add_tail(&stmt->node, &session->stmts);

    cJSON *json = NULL;

    r = ds_stmt_gen_query_resp(stmt, &json);
    if (r) break;

    tws_session_respond_json(session, json);  // FIXME: return value
    return;
  } while (0);

  ds_stmt_release(stmt);
  SAFE_FREE(stmt);
}

static void tws_session_on_recv_req_query(tws_session_t *session, cJSON *req)
{
  const char *leading = "req/query";

  const char *sql = NULL;
  int r = cjson_get_string_by_paths(req, &sql, "sql", NULL);
  if (r) {
    tws_session_append_err(session, "`%s/sql` not found or not string in json packet", leading);
    return;
  }

  tws_session_query(session, sql);
}

static ds_stmt_t* tws_session_get_stmt(tws_session_t *session, const char *s_uuid, int *bad_uuid)
{
  int r = 0;

  uuid_t uuid;
  r = uuid_parse(s_uuid, uuid);
  if (r) {
    *bad_uuid = 1;
    tws_session_append_err(session, "uuid not valid:%s", s_uuid);
    return NULL;
  }

  ds_stmt_t *stmt = NULL;
  lws_map_t *map = session->stmts_map;
  struct lws_map_item *p = lws_map_item_lookup(map, (const lws_map_key_t)uuid, sizeof(uuid));
  if (!p) return NULL;

  stmt = *(ds_stmt_t**)lws_map_item_value(p);
  A(stmt, "internal logic error");
  size_t sz = lws_map_item_value_len(p);
  A(sz == sizeof(stmt), "internal logic error");
  // A(uuid == stmt->uuid, "internal logic error");

  return stmt;
}

static void tws_session_stmt_close(tws_session_t *session, const char *s_uuid)
{
  (void)ds_stmt_fetch_block;

  int bad_uuid = 0;
  ds_stmt_t *stmt = tws_session_get_stmt(session, s_uuid, &bad_uuid);
  if (!stmt) {
    if (bad_uuid) {
      tws_session_append_err(session, "uuid not valid:%s", s_uuid);
      return;
    }
  } else {
    ds_stmt_release(stmt);
    SAFE_FREE(stmt);
  }

  tws_session_respond_ok(session, "");
}

static void tws_session_on_recv_req_stmt_close(tws_session_t *session, cJSON *req)
{
  const char *leading = "req/stmt_close";

  const char *s_uuid = NULL;
  int r = cjson_get_string_by_paths(req, &s_uuid, "uuid", NULL);
  if (r) {
    tws_session_append_err(session, "`%s/uuid` not found or not string in json packet", leading);
    return;
  }

  tws_session_stmt_close(session, s_uuid);
}

static void tws_session_stmt_fetch(tws_session_t *session, const char *s_uuid)
{
  (void)ds_stmt_fetch_block;

  int bad_uuid = 0;
  ds_stmt_t *stmt = tws_session_get_stmt(session, s_uuid, &bad_uuid);
  if (!stmt) {
    if (bad_uuid) return;
    tws_session_append_err(session, "statement not found:%s", s_uuid);
    return;
  }

  tws_session_append_err(session, "not implemented yet");
}

static void tws_session_on_recv_req_stmt_fetch(tws_session_t *session, cJSON *req)
{
  const char *leading = "req/stmt_fetch";

  const char *s_uuid = NULL;
  int r = cjson_get_string_by_paths(req, &s_uuid, "uuid", NULL);
  if (r) {
    tws_session_append_err(session, "`%s/uuid` not found or not string in json packet", leading);
    return;
  }

  tws_session_stmt_fetch(session, s_uuid);
}

static void foo_task_on_work(tws_task_t *task)
{
  foo_task_t *foo_task = (foo_task_t*)task;
  D("sleeping %d secs...", foo_task->secs_to_sleep);
  for (int i=0; i<foo_task->secs_to_sleep; ++i) {
    sleep(1);
    if (task->session->terminating) break;
    if (interrupted) break;
  }
  D("sleeping %d secs done", foo_task->secs_to_sleep);
}

static void foo_task_on_done(tws_task_t *task)
{
  tws_session_t *session = task->session;
  foo_task_t *foo_task = (foo_task_t*)task;
  tws_session_respond_ok(session, "success:%" PRIu64 "", foo_task->tick);
}

static void foo_task_on_release(tws_task_t *task)
{
  foo_task_t *foo_task = (foo_task_t*)task;
  (void)foo_task;
}

static void tws_session_foo(tws_session_t *session, int secs_to_sleep)
{
  struct lws *wsi = session->wsi;

  A(session->task == NULL, "internal logic error");

  struct lws_vhost *vhost = lws_get_vhost(wsi);
  const struct lws_protocols *protocols = lws_get_protocol(wsi);
  tws_vhost_data_t *vhd = (tws_vhost_data_t*)lws_protocol_vh_priv_get(vhost, protocols);
  foo_task_t *task = &session->foo_task;

  task->secs_to_sleep = secs_to_sleep;

  task->task.session    = session;
  task->task.on_work    = foo_task_on_work;
  task->task.on_done    = foo_task_on_done;
  task->task.on_release = foo_task_on_release;
  task->task.tick       = ++session->task_tick;
  task->tick            = ++session->foo_tick;

  session->task = &task->task;

  if (tws_vhost_data_post_task(vhd, &task->task)) {
    tws_session_append_err(session, "out of memory");
    task->task.on_release(&task->task);
    task->task.state = TASK_STATE_NONE;
    session->task = NULL;
    return;
  }

  lws_rx_flow_control(wsi, 0);
}

static void tws_session_on_recv_req_foo(tws_session_t *session, cJSON *req)
{
  const char *leading = "req/foo";

  double secs_to_sleep = 0;
  int r = cjson_get_number_by_paths(req, &secs_to_sleep, "secs_to_sleep", NULL);
  if (r) {
    tws_session_append_err(session, "`%s/secs_to_sleep` not found or not number in json packet", leading);
    return;
  }

  if (secs_to_sleep < 0) {
    tws_session_append_err(session, "`%s/secs_to_sleep:%f` invalid", leading, secs_to_sleep);
    return;
  }

  tws_session_foo(session, (int)secs_to_sleep);
}

static int tws_session_on_recv_req(tws_session_t *session, cJSON *req)
{
  cJSON *v = NULL;
  if ((v=cJSON_GetObjectItem(req, "query"))) {
    tws_session_on_recv_req_query(session, v);
    return 0;
  }

  if ((v=cJSON_GetObjectItem(req, "stmt_close"))) {
    tws_session_on_recv_req_stmt_close(session, v);
    return 0;
  }

  if ((v=cJSON_GetObjectItem(req, "stmt_fetch"))) {
    tws_session_on_recv_req_stmt_fetch(session, v);
    return 0;
  }

  if ((v=cJSON_GetObjectItem(req, "foo"))) {
    tws_session_on_recv_req_foo(session, v);
    return 0;
  }

  tws_session_append_err(session, "unknown json packet");
  return -1;
}

static int tws_session_append_json_in(tws_session_t *session, cJSON *json)
{
  D("in-comming json needs to be queued because of pending-task in place");
  tws_jio_t *jio = NULL;
  lws_dll2_t *p = lws_dll2_get_head(&session->free_jios);
  if (!p) {
    jio = (tws_jio_t*)calloc(1, sizeof(*jio));
    if (!jio) {
      tws_session_append_err(session, "out of memory");
      return -1;
    }
  } else {
    lws_dll2_remove(p);
    jio = lws_container_of(p, tws_jio_t, node);
  }

  jio->json = json;

  lws_dll2_add_tail(&jio->node, &session->pending_jins);

  return 0;
}

static int tws_session_append_json_out(tws_session_t *session, cJSON *json)
{
  D("out-going json needs to be queued because of pending-io in place");
  tws_jio_t *jio = NULL;
  lws_dll2_t *p = lws_dll2_get_head(&session->free_jios);
  if (!p) {
    jio = (tws_jio_t*)calloc(1, sizeof(*jio));
    if (!jio) {
      // FIXME:
      tws_session_append_err(session, "out of memory");
      return -1;
    }
  } else {
    lws_dll2_remove(p);
    jio = lws_container_of(p, tws_jio_t, node);
  }

  jio->json = json;

  lws_dll2_add_tail(&jio->node, &session->pending_jouts);

  return 0;
}

static int tws_session_on_recv_json(tws_session_t *session, cJSON *json)
{
  A(session->task == NULL, "internal logic error");

  int r = 0;
  cJSON *req = cJSON_GetObjectItem(json, "req");
  if (req) {
    r =  tws_session_on_recv_req(session, req);
  } else {
    tws_session_append_err(session, "`req` not found in json packet");
    r = -1;
  }
  cJSON_Delete(json);
  return r ? -1 : 0;
}

static int tws_session_on_recv(tws_session_t *session, void *in, size_t len)
{
  int r = 0;

  struct lws *wsi = session->wsi;

  if (session->terminating) return 0;

  if (interrupted) {
    tws_session_append_err(session, "service is shutting down");
    return -1;
  }

  if (lws_is_first_fragment(wsi)) {
    session->recv_nr = 0;
  }

  if (session->recv_nr + len >= session->recv_cap) {
    size_t cap = (session->recv_nr + len + 1 + 255) / 256 * 256;
    char *buf = (char*)realloc(session->recv_buf, cap);
    if (!buf) {
      tws_session_append_err(session, "out of memory");
      return -1;
    }
    session->recv_buf = buf;
    session->recv_cap = cap;
  }

  memcpy(session->recv_buf + session->recv_nr, in, len);
  session->recv_nr += len;

  if (!lws_is_final_fragment(wsi)) return 0;

  cJSON *json = cJSON_ParseWithLength(session->recv_buf, session->recv_nr);
  if (!json) {
    // const char *s = cJSON_GetErrorPtr();
    tws_session_append_err(session, "json parsing `%.*s` failure", (int)len, (const char*)in);
    return -1;
  }

  if (session->task) {
    r = tws_session_append_json_in(session, json);
    if (r) {
      cJSON_Delete(json);
      return -1;
    }
    return 0;
  }

  return tws_session_on_recv_json(session, json);
}

static int _tws_errs_reclaim_err(lws_dll2_t *d, void *user)
{
  tws_session_t *session = (tws_session_t*)user;
  lws_dll2_remove(d);
  tws_err_t *err = lws_container_of(d, tws_err_t, node);
  lws_dll2_add_tail(&err->node, &session->free_errs);
  return 0;
}

static int tws_session_on_writeable(tws_session_t *session)
{
  struct lws *wsi = session->wsi;
  tws_jout_t *jout   = &session->current_jout;

  int r = tws_session_serialize_output(session);
  A(r == 0, "internal logic error");
  if (jout->nr_buf_out || jout->nr_chunk || (!jout->frame_end)) return 0;

  tws_json_serializer_release(&session->serializer);

  A(jout->s_chunk == NULL, "internal logic error");
  A(jout->current_out, "internal logic error");
  cJSON_Delete(jout->current_out);
  jout->current_out = NULL;
  jout->frame_chunks = 0;
  jout->frame_end = 0;
  lws_dll2_foreach_safe(&session->current_jout.errs, session, _tws_errs_reclaim_err);
  jout->arr = NULL;

  lws_dll2_t *p = lws_dll2_get_head(&session->pending_jouts);
  if (p) {
    lws_dll2_remove(p);
    tws_jio_t *jio = lws_container_of(p, tws_jio_t, node);
    A(jio->json, "internal logic error");
    cJSON *json = jio->json;
    jio->json = NULL;
    lws_dll2_add_tail(&jio->node, &session->free_jios);
    return tws_session_serialize_json_out(session, json);
  }

  if (session->terminating) lws_set_timeout(wsi, 0, LWS_TO_KILL_ASYNC);

  if (session->task == NULL) {
    p = lws_dll2_get_head(&session->pending_jins);
    if (p) {
      lws_dll2_remove(p);
      tws_jio_t *jio = lws_container_of(p, tws_jio_t, node);
      cJSON *json = jio->json;
      jio->json = NULL;
      lws_dll2_add_tail(&jio->node, &session->free_jios);

      return tws_session_on_recv_json(session, json);
    }
  }

  lws_rx_flow_control(session->wsi, 1);

  return 0;
}

static int _tws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len)
{
#define DC(fmt, ...) D("wsi:%p;user:%p;global_context:%p;%s", wsi, user, global_context, _lws_callback_reason_name(reason))

  int r = 0;

  tws_session_t *session = (tws_session_t*)user;

  switch (reason) {
    case LWS_CALLBACK_PROTOCOL_INIT:
      DC("");
      A(session == NULL, "internal logic error");
      if (tws_on_protocol_init(wsi)) return -1;
      break;

    case LWS_CALLBACK_PROTOCOL_DESTROY:
      DC("");
      A(session == NULL, "internal logic error");
      if (tws_on_protocol_destroy(wsi)) return -1;
      break;

    case LWS_CALLBACK_ESTABLISHED:
      DC("");
      A(session->wsi == NULL, "internal logic error");
      session->wsi = wsi;
      A(session->current_io == NULL, "internal logic error");
      session->current_io = tws_session_push_io(session);
      if (!session->current_io) {
        D("out of memory");
        return -1;
      }
      r = tws_session_on_established(session);
      tws_session_pop_io(session);
      if (r) {
        session->terminating = 1;
        lws_callback_on_writable(wsi);
        return 0;
      }
      break;

    case LWS_CALLBACK_CLOSED:
      DC("");
      A(session->wsi == wsi, "internal logic error");
      tws_session_release(session);
      break;

    case LWS_CALLBACK_CONFIRM_EXTENSION_OKAY:
      DC("");
      break;

    case LWS_CALLBACK_SERVER_WRITEABLE:
      DC("");
      A(session->wsi == wsi, "internal logic error");
      A(session->current_io == NULL, "internal logic error");
      session->current_io = tws_session_push_io(session);
      if (!session->current_io) {
        D("out of memory");
        return -1;
      }
      r = tws_session_on_writeable(session);
      tws_session_pop_io(session);
      if (r) {
        session->terminating = 1;
        lws_callback_on_writable(wsi);
        return 0;
      }
      break;

    case LWS_CALLBACK_RECEIVE:
      DC("");
      A(session->wsi == wsi, "internal logic error");
      A(session->current_io == NULL, "internal logic error");
      session->current_io = tws_session_push_io(session);
      if (!session->current_io) {
        D("out of memory");
        return -1;
      }
      r = tws_session_on_recv(session, in, len);
      tws_session_pop_io(session);
      if (r) {
        session->terminating = 1;
        lws_callback_on_writable(wsi);
        return 0;
      }
      break;

    case LWS_CALLBACK_EVENT_WAIT_CANCELLED:
      DC("");
      A(session == NULL, "internal logic error");
      tws_wsi_on_wait_cancel(wsi);
      break;

    case LWS_CALLBACK_FILTER_PROTOCOL_CONNECTION:
      DC("");
      break;

    default:
      DC("");
      break;
  }

#undef DC

  return 0;
}

static int tws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len)
{
  int r = 0;

  r = _tws_callback(wsi, reason, user, in, len);

  return r;
}

static int run(int argc, char **argv)
{
  (void)argc;
  (void)argv;

  static struct lws_protocols protocols[] = {
    {
      "http-only",
      lws_callback_http_dummy,
      0, 0, 0, NULL, 0
    },{
      "taosws-protocol",
      tws_callback,
      sizeof(struct tws_session_s),
      0,
      0, NULL, 0
    },
    LWS_PROTOCOL_LIST_TERM
  };

  struct lws_context_creation_info info = {
    .port = 3001,
    .protocols = protocols,
  };

  struct lws_context *context = lws_create_context(&info);

  if (!context) {
    printf("Failed to create WebSocket context.\n");
    return -1;
  }

  global_context = context;

  while (!interrupted) {
    lws_service(context, 0);
  }

  if (global_vhd) {
    tws_queue_workers_stop(&global_vhd->workers);
  }

  lws_context_destroy(context);

  return 0;
}

void sigint_handler(int sig)
{
  (void)sig;

	interrupted = 1;
  lws_cancel_service(global_context);
}

int main(int argc, char **argv)
{
  int r = 0;

	signal(SIGINT, sigint_handler);
	signal(SIGHUP, sigint_handler);

  D("taos_init() ...");
  r = taos_init();
  D("taos_init() => %d", r);
  if (r == 0) {
    r = run(argc, argv);
    D("taos_clenaup() ...");
    taos_cleanup();
    D("taos_clenaup() => void");
  }

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}


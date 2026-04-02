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

#include "odbc_helpers.h"

#include <stdarg.h>
#include <stdint.h>
#ifdef _WIN32          /* { */
#include <process.h>
#endif                 /* }{ */

#define DUMP(fmt, ...)          printf(fmt "\n", ##__VA_ARGS__)

typedef enum thread_create_e      thread_create_t;
enum thread_create_e {
  USE_UNKNOWN,
  USE_CreateThread,
  USE__beginthreadex,
  USE_pthread_create,
};

typedef int (*test_f)(SQLSMALLINT HandleType, SQLHANDLE Handle);

typedef struct sql_s                 sql_t;
struct sql_s {
  const char                *sql;
  uint8_t                    forcibly:1;
};

static int execute_sqls_with_stmt(SQLHANDLE hstmt, const char **sqls, size_t nr)
{
  SQLRETURN sr = SQL_SUCCESS;

  for (size_t i=0; i<nr; ++i) {
    const char *sql = sqls[i];
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql, SQL_NTS);
    if (sr != SQL_SUCCESS && sr != SQL_SUCCESS_WITH_INFO) return -1;
    CALL_SQLCloseCursor(hstmt);
  }

  return 0;
}

static int _test_threads(SQLSMALLINT HandleType, SQLHANDLE Handle)
{
  // SQLRETURN sr = SQL_SUCCESS;
  int r = 0;
  if (HandleType != SQL_HANDLE_STMT) return 0;

  // DUMP("");
  // DUMP("%s:", __func__);

  SQLHANDLE hstmt = Handle;

  if (1) {
    const char *sqls[] = {
      "select * from information_schema.ins_configs",
    };

    r = execute_sqls_with_stmt(hstmt, sqls, sizeof(sqls)/sizeof(sqls[0]));
    if (r) return -1;
  }

  return 0;
}

#define RECORD(x) {x, #x}

static struct {
  test_f                func;
  const char           *name;
} _tests[] = {
  RECORD(_test_threads),
};

typedef struct arg_s             arg_t;
struct arg_s {
  const char      *dsn;
  const char      *uid;
  const char      *pwd;
  const char      *connstr;
  int              main;
  int              threads;
  int              executes;
  int              progress;
  thread_create_t  thread_create;

  const char      *name;
};

static int _connect(SQLHANDLE hconn, const char *dsn, const char *uid, const char *pwd)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLConnect(hconn, (SQLCHAR*)dsn, SQL_NTS, (SQLCHAR*)uid, SQL_NTS, (SQLCHAR*)pwd, SQL_NTS);
  if (FAILED(sr)) {
    E("connect [dsn:%s,uid:%s,pwd:%s] failed", dsn, uid, pwd);
    return -1;
  }

  return 0;
}

static int _driver_connect(SQLHANDLE hconn, const char *connstr)
{
  SQLRETURN sr = SQL_SUCCESS;

  sr = CALL_SQLDriverConnect(hconn, NULL, (SQLCHAR*)connstr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  if (FAILED(sr)) {
    E("driver_connect [connstr:%s] failed", connstr);
    return -1;
  }

  return 0;
}

static int test_with_stmt(const arg_t *arg, SQLHANDLE hstmt)
{
  int r = 0;

  for (size_t i=0; i<sizeof(_tests)/sizeof(_tests[0]); ++i) {
    if (arg->name == NULL || strcmp(arg->name, _tests[i].name)==0) {
      r = _tests[i].func(SQL_HANDLE_STMT, hstmt);
      if (r) return -1;
    }
  }

  return 0;
}

static int execute_sqls(SQLHANDLE hstmt, sql_t *sqls, size_t nr)
{
  SQLRETURN sr = SQL_SUCCESS;

  for (size_t i=0; i<nr; ++i) {
    sql_t *sql = sqls + i;
    sr = CALL_SQLExecDirect(hstmt, (SQLCHAR*)sql->sql, SQL_NTS);
    CALL_SQLCloseCursor(hstmt);
    if (sr == SQL_SUCCESS || sr == SQL_SUCCESS_WITH_INFO) continue;
    if (sql->forcibly) return -1;
  }

  return 0;
}

static int test_with_connected_conn(const arg_t *arg, SQLHANDLE hconn, sql_t *sqls, size_t nr)
{
  SQLRETURN sr = SQL_SUCCESS;
  int r = 0;

  for (size_t i=0; i<sizeof(_tests)/sizeof(_tests[0]); ++i) {
    if (arg->name == NULL || strcmp(arg->name, _tests[i].name)==0) {
      r = _tests[i].func(SQL_HANDLE_DBC, hconn);
      if (r) return -1;
    }
  }

  SQLHANDLE hstmt;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_STMT, hconn, &hstmt);
  if (sr != SQL_SUCCESS) return SQL_ERROR;

  r = execute_sqls(hstmt, sqls, nr);
  if (r == 0) r = test_with_stmt(arg, hstmt);

  CALL_SQLFreeHandle(SQL_HANDLE_STMT, hstmt);

  return r ? -1 : 0;
}

static int test_with_conn(const arg_t *arg, SQLHANDLE hconn, sql_t *sqls, size_t nr)
{
  int r = 0;

  if (arg->connstr) {
    r = _driver_connect(hconn, arg->connstr);
    if (r) return -1;
  } else {
    r = _connect(hconn, arg->dsn, arg->uid, arg->pwd);
    if (r) return -1;
  }

  r = test_with_connected_conn(arg, hconn, sqls, nr);

  CALL_SQLDisconnect(hconn);

  return r ? -1 : 0;
}

static int test_with_env(const arg_t *arg, SQLHANDLE henv, sql_t *sqls, size_t nr)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE hconn = SQL_NULL_HANDLE;
  sr = CALL_SQLAllocHandle(SQL_HANDLE_DBC, henv, &hconn);
  if (FAILED(sr)) return -1;

  r = test_with_conn(arg, hconn, sqls, nr);

  CALL_SQLFreeHandle(SQL_HANDLE_DBC, hconn);

  return r;
}

static int _run_with_arg_in_thread(const arg_t *arg, sql_t *sqls, size_t nr)
{
  int r = 0;
  SQLRETURN sr = SQL_SUCCESS;

  SQLHANDLE henv  = SQL_NULL_HANDLE;

  sr = CALL_SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  if (FAILED(sr)) return 1;

  do {
    sr = CALL_SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (FAILED(sr)) { r = -1; break; }

    r = test_with_env(arg, henv, sqls, nr);
    if (r) break;
  } while (0);

  CALL_SQLFreeHandle(SQL_HANDLE_ENV, henv);

  return r;
}

static int _tick = 0;
static void _init_tick(void)
{
  A(_tick == 0, "");
  ++_tick;
}

static void _init_pthread_once(void)
{
  static pthread_once_t _once = PTHREAD_ONCE_INIT;
  pthread_once(&_once, _init_tick);
}

static int _thread_routine(const arg_t *arg)
{
  int r = 0;
  _init_pthread_once();
  _init_pthread_once();
  W("%zd:%zx:thread:running", tod_get_current_process_id(), tod_get_current_thread_id());
  r = _run_with_arg_in_thread((const arg_t*)arg, NULL, 0);
  W("%zd:%zx:thread:return:%d", tod_get_current_process_id(), tod_get_current_thread_id(), r);
  if (r) return -1;
  return 0;
}

#ifdef _WIN32                /* { */
static DWORD WINAPI _routine_CreateThread(LPVOID arg)
{
  int r = 0;
  r = _thread_routine((const arg_t*)arg);
  if (r) return -1;
  return 0;
}

static int _run_with_arg_CreateThread(const arg_t *arg)
{
  HANDLE threads[16]   = {0};
  DWORD dwThreadIds[16] = {0};
  DWORD exitCodes[16]   = {0};

  int r = 0;
  size_t nr = 0;

  r = 0;
  nr = 0;

  size_t count = arg->threads;
  for (size_t i=0; i<count && i < sizeof(threads)/sizeof(threads[0]); ++i) {
    threads[i] = CreateThread(NULL, 0, _routine_CreateThread, (LPVOID)arg, CREATE_SUSPENDED, dwThreadIds + i);
    W("%d:%x:thread:create", GetProcessId(GetCurrentProcess()), GetThreadId(threads[i]));
    if (!threads[i]) {
      E("CreateThread failed");
      r = -1;
      break;
    }
    ResumeThread(threads[i]);
    ++nr;
  }

  // WaitForMultipleObjects((DWORD)nr, threads, TRUE, INFINITE);
  for (size_t i=0; i<nr; ++i) {
    DWORD dw = WaitForSingleObject(threads[i], INFINITE);
    W("%d:%x:thread:join:%d", GetProcessId(GetCurrentProcess()), GetThreadId(threads[i]), dw);
    GetExitCodeThread(threads[i], exitCodes + i);
    if (exitCodes[i]) r = -1;
    CloseHandle(threads[i]);
  }

  return r;
}

static unsigned __stdcall _routine__beginthreadex(void *arg)
{
  int r = 0;
  r = _thread_routine((const arg_t*)arg);
  if (r) return -1;
  return 0;
}

static int _run_with_arg__beginthreadex(const arg_t *arg)
{
  HANDLE threads[16]   = {0};
  unsigned threadIds[16] = {0};
  DWORD exitCodes[16]   = {0};

  int r = 0;
  size_t nr = 0;

  r = 0;
  nr = 0;

  size_t count = arg->threads;
  for (size_t i=0; i<count && i < sizeof(threads)/sizeof(threads[0]); ++i) {
    threads[i] = (HANDLE)_beginthreadex(NULL, 0, _routine__beginthreadex, (void*)arg, CREATE_SUSPENDED, threadIds + i);
    W("%d:%x:thread:create", GetProcessId(GetCurrentProcess()), GetThreadId(threads[i]));
    if (!threads[i]) {
      E("_beginthreadex failed");
      r = -1;
      break;
    }
    ResumeThread(threads[i]);
    ++nr;
  }

  for (size_t i=0; i<nr; ++i) {
    DWORD dw = WaitForSingleObject(threads[i], INFINITE);
    W("%d:%x:thread:join:%d", GetProcessId(GetCurrentProcess()), GetThreadId(threads[i]), dw);
    GetExitCodeThread(threads[i], exitCodes + i);
    if (exitCodes[i]) r = -1;
    CloseHandle(threads[i]);
  }

  return r;
}
#else                        /* }{ */
#include <pthread.h>
static void* _routine_pthread_create(void *arg)
{
  int r = 0;
  r = _thread_routine((const arg_t*)arg);
  if (r) return (void*)-1;
  return 0;
}

static int _run_with_arg_pthread_create(const arg_t *arg)
{
  pthread_t threads[16]     = {0};

  int r = 0;
  size_t nr = 0;

  r = 0;
  nr = 0;

  size_t count = arg->threads;
  for (size_t i=0; i<count && i < sizeof(threads)/sizeof(threads[0]); ++i) {
    r = pthread_create(threads + i, NULL, _routine_pthread_create, (void*)arg);
    if (r) {
      E("pthread_create failed");
      r = -1;
      break;
    }
    ++nr;
  }

  for (size_t i=0; i<nr; ++i) {
    void *p = NULL;
    pthread_join(threads[i], &p);
    if (p) r = -1;
  }

  return r;
}
#endif                       /* } */

static int _run_with_arg_once(const arg_t *arg)
{
  if (arg->threads == 0) return 0;

#ifdef _WIN32                /* { */
  switch (arg->thread_create) {
    case USE_CreateThread:
      return _run_with_arg_CreateThread(arg);
    case USE__beginthreadex:
      return _run_with_arg__beginthreadex(arg);
    default:
      E("internal logic error");
      return -1;
  }
#else                        /* }{ */
  switch (arg->thread_create) {
    case USE_pthread_create:
      return _run_with_arg_pthread_create(arg);
    default:
      E("internal logic error");
      return -1;
  }
#endif                       /* } */
}

static int _run_with_arg(const arg_t *arg, sql_t *sqls, size_t nr)
{
  int r = 0;
  if (arg->main) r = _run_with_arg_in_thread(arg, sqls, nr);
  if (r) return -1;

  int tick = 0;

  int interval = 5;
  const time_t t_0 = time(NULL) - interval;
  time_t t0 = t_0;

again:

  if (tick++ < arg->executes) {
    r = _run_with_arg_once(arg);
    if (r) return -1;
    time_t t1 = time(NULL);
    double delta = difftime(t1, t0);
    if (arg->progress && delta >= interval) {
      struct tm v;
      localtime_r(&t1, &v);
      char buf[64];
      strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &v);
      DUMP("%s:executed:%d;avg:%.3f", buf, tick, ((double)tick)/difftime(t1, t_0));
      t0 = t1;
    }
    goto again;
  }

  return 0;
}

static void usage(const char *arg0)
{
  DUMP("usage:");
  DUMP("  %s -h", arg0);
  DUMP("  %s [options] [name]...", arg0);
  DUMP("options:");
  DUMP("  --dsn <DSN>                 DSN");
  DUMP("  --uid <UID>                 UID");
  DUMP("  --pwd <PWD>                 PWD");
  DUMP("  --connstr <connstr>         Connection string");
  DUMP("  --no-main                   not running in main thread");
  DUMP("  --threads <num>             # of threads running concurrently");
  DUMP("  --executes <num>            # of executes for each test");
  DUMP("  --progress                  show progress");
#ifdef _WIN32                              /* { */
  DUMP("  --CreateThread              Using `CreateThread`");
  DUMP("  --_beginthreadex            Using `_beginthreadex`");
#else                                      /* }{ */
  DUMP("  pthread_create              Using `pthread_create`");
#endif                                     /* { */
  DUMP("");
  DUMP("supported test names:");
  for (size_t i=0; i<sizeof(_tests)/sizeof(_tests[0]); ++i) {
    DUMP("  %s", _tests[i].name);
  }
}

static int _run(int argc, char *argv[], const char *dsn)
{
  int r = 0;
  arg_t arg = {0};
  arg.dsn = dsn;
  arg.main = 1;
  arg.threads = 8;
  arg.executes = 2;
  arg.progress = 0;
#ifdef _WIN32                /* { */
  arg.thread_create = USE_CreateThread;
#else                        /* }{ */
  arg.thread_create = USE_pthread_create;
#endif                       /* } */

  sql_t sqls[] = {
    { "drop topic if exists demo", 0},
    { "drop topic if exists good", 0},
    { "drop database if exists foobar", 1},
    { "create database if not exists foobar WAL_RETENTION_PERIOD 2592000", 1},
    { "create table foobar.demo (ts timestamp, name varchar(20))", 1},
    { "create topic demo as select * from foobar.demo", 1},
    { "create table foobar.good (ts timestamp, i32 int, name varchar(20))", 1},
    { "create topic good as select * from foobar.good", 1},
    { "insert into foobar.demo (ts, name) values (now(), 'hello')", 1},
  };

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-h") == 0) {
      usage(argv[0]);
      return 0;
    }
    if (strcmp(argv[i], "--dsn") == 0) {
      ++i;
      if (i>=argc) break;
      arg.dsn = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--uid") == 0) {
      ++i;
      if (i>=argc) break;
      arg.uid = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--pwd") == 0) {
      ++i;
      if (i>=argc) break;
      arg.pwd = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--connstr") == 0) {
      ++i;
      if (i>=argc) break;
      arg.connstr = argv[i];
      continue;
    }
    if (strcmp(argv[i], "--no-main") == 0) {
      arg.main = 0;
      continue;
    }
    if (strcmp(argv[i], "--threads") == 0) {
      ++i;
      if (i>=argc) break;
      arg.threads = atoi(argv[i]);
      continue;
    }
    if (strcmp(argv[i], "--executes") == 0) {
      ++i;
      if (i>=argc) break;
      arg.executes = atoi(argv[i]);
      continue;
    }
    if (strcmp(argv[i], "--progress") == 0) {
      arg.progress = 1;
      continue;
    }
#ifdef _WIN32                              /* { */
    if (strcmp(argv[i], "--CreateThread") == 0) {
      arg.thread_create = USE_CreateThread;
      continue;
    }
    if (strcmp(argv[i], "--_beginthreadex") == 0) {
      arg.thread_create = USE__beginthreadex;
      continue;
    }
#else                                      /* }{ */
    if (strcmp(argv[i], "--pthread_create") == 0) {
      arg.thread_create = USE_pthread_create;
      continue;
    }
#endif                                     /* { */

    arg.name = argv[i];
    r = _run_with_arg(&arg, sqls, sizeof(sqls)/sizeof(sqls[0]));
    if (r) return -1;
  }

  if (arg.name) return 0;

  return _run_with_arg(&arg, sqls, sizeof(sqls)/sizeof(sqls[0]));
}

int main(int argc, char *argv[])
{
  int r = 0;

#ifndef FAKE_TAOS
  if (r == 0) r = _run(argc, argv, "TAOS_ODBC_DSN");
#endif

#ifdef HAVE_TAOSWS                /* { */
  // FIXME: segfault on cli-windows / svr-linux
  //        don't know why, need to fix it later
  // Info:
  // Windows: taos -V
  //          version: 3.2.1.0 compatible_version: 3.0.0.0
  //          gitinfo: 234463fcca65f3f1d08a1f245570e4e5d5d272e2
  //          buildInfo: Built Windows-x64 at 2023-11-16 14:45:10
  //          taosws.dll, don't know how to get version info of it's source code
  // NOTE: bypass this test case for the moment!
  // 2023-12-04

  if (r == 0) r = _run(argc, argv, "TAOS_ODBC_WS_DSN");
#endif                            /* } */

  fprintf(stderr, "==%s==\n", r ? "failure" : "success");

  return !!r;
}

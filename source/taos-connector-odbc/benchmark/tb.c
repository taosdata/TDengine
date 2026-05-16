#include "taos_helpers.h"

#include <errno.h>

#define DUMP(fmt, ...) fprintf(stderr, "[%d]:%s():" fmt "\n", __LINE__, __func__, ##__VA_ARGS__)

#define safe_free(x) do {      \
  if (x == NULL) break;        \
  free(x);                     \
  x = NULL;                    \
} while (0)

#ifdef _WIN32        /* { */
#define strcasecmp       _stricmp
#endif               /* } */

static void usage(const char *app)
{
  fprintf(stderr, "usage:\n");
  fprintf(stderr, "%s -h\n", app);
}

typedef struct handles_s                handles_t;
struct handles_s {
  TAOS                  *taos;
  TAOS_RES              *res;
  int                    precision;
  int                    affected_rows;
  int                    nr_fields;
  TAOS_FIELD            *fields;
  int                    nr_rows;
  TAOS_ROW               rows;
};

static void handles_free_result_set(handles_t *handles)
{
  if (handles->res) {
    CALL_taos_free_result(handles->res);
    handles->res = NULL;
  }
}

static void handles_disconnect(handles_t *handles)
{
  handles_free_result_set(handles);
  if (handles->taos) {
    CALL_taos_close(handles->taos);
    handles->taos = NULL;
  }
}

static void handles_release(handles_t *handles)
{
  handles_disconnect(handles);
}

static int handles_connect(handles_t *handles, const char *ip, const char *user, const char *pass, const char *db, uint16_t port)
{
  int e = 0;
  handles_disconnect(handles);
  handles->taos = CALL_taos_connect(ip, user, pass, db, port);
  if (!handles->taos) {
    e = taos_errno(NULL);
    DUMP("taos_connect failed:[0x%x]%s", e, taos_errstr(NULL));
    return -1;
  }

  return 0;
}

static int handles_read_as_varchar(handles_t *handles, int display, int i, int j)
{
  if (display ? taos_is_null(handles->res, i, j) : CALL_taos_is_null(handles->res, i, j) ) {
    if (display) fprintf(stderr, "[null]");
    return 0;
  }
  int *offsets = display ? taos_get_column_data_offset(handles->res, j) : CALL_taos_get_column_data_offset(handles->res, j);
  char *col = (char*)(handles->rows[j]);
  col += offsets[i];
  int16_t length = *(int16_t*)col;
  col += sizeof(int16_t);
  if (display) fprintf(stderr, "%.*s", length, col);
  return 0;
}

static int handles_read_as_int(handles_t *handles, int display, int i, int j)
{
  if (display ? taos_is_null(handles->res, i, j) : CALL_taos_is_null(handles->res, i, j) ) {
    if (display) fprintf(stderr, "[null]");
    return 0;
  }
  int32_t *col = (int32_t*)handles->rows[j];
  col += i;
  if (display) fprintf(stderr, "%d", *col);
  return 0;
}

/*
D:987267.693s:26c6cb:tsdb.c[368]:_query():taos_query(taos:0x558118f2b110,sql:select name,i32 from foo.t limit 1) ...
D:987269.438s:26c6cb:tsdb.c[368]:_query():taos_query(taos:0x558118f2b110,sql:select name,i32 from foo.t limit 1) => 0x558118f362f0
D:987269.521s:26c6cb:tsdb.c[338]:_stmt_post_query():taos_result_precision(res:0x558118f362f0) ...
D:987269.656s:26c6cb:tsdb.c[338]:_stmt_post_query():taos_result_precision(res:0x558118f362f0) => 0
D:987269.798s:26c6cb:tsdb.c[343]:_stmt_post_query():taos_affected_rows(res:0x558118f362f0) ...
D:987269.864s:26c6cb:tsdb.c[343]:_stmt_post_query():taos_affected_rows(res:0x558118f362f0) => 0
D:987269.942s:26c6cb:tsdb.c[344]:_stmt_post_query():taos_field_count(res:0x558118f362f0) ...
D:987270.071s:26c6cb:tsdb.c[344]:_stmt_post_query():taos_field_count(res:0x558118f362f0) => 2
D:987270.137s:26c6cb:tsdb.c[346]:_stmt_post_query():taos_fetch_fields(res:0x558118f362f0) ...
D:987270.322s:26c6cb:tsdb.c[346]:_stmt_post_query():taos_fetch_fields(res:0x558118f362f0) => 0x7f2df8003920
D:987270.495s:26c6cb:tsdb.c[935]:_tsdb_stmt_fetch_rows_block():taos_fetch_block(res:0x558118f362f0,rows:0x7ffd1c3adff0) ...
D:987270.964s:26c6cb:tsdb.c[935]:_tsdb_stmt_fetch_rows_block():taos_fetch_block(res:0x558118f362f0,rows:0x7ffd1c3adff0(0x7f2df4000f90)) => 1
D:987271.040s:26c6cb:taos_helpers.c[165]:helper_get_tsdb():taos_is_null(res:0x558118f362f0,row:0,col:0) ...
D:987271.104s:26c6cb:taos_helpers.c[165]:helper_get_tsdb():taos_is_null(res:0x558118f362f0,row:0,col:0) => false
D:987271.153s:26c6cb:taos_helpers.c[243]:helper_get_tsdb():taos_get_column_data_offset(res:0x558118f362f0,columnIndex:0) ...
D:987271.216s:26c6cb:taos_helpers.c[243]:helper_get_tsdb():taos_get_column_data_offset(res:0x558118f362f0,columnIndex:0) => 0x7f2df4001473
D:987271.276s:26c6cb:stmt.c[1700]:_stmt_get_data_copy_buf_to_char():iconv(inbuf:0x7ffd1c3a9db0(0x7f2df4001479);inbytesleft:0x7ffd1c3a9db8(10);outbuf:0x7ffd1c3a9dc0(0x558118f57b60);outbytesleft:0x7ffd1c3a9dc8(1023)) ...
D:987271.336s:26c6cb:stmt.c[1700]:_stmt_get_data_copy_buf_to_char():iconv(inbuf:0x7ffd1c3a9db0(0x7f2df4001483);inbytesleft:0x7ffd1c3a9db8(0);outbuf:0x7ffd1c3a9dc0(0x558118f57b6a);outbytesleft:0x7ffd1c3a9dc8(1013)) => 0
D:987271.428s:26c6cb:taos_helpers.c[165]:helper_get_tsdb():taos_is_null(res:0x558118f362f0,row:0,col:1) ...
D:987271.564s:26c6cb:taos_helpers.c[165]:helper_get_tsdb():taos_is_null(res:0x558118f362f0,row:0,col:1) => false
D:987271.672s:26c6cb:tsdb.c[935]:_tsdb_stmt_fetch_rows_block():taos_fetch_block(res:0x558118f362f0,rows:0x7ffd1c3adff0) ...
D:987271.735s:26c6cb:tsdb.c[935]:_tsdb_stmt_fetch_rows_block():taos_fetch_block(res:0x558118f362f0,rows:0x7ffd1c3adff0(0x7f2df4000f90)) => 0
[453]:run_query_with_cols():1 rows fetched
D:987271.882s:26c6cb:tsdb.c[277]:tsdb_res_reset():taos_free_result(res:0x558118f362f0) ...
D:987272.009s:26c6cb:tsdb.c[277]:tsdb_res_reset():taos_free_result(res:0x558118f362f0) => void
*/

static int handles_query(handles_t *handles, const char *sql, int display)
{
  int e = 0;
  int r = 0;

  handles_free_result_set(handles);
  handles->res = CALL_taos_query(handles->taos, sql);
  e = taos_errno(handles->res);
  if (e) {
    DUMP("taos_query failed:[0x%x]%s", e, taos_errstr(handles->res));
    handles_free_result_set(handles);
    return -1;
  }
  handles->precision = CALL_taos_result_precision(handles->res);
  handles->affected_rows = CALL_taos_affected_rows(handles->res);
  handles->nr_fields = CALL_taos_field_count(handles->res);
  handles->fields    = CALL_taos_fetch_fields(handles->res);

  size_t nr_total_rows = 0;

again:

  handles->nr_rows   = CALL_taos_fetch_block(handles->res, &handles->rows);
  if (handles->nr_rows <= 0) {
    DUMP("%zd rows returned", nr_total_rows);
    return 0;
  }

  for (int i=0; i<handles->nr_rows; ++i) {
    if (display) fprintf(stderr, "row[%zd]:", nr_total_rows + i + 1);
    for (int j=0; j<handles->nr_fields; ++j) {
      TAOS_FIELD *field = handles->fields + j;
      if (display && j) fprintf(stderr, ",");
      if (display) fprintf(stderr, "%s:", field->name);
      switch (field->type) {
        case TSDB_DATA_TYPE_VARCHAR:
          r = handles_read_as_varchar(handles, display, i, j);
          if (r) return -1;
          break;
        case TSDB_DATA_TYPE_INT:
          r = handles_read_as_int(handles, display, i, j);
          if (r) return -1;
          break;
        default:
          if (display) fprintf(stderr, "\n");
          DUMP("unknow field type:[0x%x]", field->type);
          return -1;
      }
    }
    if (display) fprintf(stderr, "\n");
  }

  nr_total_rows += handles->nr_rows;

  goto again;
}

static int run(handles_t *handles, int argc, char *argv[])
{
  const char *app = argv[0];

  int r = 0;

  const char *ip       = NULL;
  const char *user     = NULL;
  const char *pass     = NULL;
  const char *db       = NULL;
  uint16_t    port     = 0;

  int changed = 0;

  int display          = 0;

  for (int i=1; i<argc; ++i) {
    const char *arg = argv[i];
    if (0 == strcasecmp(arg, "--")) continue;
    if (0 == strcasecmp(arg, "-h")) {
      usage(app);
      return 0;
    }
    if (0 == strcasecmp(arg, "--ip")) {
      if (i+1 >= argc) {
        DUMP("<ip> expected after --ip");
        return -1;
      }
      ip = argv[++i];
      changed = 1;
      continue;
    }
    if (0 == strcasecmp(arg, "--user")) {
      if (i+1 >= argc) {
        DUMP("<user> expected after --user");
        return -1;
      }
      user = argv[++i];
      changed = 1;
      continue;
    }
    if (0 == strcasecmp(arg, "--pass")) {
      if (i+1 >= argc) {
        DUMP("<pass> expected after --pass");
        return -1;
      }
      pass = argv[++i];
      changed = 1;
      continue;
    }
    if (0 == strcasecmp(arg, "--db")) {
      if (i+1 >= argc) {
        DUMP("<db> expected after --db");
        return -1;
      }
      db = argv[++i];
      changed = 1;
      continue;
    }
    if (0 == strcasecmp(arg, "--port")) {
      if (i+1 >= argc) {
        DUMP("<port> expected after --port");
        return -1;
      }
      char *endptr;
      long long port_ll;
      errno = 0;
      port_ll = strtoll(argv[++i], &endptr, 0);
      if (endptr == argv[i] || *endptr != '\0' || errno != 0) {
        DUMP("error: invalid port number '%s'", argv[i]);
        return -1;
      }

      if (port_ll < 0 || port_ll > UINT16_MAX) {
        DUMP("error: port number out of range. must be between 0 and %u", UINT16_MAX);
        return -1;
      }

      port = (uint16_t)port_ll;
      changed = 1;
      continue;
    }
    if (0 == strcasecmp(arg, "conn")) {
      changed = 0;
      r = handles_connect(handles, ip, user, pass, db, port);
      if (r) return -1;
      continue;
    }
    if (0 == strcasecmp(arg, "query")) {
      if (changed) {
        changed = 0;
        r = handles_connect(handles, ip, user, pass, db, port);
        if (r) return -1;
      }
      for (++i; i<argc; ++i) {
        arg = argv[i];
        if (0 == strcasecmp(arg, "--")) {
          --i;
          break;
        }
        if (0 == strcasecmp(arg, "--display")) {
          display = 1;
          continue;
        }
        r = handles_query(handles, arg, display);
        if (r) return -1;
      }
      continue;
    }
    DUMP("unknown argument:%s", arg);
    return -1;
  }
  if (changed) {
    return handles_connect(handles, ip, user, pass, db, port);
  }

  return 0;
}

int main(int argc, char *argv[])
{
  int r = 0;

  handles_t   handles  = {0};

  CALL_taos_init();

  r = run(&handles, argc, argv);

  handles_release(&handles);

  CALL_taos_cleanup();

  DUMP("-=%s=-", r ? "failure" : "success");
  return !!r;
}


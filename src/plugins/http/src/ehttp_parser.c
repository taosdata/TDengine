#include "ehttp_parser.h"

#include "ehttp_gzip.h"
#include "ehttp_util_string.h"
#include "elog.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

struct ehttp_status_code_s {
  int         status_code;
  const char *status_desc;
};

static ehttp_status_code_t status_codes[] = {
  {100, "Continue"},
  {101, "Switching Protocol"},
  {102, "Processing (WebDAV)"},
  {103, "Early Hints"},
  {200, "OK"},
  {201, "Created"},
  {202, "Accepted"},
  {203, "Non-Authoritative Information"},
  {204, "No Content"},
  {205, "Reset Content"},
  {206, "Partial Content"},
  {207, "Multi-Status (WebDAV)"},
  {208, "Already Reported (WebDAV)"},
  {226, "IM Used (HTTP Delta encoding)"},
  {300, "Multiple Choice"},
  {301, "Moved Permanently"},
  {302, "Found"},
  {303, "See Other"},
  {304, "Not Modified"},
  {305, "Use Proxy"},
  {306, "unused"},
  {307, "Temporary Redirect"},
  {308, "Permanent Redirect"},
  {400, "Bad Request"},
  {401, "Unauthorized"},
  {402, "Payment Required"},
  {403, "Forbidden"},
  {404, "Not Found"},
  {405, "Method Not Allowed"},
  {406, "Not Acceptable"},
  {407, "Proxy Authentication Required"},
  {408, "Request Timeout"},
  {409, "Conflict"},
  {410, "Gone"},
  {411, "Length Required"},
  {412, "Precondition Failed"},
  {413, "Payload Too Large"},
  {414, "URI Too Long"},
  {415, "Unsupported Media Type"},
  {416, "Range Not Satisfiable"},
  {417, "Expectation Failed"},
  {418, "I'm a teapot"},
  {421, "Misdirected Request"},
  {422, "Unprocessable Entity (WebDAV)"},
  {423, "Locked (WebDAV)"},
  {424, "Failed Dependency (WebDAV)"},
  {425, "Too Early"},
  {426, "Upgrade Required"},
  {428, "Precondition Required"},
  {429, "Too Many Requests"},
  {431, "Request Header Fields Too Large"},
  {451, "Unavailable For Legal Reasons"},
  {500, "Internal Server Error"},
  {501, "Not Implemented"},
  {502, "Bad Gateway"},
  {503, "Service Unavailable"},
  {504, "Gateway Timeout"},
  {505, "HTTP Version Not Supported"},
  {506, "Variant Also Negotiates"},
  {507, "Insufficient Storage"},
  {508, "Loop Detected (WebDAV)"},
  {510, "Not Extended"},
  {511, "Network Authentication Required"},
  {0,   NULL}
};

const char* ehttp_status_code_get_desc(const int status_code) {
  ehttp_status_code_t *p = status_codes;
  while (p->status_code!=0) {
    if (p->status_code==status_code) return p->status_desc;
    ++p;
  }
  return "Unknow status code";
}

typedef enum HTTP_PARSER_STATE {
  HTTP_PARSER_BEGIN,
  HTTP_PARSER_REQUEST_OR_RESPONSE,
  HTTP_PARSER_METHOD,
  HTTP_PARSER_TARGET,
  HTTP_PARSER_HTTP_VERSION,
  HTTP_PARSER_SP,
  HTTP_PARSER_STATUS_CODE,
  HTTP_PARSER_REASON_PHRASE,
  HTTP_PARSER_CRLF,
  HTTP_PARSER_HEADER,
  HTTP_PARSER_HEADER_KEY,
  HTTP_PARSER_HEADER_VAL,
  HTTP_PARSER_CHUNK_SIZE,
  HTTP_PARSER_CHUNK,
  HTTP_PARSER_END,
  HTTP_PARSER_ERROR,
} HTTP_PARSER_STATE;

typedef struct ehttp_parser_kv_s            ehttp_parser_kv_t;

struct ehttp_parser_kv_s {
  char            *key;
  char            *val;
};

struct ehttp_parser_s {
  ehttp_parser_callbacks_t     callbacks;
  void                        *arg;
  ehttp_parser_conf_t          conf;

  char                        *method;
  char                        *target;
  char                        *target_raw;
  char                        *version;

  int                          http_10:2;
  int                          http_11:2;
  int                          accept_encoding_gzip:2;
  int                          accept_encoding_chunked:2;
  int                          transfer_gzip:2;
  int                          transfer_chunked:2;
  int                          content_length_specified:2;
  int                          content_chunked:2;


  int                          status_code;
  char                        *reason_phrase;

  char                        *key;
  char                        *val;
  ehttp_parser_kv_t           *kvs;
  size_t                       kvs_count;

  char                        *auth_basic;

  size_t                       content_length;

  size_t                       chunk_size;
  size_t                       received_chunk_size;
  size_t                       received_size;

  ehttp_gzip_t                *gzip;
  ehttp_util_string_t          str;
  HTTP_PARSER_STATE           *stacks;
  size_t                       stacks_count;
};

static void dummy_on_request_line(void *arg, const char *method, const char *target, const char *version, const char *target_raw) {
}

static void dummy_on_status_line(void *arg, const char *version, int status_code, const char *reason_phrase) {
}

static void dummy_on_header_field(void *arg, const char *key, const char *val) {
}

static void dummy_on_body(void *arg, const char *chunk, size_t len) {
}

static void dummy_on_end(void *arg) {
}

static void dummy_on_error(void *arg, int status_code) {
}


static HTTP_PARSER_STATE ehttp_parser_top(ehttp_parser_t *parser) {
  EQ_ASSERT(parser->stacks_count >= 1);
  EQ_ASSERT(parser->stacks);

  return parser->stacks[parser->stacks_count-1];
}

static int ehttp_parser_push(ehttp_parser_t *parser, HTTP_PARSER_STATE state) {
  size_t                   n      = parser->stacks_count + 1;
  // HTTP_PARSER_STATE *stacks       = (HTTP_PARSER_STATE*)reallocarray(parser->stacks, n, sizeof(*stacks));
  HTTP_PARSER_STATE *stacks       = (HTTP_PARSER_STATE*)realloc(parser->stacks, n * sizeof(*stacks));
  if (!stacks) return -1;

  parser->stacks_count            = n;
  parser->stacks                  = stacks;
  parser->stacks[n-1]             = state;

  return 0;
}

static int ehttp_parser_pop(ehttp_parser_t *parser) {
  if (parser->stacks_count <= 0) return -1;
  --parser->stacks_count;

  return 0;
}

ehttp_parser_t *ehttp_parser_create(ehttp_parser_callbacks_t callbacks, ehttp_parser_conf_t conf, void *arg) {
  ehttp_parser_t *parser = (ehttp_parser_t*)calloc(1, sizeof(*parser));
  if (!parser) return NULL;

  parser->callbacks       = callbacks;
  parser->arg             = arg;
  parser->conf            = conf;

  if (parser->callbacks.on_request_line == NULL) {
    parser->callbacks.on_request_line = dummy_on_request_line;
  }
  if (parser->callbacks.on_status_line == NULL) {
    parser->callbacks.on_status_line = dummy_on_status_line;
  }
  if (parser->callbacks.on_header_field == NULL) {
    parser->callbacks.on_header_field = dummy_on_header_field;
  }
  if (parser->callbacks.on_body == NULL) {
    parser->callbacks.on_body = dummy_on_body;
  }
  if (parser->callbacks.on_end == NULL) {
    parser->callbacks.on_end = dummy_on_end;
  }
  if (parser->callbacks.on_error == NULL) {
    parser->callbacks.on_error = dummy_on_error;
  }

  ehttp_parser_push(parser, HTTP_PARSER_BEGIN);

  return parser;
}

static void ehttp_parser_kvs_destroy(ehttp_parser_t *parser) {
  if (!parser->kvs) return;

  for (size_t i=0; i<parser->kvs_count; ++i) {
    ehttp_parser_kv_t *p = &parser->kvs[i];
    free(p->key); p->key = NULL;
    free(p->val); p->val = NULL;
  }
  free(parser->kvs);
  parser->kvs = NULL;
  parser->kvs_count = 0;

  free(parser->auth_basic);
  parser->auth_basic = NULL;
}

void ehttp_parser_destroy(ehttp_parser_t *parser) {
  if (!parser) return;

  free(parser->method);         parser->method          = NULL;
  free(parser->target);         parser->target          = NULL;
  free(parser->target_raw);     parser->target_raw      = NULL;
  free(parser->version);        parser->version         = NULL;
  free(parser->reason_phrase);  parser->reason_phrase   = NULL;
  free(parser->key);            parser->key             = NULL;
  free(parser->val);            parser->val             = NULL;
  free(parser->auth_basic);     parser->auth_basic      = NULL;
  free(parser->stacks);         parser->stacks          = NULL;

  parser->stacks_count = 0;

  ehttp_parser_kvs_destroy(parser);

  ehttp_util_string_cleanup(&parser->str);
  if (parser->gzip) {
    ehttp_gzip_destroy(parser->gzip);
    parser->gzip = NULL;
  }

  free(parser);
}

#define is_token(c)      (strchr("!#$%&'*+-.^_`|~", c) || isdigit(c) || isalpha(c))

char *ehttp_parser_urldecode(const char *enc) {
  int ok = 1;
  ehttp_util_string_t str = {0};
  while (*enc) {
    char *p = strchr(enc, '%');
    if (!p) break;
    int hex, cnt;
    int n = sscanf(p+1, "%2x%n", &hex, &cnt);
    if (n!=1 && cnt !=2) { ok = 0; break; }
    if (ehttp_util_string_append(&str, enc, p-enc)) { ok = 0; break; }
    char c = (char)hex;
    if (ehttp_util_string_append(&str, &c, 1)) { ok = 0; break; }
    enc    = p+3;
  }
  char *dec = NULL;
  if (ok && *enc) {
    if (ehttp_util_string_append(&str, enc, strlen(enc))) { ok = 0; }
  }
  if (ok) {
    dec = str.str;
    str.str = NULL;
  }
  ehttp_util_string_cleanup(&str);
  return dec;
}

static void on_data(ehttp_gzip_t *gzip, void *arg, const char *buf, size_t len) {
  ehttp_parser_t *parser = (ehttp_parser_t*)arg;
  parser->callbacks.on_body(parser->arg, buf, len);
}

static int ehttp_parser_check_field(ehttp_parser_t *parser, const char *key, const char *val) {
  int ok = 0;
  do {
    if (0==strcasecmp(key, "Content-Length")) {
      size_t len = 0;
      int bytes = 0;
      int n = sscanf(val, "%ld%n", &len, &bytes);
      if (n==1 && bytes==strlen(val)) {
        parser->content_length = len;
        parser->chunk_size     = len;
        parser->content_length_specified = 1;
        break;
      }
      ok = -1;
      break;
    }
    if (0==strcasecmp(key, "Accept-Encoding")) {
      if (strstr(val, "gzip")) {
        parser->accept_encoding_gzip = 1;
      }
      if (strstr(val, "chunked")) {
        parser->accept_encoding_chunked = 1;
      }
      break;
    }
    if (0==strcasecmp(key, "Content-Encoding")) {
      if (0==strcmp(val, "gzip")) {
        parser->content_chunked = 1;
      }
      break;
    }
    if (0==strcasecmp(key, "Transfer-Encoding")) {
      if (strstr(val, "gzip")) {
        parser->transfer_gzip = 1;
        ehttp_gzip_conf_t             conf = {0};
        ehttp_gzip_callbacks_t        callbacks = {0};

        callbacks.on_data     = on_data;

        parser->gzip          = ehttp_gzip_create_decompressor(conf, callbacks, parser);

        if (!parser->gzip) {
          E("failed to create gzip decompressor");
          ok = -1;
          break;
        }
      }
      if (strstr(val, "chunked")) {
        parser->transfer_chunked = 1;
      }
      break;
    }
    if (0==strcasecmp(key, "Authorization")) {
      char *t   = NULL;
      char *s   = NULL;
      int bytes = 0;
      int n = sscanf(val, "%ms %ms%n", &t, &s, &bytes);
      if (n==2 && t && s && bytes==strlen(val) && strcmp(t, "Basic")) {
        free(parser->auth_basic);
        parser->auth_basic = s; s = NULL;
      } else {
        ok = -1;
      }
      free(t); free(s);
      break;
    }
  } while (0);
  return ok;
}

static int ehttp_parser_kvs_append_kv(ehttp_parser_t *parser, const char *key, const char *val) {
  // ehttp_parser_kv_t *kvs   = (ehttp_parser_kv_t*)reallocarray(parser->kvs, parser->kvs_count + 1, sizeof(*kvs));
  ehttp_parser_kv_t *kvs   = (ehttp_parser_kv_t*)realloc(parser->kvs, (parser->kvs_count + 1) * sizeof(*kvs));
  if (!kvs) return -1;

  parser->kvs = kvs;

  kvs[parser->kvs_count].key  = strdup(key);
  kvs[parser->kvs_count].val  = strdup(val);

  if (kvs[parser->kvs_count].key && kvs[parser->kvs_count].val) {
    ++parser->kvs_count;
    return 0;
  }

  free(kvs[parser->kvs_count].key);
  kvs[parser->kvs_count].key = NULL;
  free(kvs[parser->kvs_count].val);
  kvs[parser->kvs_count].val = NULL;

  return -1;
}

static int on_begin(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (c=='G' || c=='P' || c=='H' || c=='D' || c=='C' || c=='O' || c=='T') {
      if (ehttp_util_string_append(&parser->str, &c, 1)) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      ehttp_parser_pop(parser);
      ehttp_parser_push(parser, HTTP_PARSER_REQUEST_OR_RESPONSE);
      break;
    }
    E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int on_request_or_response(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (parser->str.len==1) {
      if (c=='T' && parser->str.str[0]=='H') {
        ehttp_parser_pop(parser);
        ehttp_parser_push(parser, HTTP_PARSER_END);
        ehttp_parser_push(parser, HTTP_PARSER_HEADER);
        ehttp_parser_push(parser, HTTP_PARSER_CRLF);
        ehttp_parser_push(parser, HTTP_PARSER_REASON_PHRASE);
        ehttp_parser_push(parser, HTTP_PARSER_SP);
        ehttp_parser_push(parser, HTTP_PARSER_STATUS_CODE);
        ehttp_parser_push(parser, HTTP_PARSER_SP);
        ehttp_parser_push(parser, HTTP_PARSER_HTTP_VERSION);
        *again = 1;
        break;
      }
      ehttp_parser_pop(parser);
      ehttp_parser_push(parser, HTTP_PARSER_END);
      ehttp_parser_push(parser, HTTP_PARSER_HEADER);
      ehttp_parser_push(parser, HTTP_PARSER_CRLF);
      ehttp_parser_push(parser, HTTP_PARSER_HTTP_VERSION);
      ehttp_parser_push(parser, HTTP_PARSER_SP);
      ehttp_parser_push(parser, HTTP_PARSER_TARGET);
      ehttp_parser_push(parser, HTTP_PARSER_SP);
      ehttp_parser_push(parser, HTTP_PARSER_METHOD);
      *again = 1;
      break;
    }
    E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int on_method(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (isalnum(c) || strchr("!#$%&'*+-.^_`|~", c)) {
      if (ehttp_util_string_append(&parser->str, &c, 1)) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    parser->method = strdup(parser->str.str);
    if (!parser->method) {
      E("parser state: %d, char: [%c]%02x, oom", state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    ehttp_util_string_clear(&parser->str);
    ehttp_parser_pop(parser);
    *again = 1;
  } while (0);
  return ok;
}

static int on_target(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (!isspace(c) && c!='\r' && c!='\n') {
      if (ehttp_util_string_append(&parser->str, &c, 1)) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    parser->target_raw = strdup(parser->str.str);
    parser->target     = ehttp_parser_urldecode(parser->str.str);
    if (!parser->target_raw || !parser->target) {
      E("parser state: %d, char: [%c]%02x, oom", state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    ehttp_util_string_clear(&parser->str);
    ehttp_parser_pop(parser);
    *again = 1;
  } while (0);
  return ok;
}

static int on_version(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    const char *prefix = "HTTP/1.";
    int            len = strlen(prefix);
    if (parser->str.len < len) {
      if (prefix[parser->str.len]!=c) {
        E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 400);
        break;
      }
      if (ehttp_util_string_append(&parser->str, &c, 1)) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }

    if (c!='0' && c!='1') {
      E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 400);
      break;
    }
    if (ehttp_util_string_append(&parser->str, &c, 1)) {
      E("parser state: %d, char: [%c]%02x, oom", state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    if (c=='0') parser->http_10 = 1;
    if (c=='1') parser->http_11 = 1;

    parser->version = strdup(parser->str.str);
    if (!parser->version) {
      E("parser state: %d, char: [%c]%02x, oom", state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }

    if (parser->method) {
      parser->callbacks.on_request_line(parser->arg, parser->method, parser->target, parser->version, parser->target_raw);
    }

    ehttp_util_string_clear(&parser->str);
    ehttp_parser_pop(parser);
  } while (0);
  return ok;
}

static int on_sp(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (c==' ') {
      ehttp_parser_pop(parser);
      break;
    }
    E("parser state: %d, char: [%c]%02x, oom", state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 507);
  } while (0);
  return ok;
}

static int on_status_code(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (isdigit(c)) {
      if (ehttp_util_string_append(&parser->str, &c, 1)) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      if (parser->str.len < 3) break;

      sscanf(parser->str.str, "%d", &parser->status_code);
      ehttp_util_string_clear(&parser->str);
      ehttp_parser_pop(parser);
      break;
    }
    E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int on_reason_phrase(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (c=='\r') {
      parser->reason_phrase = strdup(parser->str.str);
      if (!parser->reason_phrase) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      parser->callbacks.on_status_line(parser->arg, parser->version, parser->status_code, parser->reason_phrase);
      ehttp_util_string_clear(&parser->str);
      ehttp_parser_pop(parser);
      *again = 1;
      break;
    }
    if (ehttp_util_string_append(&parser->str, &c, 1)) {
      E("parser state: %d, char: [%c]%02x, oom", state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
  } while (0);
  return ok;
}

static int post_process(ehttp_parser_t *parser) {
  if (parser->gzip) {
    if (ehttp_gzip_finish(parser->gzip)) {
      E("gzip failed");
      parser->callbacks.on_error(parser->arg, 507);
      return -1;
    }
  }
  parser->callbacks.on_end(parser->arg);
  return 0;
}

static int on_crlf(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    const char *s   = "\r\n";
    int   len       = strlen(s);
    if (s[parser->str.len]!=c) {
      E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 400);
      break;
    }
    if (ehttp_util_string_append(&parser->str, &c, 1)) {
      E("parser state: %d, char: [%c]%02x, oom", state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    if (parser->str.len == len) {
      ehttp_util_string_clear(&parser->str);
      ehttp_parser_pop(parser);
      if (ehttp_parser_top(parser) == HTTP_PARSER_END) {
        ok = post_process(parser);
      }
    }
    break;
  } while (0);
  return ok;
}

static int on_header(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (c=='\r') {
      ehttp_parser_pop(parser);
      if (parser->transfer_chunked) {
        ehttp_parser_push(parser, HTTP_PARSER_CHUNK_SIZE);
        ehttp_parser_push(parser, HTTP_PARSER_CRLF);
      } else {
        if (parser->content_length > 0) {
          ehttp_parser_push(parser, HTTP_PARSER_CHUNK);
        }
        ehttp_parser_push(parser, HTTP_PARSER_CRLF);
      }
      *again = 1;
      break;
    }
    if (c!=' ' && c!='\t' && c!=':' ) {
      if (ehttp_util_string_append(&parser->str, &c, 1)) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      ehttp_parser_push(parser, HTTP_PARSER_CRLF);
      ehttp_parser_push(parser, HTTP_PARSER_HEADER_VAL);
      ehttp_parser_push(parser, HTTP_PARSER_SP);
      ehttp_parser_push(parser, HTTP_PARSER_HEADER_KEY);
      break;
    }
    E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int on_header_key(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (isalnum(c) || strchr("!#$%&'*+-.^_`|~", c)) {
      if (ehttp_util_string_append(&parser->str, &c, 1)) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    if (c==':') {
      parser->key        = strdup(parser->str.str);
      if (!parser->key) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      ehttp_util_string_clear(&parser->str);
      ehttp_parser_pop(parser);
      break;
    }
    E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int on_header_val(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (c != '\r' && c != '\n' && (!isspace(c) || parser->str.len>0)) {
      if (ehttp_util_string_append(&parser->str, &c, 1)) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    const char *val        = parser->str.str;
    ok = ehttp_parser_check_field(parser, parser->key, val);
    if (ehttp_parser_kvs_append_kv(parser, parser->key, val)) {
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
    } else {
      parser->callbacks.on_header_field(parser->arg, parser->key, val);
    }
    free(parser->key); parser->key = NULL;
    val = NULL;
    if (ok==-1) break;
    ehttp_util_string_clear(&parser->str);
    ehttp_parser_pop(parser);
    *again = 1;
  } while (0);
  return ok;
}

static int on_chunk_size(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  int bytes;
  size_t len;
  int n;
  do {
    if (isxdigit(c)) {
      if (ehttp_util_string_append(&parser->str, &c, 1)) {
        E("parser state: %d, char: [%c]%02x, oom", state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    if (c=='\r') {
      n = sscanf(parser->str.str, "%lx%n", &len, &bytes);
      if (n==1 && bytes==strlen(parser->str.str) && len>=0) {
        if (len==0) {
          if (parser->content_length_specified == 0 || parser->received_size == parser->content_length) {
            ehttp_util_string_clear(&parser->str);
            ehttp_parser_pop(parser);
            ehttp_parser_push(parser, HTTP_PARSER_CRLF);
            ehttp_parser_push(parser, HTTP_PARSER_CRLF);
            *again = 1;
            break;
          }
        } else {
          if (parser->content_length_specified == 0 || parser->received_size + len <= parser->content_length) {
            parser->chunk_size = len;
            ehttp_util_string_clear(&parser->str);
            ehttp_parser_pop(parser);
            ehttp_parser_push(parser, HTTP_PARSER_CHUNK_SIZE);
            ehttp_parser_push(parser, HTTP_PARSER_CRLF);
            ehttp_parser_push(parser, HTTP_PARSER_CHUNK);
            ehttp_parser_push(parser, HTTP_PARSER_CRLF);
            *again = 1;
            break;
          }
        }
      }
    }
    E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int on_chunk(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (ehttp_util_string_append(&parser->str, &c, 1)) {
      E("parser state: %d, char: [%c]%02x, oom", state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    ++parser->received_size;
    ++parser->received_chunk_size;
    if (parser->received_chunk_size < parser->chunk_size) break;

    if (parser->gzip) {
      if (ehttp_gzip_write(parser->gzip, parser->str.str, parser->str.len)) {
        E("gzip failed");
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
    } else {
      parser->callbacks.on_body(parser->arg, parser->str.str, parser->str.len);
    }
    parser->received_chunk_size = 0;
    ehttp_util_string_clear(&parser->str);
    ehttp_parser_pop(parser);
    if (ehttp_parser_top(parser) == HTTP_PARSER_END) {
      ok = post_process(parser);
    }
  } while (0);
  return ok;
}

static int on_end(ehttp_parser_t *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    E("parser state: %d, unexpected char: [%c]%02x", state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 507);
  } while (0);
  return ok;
}

static int parse_char(ehttp_parser_t *parser, const char c, int *again) {
  int ok = 0;
  HTTP_PARSER_STATE state = ehttp_parser_top(parser);
  do {
    if (state == HTTP_PARSER_BEGIN) {
      ok = on_begin(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_REQUEST_OR_RESPONSE) {
      ok = on_request_or_response(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_METHOD) {
      ok = on_method(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_TARGET) {
      ok = on_target(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HTTP_VERSION) {
      ok = on_version(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_SP) {
      ok = on_sp(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_STATUS_CODE) {
      ok = on_status_code(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_REASON_PHRASE) {
      ok = on_reason_phrase(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_CRLF) {
      ok = on_crlf(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HEADER) {
      ok = on_header(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HEADER_KEY) {
      ok = on_header_key(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HEADER_VAL) {
      ok = on_header_val(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_CHUNK_SIZE) {
      ok = on_chunk_size(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_CHUNK) {
      ok = on_chunk(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_END) {
      ok = on_end(parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_ERROR) {
      ok = -2;
      break;
    }
    E("unknown parser state: %d", state);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 500);
  } while (0);
  if (ok==-1) {
    ehttp_parser_push(parser, HTTP_PARSER_ERROR);
  }
  if (ok==-2) ok = -1;
  return ok;
}

int ehttp_parser_parse_string(ehttp_parser_t *parser, const char *str) {
  return ehttp_parser_parse(parser, str, str?strlen(str):0);
}

int ehttp_parser_parse_char(ehttp_parser_t *parser, const char c) {
  return ehttp_parser_parse(parser, &c, 1);
}

int ehttp_parser_parse(ehttp_parser_t *parser, const char *buf, size_t len) {
  const char *p = buf;
  int ret       = 0;
  size_t i      = 0;
  while (i < len) {
    int again = 0;
    ret = parse_char(parser, *p, &again);
    if (ret) break;
    if (again) continue;
    ++p;
    ++i;
  }
  return ret;
}


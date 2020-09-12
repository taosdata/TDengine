#include "os.h"
#include "httpLog.h"
#include "httpContext.h"
#include "ehttp_util_string.h"
#include "ehttp_parser.h"

#include "ehttp_gzip.h"
#include "ehttp_util_string.h"
#include "elog.h"


#include <ctype.h>
#include <stdlib.h>
#include <string.h>


static HttpParserStatusObj status_codes[] = {
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
  HttpParserStatusObj *p = status_codes;
  while (p->status_code!=0) {
    if (p->status_code==status_code) return p->status_desc;
    ++p;
  }
  return "Unknow status code";
}

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

static HTTP_PARSER_STATE httpParserTop(HttpParserObj *parser) {
  ASSERT(parser->stacks_count >= 1);
  ASSERT(parser->stacks);

  return parser->stacks[parser->stacks_count - 1];
}

static int httpParserPush(HttpParserObj *parser, HTTP_PARSER_STATE state) {
  size_t                   n      = parser->stacks_count + 1;
  // HTTP_PARSER_STATE *stacks       = (HTTP_PARSER_STATE*)reallocarray(parser->stacks, n, sizeof(*stacks));
  HTTP_PARSER_STATE *stacks       = (HTTP_PARSER_STATE*)realloc(parser->stacks, n * sizeof(*stacks));
  if (!stacks) return -1;

  parser->stacks_count            = n;
  parser->stacks                  = stacks;
  parser->stacks[n-1]             = state;

  return 0;
}

static int httpParserPop(HttpParserObj *parser) {
  if (parser->stacks_count <= 0) return -1;
  --parser->stacks_count;

  return 0;
}

HttpParserObj *httpParserCreate(HttpParserCallbackObj callbacks, HttpParserConfObj conf, void *arg) {
  HttpParserObj *parser = (HttpParserObj*)calloc(1, sizeof(*parser));
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

  httpParserPush(parser, HTTP_PARSER_BEGIN);

  return parser;
}

static void ehttp_parser_kvs_destroy(HttpParserObj *parser) {
  if (!parser->kvs) return;

  for (size_t i=0; i<parser->kvs_count; ++i) {
    HttpParseKvObj *p = &parser->kvs[i];
    free(p->key); p->key = NULL;
    free(p->val); p->val = NULL;
  }
  free(parser->kvs);
  parser->kvs = NULL;
  parser->kvs_count = 0;

  free(parser->auth_basic);
  parser->auth_basic = NULL;
}

void httpParserDestroy(HttpParserObj *parser) {
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

  httpParserCleanupString(&parser->str);
  if (parser->gzip) {
    ehttp_gzip_destroy(parser->gzip);
    parser->gzip = NULL;
  }

  free(parser);
}

#define is_token(c)      (strchr("!#$%&'*+-.^_`|~", c) || isdigit(c) || isalpha(c))

char *ehttp_parser_urldecode(const char *enc) {
  int ok = 1;
  HttpUtilString str = {0};
  while (*enc) {
    char *p = strchr(enc, '%');
    if (!p) break;
    int hex, cnt;
    int n = sscanf(p+1, "%2x%n", &hex, &cnt);
    if (n!=1 && cnt !=2) { ok = 0; break; }
    if (httpParserAppendString(&str, enc, p-enc)) { ok = 0; break; }
    char c = (char)hex;
    if (httpParserAppendString(&str, &c, 1)) { ok = 0; break; }
    enc    = p+3;
  }
  char *dec = NULL;
  if (ok && *enc) {
    if (httpParserAppendString(&str, enc, strlen(enc))) { ok = 0; }
  }
  if (ok) {
    dec = str.str;
    str.str = NULL;
  }
  httpParserCleanupString(&str);
  return dec;
}

static void on_data(ehttp_gzip_t *gzip, void *arg, const char *buf, size_t len) {
  HttpParserObj *parser = (HttpParserObj*)arg;
  parser->callbacks.on_body(parser->arg, buf, len);
}

static int32_t httpParserCheckField(HttpContext *pContext, HttpParserObj *parser, const char *key, const char *val) {
  int32_t ok = 0;
  do {
    if (0 == strcasecmp(key, "Content-Length")) {
      int32_t len = 0;
      int32_t bytes = 0;
      int32_t n = sscanf(val, "%d%n", &len, &bytes);
      if (n == 1 && bytes == strlen(val)) {
        parser->content_length = len;
        parser->chunk_size = len;
        parser->content_length_specified = 1;
        break;
      }
      ok = -1;
      break;
    }
    if (0 == strcasecmp(key, "Accept-Encoding")) {
      if (strstr(val, "gzip")) {
        parser->accept_encoding_gzip = 1;
      }
      if (strstr(val, "chunked")) {
        parser->accept_encoding_chunked = 1;
      }
      break;
    }
    if (0 == strcasecmp(key, "Content-Encoding")) {
      if (0 == strcmp(val, "gzip")) {
        parser->content_chunked = 1;
      }
      break;
    }
    if (0 == strcasecmp(key, "Transfer-Encoding")) {
      if (strstr(val, "gzip")) {
        parser->transfer_gzip = 1;
        ehttp_gzip_conf_t             conf = {0};
        ehttp_gzip_callbacks_t        callbacks = {0};

        callbacks.on_data     = on_data;

        parser->gzip          = ehttp_gzip_create_decompressor(conf, callbacks, parser);

        if (!parser->gzip) {
          httpDebug("failed to create gzip decompressor");
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
      int32_t bytes = 0;
      int32_t n = sscanf(val, "%ms %ms%n", &t, &s, &bytes);
      if (n == 2 && t && s && bytes == strlen(val)) {
        if (strcmp(t, "Basic") == 0) {
          free(parser->auth_basic);
          parser->auth_basic = s;
          s = NULL;
        } else if (n == 2 && t && s && strcmp(t, "Taosd") == 0) {
          free(parser->auth_taosd);
          parser->auth_taosd = s;
          s = NULL;
        } else {
          httpError("context:%p, fd:%d, invalid auth, t:%s s:%s", pContext, pContext->fd, t, s);
          ok = -1;
        }
      } else {
        httpError("context:%p, fd:%d, parse auth failed, t:%s s:%s", pContext, pContext->fd, t, s);
        ok = -1;
      }

      free(t);
      free(s);
      break;
    }
  } while (0);
  return ok;
}

static int httpParserAppendKv(HttpParserObj *parser, const char *key, const char *val) {
  // HttpParseKvObj *kvs   = (HttpParseKvObj*)reallocarray(parser->kvs, parser->kvs_count + 1, sizeof(*kvs));
  HttpParseKvObj *kvs   = (HttpParseKvObj*)realloc(parser->kvs, (parser->kvs_count + 1) * sizeof(*kvs));
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

static int32_t httpParserOnBegin(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  do {
    if (c == 'G' || c == 'P' || c == 'H' || c == 'D' || c == 'C' || c == 'O' || c == 'T') {
      if (httpParserAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      httpParserPop(parser);
      httpParserPush(parser, HTTP_PARSER_REQUEST_OR_RESPONSE);
      break;
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int32_t httpParserOnRquestOrResponse(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  do {
    if (parser->str.len == 1) {
      if (c == 'T' && parser->str.str[0] == 'H') {
        httpParserPop(parser);
        httpParserPush(parser, HTTP_PARSER_END);
        httpParserPush(parser, HTTP_PARSER_HEADER);
        httpParserPush(parser, HTTP_PARSER_CRLF);
        httpParserPush(parser, HTTP_PARSER_REASON_PHRASE);
        httpParserPush(parser, HTTP_PARSER_SP);
        httpParserPush(parser, HTTP_PARSER_STATUS_CODE);
        httpParserPush(parser, HTTP_PARSER_SP);
        httpParserPush(parser, HTTP_PARSER_HTTP_VERSION);
        *again = 1;
        break;
      }
      httpParserPop(parser);
      httpParserPush(parser, HTTP_PARSER_END);
      httpParserPush(parser, HTTP_PARSER_HEADER);
      httpParserPush(parser, HTTP_PARSER_CRLF);
      httpParserPush(parser, HTTP_PARSER_HTTP_VERSION);
      httpParserPush(parser, HTTP_PARSER_SP);
      httpParserPush(parser, HTTP_PARSER_TARGET);
      httpParserPush(parser, HTTP_PARSER_SP);
      httpParserPush(parser, HTTP_PARSER_METHOD);
      *again = 1;
      break;
    }

    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int32_t httpParserOnMethod(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  do {
    if (isalnum(c) || strchr("!#$%&'*+-.^_`|~", c)) {
      if (httpParserAppendString(&parser->str, &c, 1)) {
        httpDebug("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    parser->method = strdup(parser->str.str);
    if (!parser->method) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    httpParserClearString(&parser->str);
    httpParserPop(parser);
    *again = 1;
  } while (0);
  return ok;
}

static int32_t httpParserOnTarget(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  do {
    if (!isspace(c) && c != '\r' && c != '\n') {
      if (httpParserAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    parser->target_raw = strdup(parser->str.str);
    parser->target = ehttp_parser_urldecode(parser->str.str);
    if (!parser->target_raw || !parser->target) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    httpParserClearString(&parser->str);
    httpParserPop(parser);
    *again = 1;
  } while (0);
  return ok;
}

static int32_t httpParserOnVersion(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  do {
    const char *prefix = "HTTP/1.";
    int32_t     len = strlen(prefix);
    if (parser->str.len < len) {
      if (prefix[parser->str.len]!=c) {
        httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 400);
        break;
      }
      if (httpParserAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }

    if (c!='0' && c!='1') {
      httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 400);
      break;
    }
    if (httpParserAppendString(&parser->str, &c, 1)) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    if (c=='0') parser->http_10 = 1;
    if (c=='1') parser->http_11 = 1;

    parser->version = strdup(parser->str.str);
    if (!parser->version) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }

    if (parser->method) {
      parser->callbacks.on_request_line(parser->arg, parser->method, parser->target, parser->version, parser->target_raw);
    }

    httpParserClearString(&parser->str);
    httpParserPop(parser);
  } while (0);
  return ok;
}

static int32_t httpParserOnSp(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int32_t ok = 0;
  do {
    if (c == ' ') {
      httpParserPop(parser);
      break;
    }
    httpDebug("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 507);
  } while (0);
  return ok;
}

static int32_t httpParserOnStatusCode(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (isdigit(c)) {
      if (httpParserAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      if (parser->str.len < 3) break;

      sscanf(parser->str.str, "%d", &parser->status_code);
      httpParserClearString(&parser->str);
      httpParserPop(parser);
      break;
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int32_t httpParserOnReasonPhrase(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (c=='\r') {
      parser->reason_phrase = strdup(parser->str.str);
      if (!parser->reason_phrase) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      parser->callbacks.on_status_line(parser->arg, parser->version, parser->status_code, parser->reason_phrase);
      httpParserClearString(&parser->str);
      httpParserPop(parser);
      *again = 1;
      break;
    }
    if (httpParserAppendString(&parser->str, &c, 1)) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
  } while (0);
  return ok;
}

static int32_t httpParserPostProcess(HttpContext *pContext, HttpParserObj *parser) {
  if (parser->gzip) {
    if (ehttp_gzip_finish(parser->gzip)) {
      httpError("context:%p, fd:%d, gzip failed", pContext, pContext->fd);
      parser->callbacks.on_error(parser->arg, 507);
      return -1;
    }
  }
  parser->callbacks.on_end(parser->arg);
  return 0;
}

static int32_t httpParserOnCrlf(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  do {
    const char *s   = "\r\n";
    int32_t   len   = strlen(s);
    if (s[parser->str.len]!=c) {
      httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 400);
      break;
    }
    if (httpParserAppendString(&parser->str, &c, 1)) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    if (parser->str.len == len) {
      httpParserClearString(&parser->str);
      httpParserPop(parser);
      if (httpParserTop(parser) == HTTP_PARSER_END) {
        ok = httpParserPostProcess(pContext, parser);
      }
    }
    break;
  } while (0);
  return ok;
}

static int32_t httpParserOnHeader(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  do {
    if (c=='\r') {
      httpParserPop(parser);
      if (parser->transfer_chunked) {
        httpParserPush(parser, HTTP_PARSER_CHUNK_SIZE);
        httpParserPush(parser, HTTP_PARSER_CRLF);
      } else {
        if (parser->content_length > 0) {
          httpParserPush(parser, HTTP_PARSER_CHUNK);
        }
        httpParserPush(parser, HTTP_PARSER_CRLF);
      }
      *again = 1;
      break;
    }
    if (c!=' ' && c!='\t' && c!=':' ) {
      if (httpParserAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      httpParserPush(parser, HTTP_PARSER_CRLF);
      httpParserPush(parser, HTTP_PARSER_HEADER_VAL);
      httpParserPush(parser, HTTP_PARSER_SP);
      httpParserPush(parser, HTTP_PARSER_HEADER_KEY);
      break;
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int httpParserOnHeaderKey(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (isalnum(c) || strchr("!#$%&'*+-.^_`|~", c)) {
      if (httpParserAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    if (c==':') {
      parser->key        = strdup(parser->str.str);
      if (!parser->key) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      httpParserClearString(&parser->str);
      httpParserPop(parser);
      break;
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int32_t httpParserOnHeaderVal(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  do {
    if (c != '\r' && c != '\n' && (!isspace(c) || parser->str.len>0)) {
      if (httpParserAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    const char *val = parser->str.str;
    ok = httpParserCheckField(pContext, parser, parser->key, val);
    if (httpParserAppendKv(parser, parser->key, val)) {
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
    } else {
      parser->callbacks.on_header_field(parser->arg, parser->key, val);
    }
    free(parser->key);
    parser->key = NULL;
    val = NULL;
    if (ok == -1) break;
    httpParserClearString(&parser->str);
    httpParserPop(parser);
    *again = 1;
  } while (0);
  return ok;
}

static int32_t httpParserOnChunkSize(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  int32_t bytes;
  int32_t len;
  int n;
  do {
    if (isxdigit(c)) {
      if (httpParserAppendString(&parser->str, &c, 1)) {
        httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
      break;
    }
    if (c=='\r') {
      n = sscanf(parser->str.str, "%x%n", &len, &bytes);
      if (n==1 && bytes==strlen(parser->str.str) && len>=0) {
        if (len==0) {
          if (parser->content_length_specified == 0 || parser->received_size == parser->content_length) {
            httpParserClearString(&parser->str);
            httpParserPop(parser);
            httpParserPush(parser, HTTP_PARSER_CRLF);
            httpParserPush(parser, HTTP_PARSER_CRLF);
            *again = 1;
            break;
          }
        } else {
          if (parser->content_length_specified == 0 || parser->received_size + len <= parser->content_length) {
            parser->chunk_size = len;
            httpParserClearString(&parser->str);
            httpParserPop(parser);
            httpParserPush(parser, HTTP_PARSER_CHUNK_SIZE);
            httpParserPush(parser, HTTP_PARSER_CRLF);
            httpParserPush(parser, HTTP_PARSER_CHUNK);
            httpParserPush(parser, HTTP_PARSER_CRLF);
            *again = 1;
            break;
          }
        }
      }
    }
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 400);
  } while (0);
  return ok;
}

static int httpParserOnChunk(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int *again) {
  int ok = 0;
  do {
    if (httpParserAppendString(&parser->str, &c, 1)) {
      httpError("context:%p, fd:%d, parser state:%d, char:[%c]%02x, oom", pContext, pContext->fd, state, c, c);
      ok = -1;
      parser->callbacks.on_error(parser->arg, 507);
      break;
    }
    ++parser->received_size;
    ++parser->received_chunk_size;
    if (parser->received_chunk_size < parser->chunk_size) break;

    if (parser->gzip) {
      if (ehttp_gzip_write(parser->gzip, parser->str.str, parser->str.len)) {
        httpError("context:%p, fd:%d, gzip failed", pContext, pContext->fd);
        ok = -1;
        parser->callbacks.on_error(parser->arg, 507);
        break;
      }
    } else {
      parser->callbacks.on_body(parser->arg, parser->str.str, parser->str.len);
    }
    parser->received_chunk_size = 0;
    httpParserClearString(&parser->str);
    httpParserPop(parser);
    if (httpParserTop(parser) == HTTP_PARSER_END) {
      ok = httpParserPostProcess(pContext, parser);
    }
  } while (0);
  return ok;
}

static int32_t httpParserOnEnd(HttpContext *pContext, HttpParserObj *parser, HTTP_PARSER_STATE state, const char c, int32_t *again) {
  int32_t ok = 0;
  do {
    httpError("context:%p, fd:%d, parser state:%d, unexpected char:[%c]%02x", pContext, pContext->fd, state, c, c);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 507);
  } while (0);
  return ok;
}

static int32_t httpParseChar(HttpContext *pContext, HttpParserObj *parser, const char c, int32_t *again) {
  int32_t ok = 0;
  HTTP_PARSER_STATE state = httpParserTop(parser);
  do {
    if (state == HTTP_PARSER_BEGIN) {
      ok = httpParserOnBegin(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_REQUEST_OR_RESPONSE) {
      ok = httpParserOnRquestOrResponse(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_METHOD) {
      ok = httpParserOnMethod(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_TARGET) {
      ok = httpParserOnTarget(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HTTP_VERSION) {
      ok = httpParserOnVersion(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_SP) {
      ok = httpParserOnSp(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_STATUS_CODE) {
      ok = httpParserOnStatusCode(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_REASON_PHRASE) {
      ok = httpParserOnReasonPhrase(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_CRLF) {
      ok = httpParserOnCrlf(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HEADER) {
      ok = httpParserOnHeader(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HEADER_KEY) {
      ok = httpParserOnHeaderKey(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_HEADER_VAL) {
      ok = httpParserOnHeaderVal(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_CHUNK_SIZE) {
      ok = httpParserOnChunkSize(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_CHUNK) {
      ok = httpParserOnChunk(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_END) {
      ok = httpParserOnEnd(pContext, parser, state, c, again);
      break;
    }
    if (state == HTTP_PARSER_ERROR) {
      ok = -2;
      break;
    }

    httpError("context:%p, fd:%d, unknown parse state:%d", pContext, pContext->fd, state);
    ok = -1;
    parser->callbacks.on_error(parser->arg, 500);
  } while (0);

  if (ok == -1) {
    httpError("context:%p, fd:%d, failed to parse, state:%d ok:%d", pContext, pContext->fd, state, ok);
    httpParserPush(parser, HTTP_PARSER_ERROR);
  }

  if (ok == -2) {
    httpError("context:%p, fd:%d, failed to parse, state:%d ok:%d", pContext, pContext->fd, state, ok);
    ok = -1;
  }

  return ok;
}

int32_t httpParserBuf(HttpContext *pContext, HttpParserObj *parser, const char *buf, int32_t len) {
  const char *p = buf;
  int32_t     ret = 0;
  int32_t     i = 0;

  while (i < len) {
    int32_t again = 0;
    ret = httpParseChar(pContext, parser, *p, &again);
    if (ret != 0) {
      httpError("context:%p, fd:%d, parse failed, ret:%d i:%d len:%d buf:%s", pContext, pContext->fd, ret, i, len, buf);
      break;
    }
    if (again) continue;
    ++p;
    ++i;
  }

  return ret;
}

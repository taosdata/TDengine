#ifndef HTTP_PARSER_H
#define HTTP_PARSER_H

#include "ehttp_util_string.h"
#include "ehttp_gzip.h"

struct HttpContext;

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

typedef struct HttpParserStatusObj {
  int32_t     status_code;
  const char *status_desc;
} HttpParserStatusObj;

typedef struct HttpParserCallbackObj {
  void (*on_request_line)(void *arg, const char *method, const char *target, const char *version, const char *target_raw);
  void (*on_status_line)(void *arg, const char *version, int status_code, const char *reason_phrase);
  void (*on_header_field)(void *arg, const char *key, const char *val);
  void (*on_body)(void *arg, const char *chunk, size_t len);
  void (*on_end)(void *arg);
  void (*on_error)(void *arg, int status_code);
} HttpParserCallbackObj;

typedef struct HttpParserConfObj {
  size_t flush_block_size;  // <=0: immediately
} HttpParserConfObj;

typedef struct HttpParseKvObj {
  char *key;
  char *val;
} HttpParseKvObj;

typedef struct HttpParserObj {
  HttpParserCallbackObj callbacks;
  HttpParserConfObj      conf;
  void *             arg;
  char *             method;
  char *             target;
  char *             target_raw;
  char *             version;
  int                http_10 : 2;
  int                http_11 : 2;
  int                accept_encoding_gzip : 2;
  int                accept_encoding_chunked : 2;
  int                transfer_gzip : 2;
  int                transfer_chunked : 2;
  int                content_length_specified : 2;
  int                content_chunked : 2;
  int                status_code;
  char *             reason_phrase;
  char *             key;
  char *             val;
  HttpParseKvObj *   kvs;
  size_t             kvs_count;
  char *             auth_basic;
  char *             auth_taosd;
  size_t             content_length;
  size_t             chunk_size;
  size_t             received_chunk_size;
  size_t             received_size;
  ehttp_gzip_t *     gzip;
  HttpUtilString     str;
  HTTP_PARSER_STATE *stacks;
  size_t             stacks_count;
} HttpParserObj;

HttpParserObj* httpParserCreate(HttpParserCallbackObj callbacks, HttpParserConfObj conf, void *arg);
void           httpParserDestroy(HttpParserObj *parser);
int32_t        httpParserBuf(struct HttpContext *pContext, HttpParserObj *parser, const char *buf, int32_t len);

char* ehttp_parser_urldecode(const char *enc);

const char* ehttp_status_code_get_desc(const int status_code);

#endif

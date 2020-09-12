#ifndef _ehttp_parser_fc7f9ac9_52da_4ee3_b556_deb2e1c3866e
#define _ehttp_parser_fc7f9ac9_52da_4ee3_b556_deb2e1c3866e

#include <stddef.h>

typedef struct ehttp_parser_s               ehttp_parser_t;
typedef struct ehttp_parser_callbacks_s     ehttp_parser_callbacks_t;
typedef struct ehttp_parser_conf_s          ehttp_parser_conf_t;
typedef struct ehttp_status_code_s          ehttp_status_code_t;

struct ehttp_parser_callbacks_s {
  void (*on_request_line)(void *arg, const char *method, const char *target, const char *version, const char *target_raw);
  void (*on_status_line)(void *arg, const char *version, int status_code, const char *reason_phrase);
  void (*on_header_field)(void *arg, const char *key, const char *val);
  void (*on_body)(void *arg, const char *chunk, size_t len);
  void (*on_end)(void *arg);
  void (*on_error)(void *arg, int status_code);
};

struct ehttp_parser_conf_s {
  size_t             flush_block_size;       // <=0: immediately
};

ehttp_parser_t* ehttp_parser_create(ehttp_parser_callbacks_t callbacks, ehttp_parser_conf_t conf, void *arg);
void            ehttp_parser_destroy(ehttp_parser_t *parser);
int             ehttp_parser_parse(ehttp_parser_t *parser, const char *buf, size_t len);
int             ehttp_parser_parse_string(ehttp_parser_t *parser, const char *str);
int             ehttp_parser_parse_char(ehttp_parser_t *parser, const char c);
int             ehttp_parser_parse_end(ehttp_parser_t *parser);

char* ehttp_parser_urldecode(const char *enc);

const char* ehttp_status_code_get_desc(const int status_code);

#endif // _ehttp_parser_fc7f9ac9_52da_4ee3_b556_deb2e1c3866e


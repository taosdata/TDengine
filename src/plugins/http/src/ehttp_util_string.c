#include "os.h"
#include "ehttp_util_string.h"

void httpParserCleanupString(HttpUtilString *str) {
  free(str->str);
  str->str = NULL;
  str->len = 0;
}

int32_t httpParserAppendString(HttpUtilString *str, const char *s, int32_t len) {
  int32_t n   = str->len;
  char *p     = (char*)realloc(str->str, n + len + 1);
  if (!p) return -1;
  strncpy(p+n, s, len);
  p[n+len]    = '\0';
  str->str    = p;
  str->len    = n+len;
  return 0;
}

void httpParserClearString(HttpUtilString *str) {
  if (str->str) {
    str->str[0] = '\0';
    str->len    = 0;
  }
}


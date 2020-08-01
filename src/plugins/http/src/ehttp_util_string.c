#include "ehttp_util_string.h"

#include <stdlib.h>
#include <string.h>

void ehttp_util_string_cleanup(ehttp_util_string_t *str) {
  free(str->str);
  str->str = NULL;
  str->len = 0;
}

int ehttp_util_string_append(ehttp_util_string_t *str, const char *s, size_t len) {
  // int   n     = str->str?strlen(str->str):0;
  int   n     = str->len;
  char *p     = (char*)realloc(str->str, n + len + 1);
  if (!p) return -1;
  strncpy(p+n, s, len);
  p[n+len]    = '\0';
  str->str    = p;
  str->len    = n+len;
  return 0;
}

void ehttp_util_string_clear(ehttp_util_string_t *str) {
  if (str->str) {
    str->str[0] = '\0';
    str->len    = 0;
  }
}


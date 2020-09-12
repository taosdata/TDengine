#ifndef HTTP_UTIL_STRING
#define HTTP_UTIL_STRING

typedef struct HttpUtilString {
  char * str;
  size_t len;
} HttpUtilString;

void    httpParserCleanupString(HttpUtilString *str);
int32_t httpParserAppendString(HttpUtilString *str, const char *s, int32_t len);
void    httpParserClearString(HttpUtilString *str);

#endif

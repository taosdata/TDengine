#ifndef _elog_h_8897be44_dda8_45b6_9d37_8d8691cb05fb_
#define _elog_h_8897be44_dda8_45b6_9d37_8d8691cb05fb_

#include <stdio.h>

typedef enum {
  ELOG_DEBUG,
  ELOG_INFO,
  ELOG_WARN,
  ELOG_ERROR,
  ELOG_CRITICAL,
  ELOG_VERBOSE,
  ELOG_ABORT,
} ELOG_LEVEL;

void elog_set_level(ELOG_LEVEL base); // only log those not less than base
void elog_set_thread_name(const char *name);

void elog(ELOG_LEVEL level, int fd, const char *file, int line, const char *func, const char *fmt, ...)
#ifdef __GNUC__
 __attribute__((format(printf, 6, 7)))
#endif
;

#define DLOG(fd, fmt, ...)     elog(ELOG_DEBUG,    fd, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define ILOG(fd, fmt, ...)     elog(ELOG_INFO,     fd, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define WLOG(fd, fmt, ...)     elog(ELOG_WARN,     fd, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define ELOG(fd, fmt, ...)     elog(ELOG_ERROR,    fd, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define CLOG(fd, fmt, ...)     elog(ELOG_CRITICAL, fd, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define VLOG(fd, fmt, ...)     elog(ELOG_VERBOSE,  fd, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define ALOG(fd, fmt, ...)     elog(ELOG_ABORT,    fd, __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)

#define D(fmt, ...)     elog(ELOG_DEBUG,    fileno(stdout), __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define I(fmt, ...)     elog(ELOG_INFO,     fileno(stdout), __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define W(fmt, ...)     elog(ELOG_WARN,     fileno(stdout), __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define E(fmt, ...)     elog(ELOG_ERROR,    fileno(stdout), __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define C(fmt, ...)     elog(ELOG_CRITICAL, fileno(stdout), __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define V(fmt, ...)     elog(ELOG_VERBOSE,  fileno(stdout), __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)
#define A(fmt, ...)     elog(ELOG_ABORT,    fileno(stdout), __FILE__, __LINE__, __FUNCTION__, fmt, ##__VA_ARGS__)



// NOTE: https://en.wikipedia.org/wiki/Fail-fast
// for the sake of simplicity, both implementation and usage,
// we'll follow `fail-fast` or `let-it-crash` philosophy.

// assertion in both debug/release build
#define EQ_ABORT(fmt, ...) A("Assertion failure: "fmt, ##__VA_ARGS__)

#define EQ_ASSERT(statement) do {                                            \
  if (statement) break;                                                      \
  A("Assertion failure: %s", #statement);                                    \
} while (0)

#define EQ_ASSERT_EXT(statement, fmt, ...) do {                              \
  if (statement) break;                                                      \
  A("Assertion failure: %s: "fmt, #statement, ##__VA_ARGS__);                \
} while (0)

#define EQ_ASSERT_API0(statement) do {                                             \
  if (statement) break;                                                            \
  A("Assertion failure: %s failed: [%d]%s", #statement, errno, strerror(errno));   \
} while (0)

#define EQ_ASSERT_API(api) do {                                              \
  A("Assertion failure: %s failed: [%d]%s", #api, errno, strerror(errno));   \
} while (0)


#endif // _elog_h_8897be44_dda8_45b6_9d37_8d8691cb05fb_


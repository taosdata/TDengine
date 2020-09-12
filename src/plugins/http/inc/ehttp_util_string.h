#ifndef _ehttp_util_string_h_99dacde5_2e7d_4662_97d6_04611fde683b_
#define _ehttp_util_string_h_99dacde5_2e7d_4662_97d6_04611fde683b_

#include <stddef.h>

typedef struct ehttp_util_string_s   ehttp_util_string_t;

struct ehttp_util_string_s {
  char                        *str;
  size_t                       len;
};

void ehttp_util_string_cleanup(ehttp_util_string_t *str);
int  ehttp_util_string_append(ehttp_util_string_t *str, const char *s, size_t len);
void ehttp_util_string_clear(ehttp_util_string_t *str);

#endif // _ehttp_util_string_h_99dacde5_2e7d_4662_97d6_04611fde683b_


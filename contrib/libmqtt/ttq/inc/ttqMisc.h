#ifndef _TD_MISC_TTQ_H_
#define _TD_MISC_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdio.h>

FILE *tmqtt__fopen(const char *path, const char *mode, bool restrict_read);
char *misc__trimblanks(char *str);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MISC_TTQ_H_*/

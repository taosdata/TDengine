#ifndef _TD_TTQ_MEMORY_H_
#define _TD_TTQ_MEMORY_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

void *ttq_calloc(size_t nmemb, size_t size);
void  ttq_free(void *mem);
void *ttq_malloc(size_t size);
void *ttq_realloc(void *ptr, size_t size);
char *ttq_strdup(const char *s);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TTQ_MEMORY_H_*/

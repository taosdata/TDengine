#include "ttqMemory.h"

#include <stdlib.h>
#include <string.h>

#ifdef REAL_WITH_MEMORY_TRACKING
#if defined(__APPLE__)
#include <malloc/malloc.h>
#define malloc_usable_size malloc_size
#else
#include <malloc.h>
#endif
#endif

#ifdef REAL_WITH_MEMORY_TRACKING
static unsigned long memcount = 0;
static unsigned long max_memcount = 0;
#endif

#ifdef WITH_BROKER
static size_t mem_limit = 0;
void          memory__set_limit(size_t lim) { mem_limit = lim; }
#endif

void *ttq_calloc(size_t nmemb, size_t size) {
  void *mem;
#ifdef REAL_WITH_MEMORY_TRACKING
  if (mem_limit && memcount + size > mem_limit) {
    return NULL;
  }
#endif
  mem = calloc(nmemb, size);

#ifdef REAL_WITH_MEMORY_TRACKING
  if (mem) {
    memcount += malloc_usable_size(mem);
    if (memcount > max_memcount) {
      max_memcount = memcount;
    }
  }
#endif

  return mem;
}

void ttq_free(void *mem) {
#ifdef REAL_WITH_MEMORY_TRACKING
  if (!mem) {
    return;
  }
  memcount -= malloc_usable_size(mem);
#endif
  free(mem);
}

void *ttq_malloc(size_t size) {
  void *mem;

#ifdef REAL_WITH_MEMORY_TRACKING
  if (mem_limit && memcount + size > mem_limit) {
    return NULL;
  }
#endif

  mem = malloc(size);

#ifdef REAL_WITH_MEMORY_TRACKING
  if (mem) {
    memcount += malloc_usable_size(mem);
    if (memcount > max_memcount) {
      max_memcount = memcount;
    }
  }
#endif

  return mem;
}

void *ttq_realloc(void *ptr, size_t size) {
  void *mem;
#ifdef REAL_WITH_MEMORY_TRACKING
  if (mem_limit && memcount + size > mem_limit) {
    return NULL;
  }
  if (ptr) {
    memcount -= malloc_usable_size(ptr);
  }
#endif
  mem = realloc(ptr, size);

#ifdef REAL_WITH_MEMORY_TRACKING
  if (mem) {
    memcount += malloc_usable_size(mem);
    if (memcount > max_memcount) {
      max_memcount = memcount;
    }
  }
#endif

  return mem;
}

char *ttq_strdup(const char *s) {
  char *str;
#ifdef REAL_WITH_MEMORY_TRACKING
  if (mem_limit && memcount + strlen(s) > mem_limit) {
    return NULL;
  }
#endif
  str = strdup(s);

#ifdef REAL_WITH_MEMORY_TRACKING
  if (str) {
    memcount += malloc_usable_size(str);
    if (memcount > max_memcount) {
      max_memcount = memcount;
    }
  }
#endif

  return str;
}

#ifndef _ehttp_gzip_h_9196791b_ac2a_4d73_9979_f4b41abbc4c0_
#define _ehttp_gzip_h_9196791b_ac2a_4d73_9979_f4b41abbc4c0_

#include <stddef.h>

#define EHTTP_GZIP_CHUNK_SIZE_DEFAULT      (1024*16)

typedef struct ehttp_gzip_s              ehttp_gzip_t;

typedef struct ehttp_gzip_callbacks_s    ehttp_gzip_callbacks_t;
typedef struct ehttp_gzip_conf_s         ehttp_gzip_conf_t;

struct ehttp_gzip_callbacks_s {
  void (*on_data)(ehttp_gzip_t *gzip, void *arg, const char *buf, size_t len);
};

struct ehttp_gzip_conf_s {
  int        get_header:2; // 0: not fetching header info
  size_t     chunk_size;   // 0: fallback to default: EHTTP_GZIP_CHUNK_SIZE_DEFAULT
};

ehttp_gzip_t* ehttp_gzip_create_decompressor(ehttp_gzip_conf_t conf, ehttp_gzip_callbacks_t callbacks, void *arg);
ehttp_gzip_t* ehttp_gzip_create_compressor(ehttp_gzip_conf_t conf, ehttp_gzip_callbacks_t callbacks, void *arg);
void          ehttp_gzip_destroy(ehttp_gzip_t *gzip);

int           ehttp_gzip_write(ehttp_gzip_t *gzip, const char *buf, size_t len);
int           ehttp_gzip_finish(ehttp_gzip_t *gzip);

#endif // _ehttp_gzip_h_9196791b_ac2a_4d73_9979_f4b41abbc4c0_


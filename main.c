#ifdef _ext_cjson
#include "cJSON.h"
#endif
#ifdef _ext_lz4
#include "lz4.h"
#endif
#ifdef _ext_libuv
#include "uv.h"
#endif
#ifdef _ext_geos
#include "geos_c.h"
#endif
#ifdef _ext_pcre2
#define PCRE2_CODE_UNIT_WIDTH 8
#include "pcre2.h"
// #include "pcre2posix.h"
#endif
#ifdef _ext_lzma2
#include "xxhash.h"
#endif
#ifdef _ext_pthread
#include "pthread.h"
#endif
#ifdef _ext_iconv
#include "iconv.h"
#endif
#ifdef _ext_msvcregex
#include "regex.h"
#endif
#ifdef _ext_wcwidth
#include <wchar.h>
extern int wcwidth(wchar_t c);
#endif
#ifdef _ext_rocksdb
#include "rocksdb/c.h"
#endif
#ifdef _ext_zlib
#include "zlib.h"
#endif

int main(void)
{
  if (1) return 0;
#ifdef _ext_cjson
  cJSON *root = cJSON_Parse("{\"key1\": \"value1\", \"key2\": \"value2\"}");
  cJSON_Delete(root);
#endif
#ifdef _ext_lz4
  LZ4_stream_t *stream = LZ4_createStream();
  LZ4_freeStream(stream);
#endif
#ifdef _ext_libuv
  uv_loop_t *loop = uv_default_loop();
  uv_loop_init(loop);
  uv_loop_close(loop);
#endif
#ifdef _ext_geos
  GEOSContextHandle_t handle = GEOS_init_r();
  GEOS_finish_r(handle);
#endif

#ifdef _ext_pcre2
  if (1) {
    int        errorcode;
    PCRE2_SIZE erroroffset;
    pcre2_code*       pRegex = NULL;
    pcre2_match_data* pMatchData = NULL;
    pRegex = pcre2_compile((PCRE2_SPTR8)NULL, PCRE2_ZERO_TERMINATED, PCRE2_CASELESS, &errorcode, &erroroffset, NULL);
    pMatchData = pcre2_match_data_create_from_pattern(pRegex, NULL);
    int32_t ret = 0;
    ret = pcre2_match(pRegex, (PCRE2_SPTR)NULL, PCRE2_ZERO_TERMINATED, 0, 0, pMatchData, NULL);
    pcre2_code_free(pRegex);
    pcre2_match_data_free(pMatchData);
  }
  // if (1) {
  //   regex_t re;
  //   pcre2_regcomp(&re, "", 0);
  //   regmatch_t match;
  //   pcre2_regexec(&re, "", 0, &match, 0);
  //   pcre2_regfree(&re);
  // }
#endif

#ifdef _ext_lzma2
  XXH64_state_t *state = XXH64_createState();
  XXH64_freeState(state);
#endif

#ifdef _ext_pthread
  pthread_t thread;
  pthread_create(&thread, NULL, NULL, NULL);
  pthread_join(thread, NULL);
#endif

#ifdef _ext_iconv
  iconv_t cd = iconv_open("UTF-8", "UTF-8");
  iconv_close(cd);
#endif

#ifdef _ext_msvcregex
  regex_t re;
  regcomp(&re, "", 0);
  regmatch_t match; 
  regexec(&re, "", 0, &match, 0);
  regfree(&re);
#endif

#ifdef _ext_wcwitdh
  wchar_t c = L'x';
  wcwidth(c);
  #error not implemented
#endif

#ifdef _ext_rocksdb
  rocksdb_options_t* options = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(options, 1);
  char *err;
  rocksdb_t *db = rocksdb_open(options, "", &err);
  rocksdb_close(db);
#endif

#ifdef _ext_zlib
  z_stream strm;
  deflateInit(&strm, Z_DEFAULT_COMPRESSION);
  deflate(&strm, Z_FINISH);
  deflateEnd(&strm);
#endif

  return 0;
}

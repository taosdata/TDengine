#include "os.h"
#include "tutil.h"

#include "tname.h"
#include "tstoken.h"
#include "ttokendef.h"

// todo refactor
__attribute__((unused)) static FORCE_INLINE const char* skipSegments(const char* input, char delim, int32_t num) {
  for (int32_t i = 0; i < num; ++i) {
    while (*input != 0 && *input++ != delim) {
    };
  }
  return input;
}

__attribute__((unused)) static FORCE_INLINE size_t copy(char* dst, const char* src, char delimiter) {
  size_t len = 0;
  while (*src != delimiter && *src != 0) {
    *dst++ = *src++;
    len++;
  }
  
  return len;
}

void extractTableName(const char* tableId, char* name) {
  size_t s1 = strcspn(tableId, &TS_PATH_DELIMITER[0]);
  size_t s2 = strcspn(&tableId[s1 + 1], &TS_PATH_DELIMITER[0]);
  
  strncpy(name, &tableId[s1 + s2 + 2], TSDB_TABLE_NAME_LEN);
  name[TSDB_TABLE_NAME_LEN] = 0;
}

char* extractDBName(const char* tableId, char* name) {
  size_t offset1 = strcspn(tableId, &TS_PATH_DELIMITER[0]);
  size_t len = strcspn(&tableId[offset1 + 1], &TS_PATH_DELIMITER[0]);
  
  return strncpy(name, &tableId[offset1 + 1], len);
}

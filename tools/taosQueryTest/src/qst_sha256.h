/* Self-contained SHA-256 used by versioned on-disk and shared formats. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_SHA256_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_SHA256_H_

#include <stdint.h>

typedef struct {
  uint32_t state[8];
  uint64_t total_bytes;
  uint32_t buffered_bytes;
  uint8_t buffer[64];
} SQstSha256;

void qst_sha256_init(SQstSha256 *context);
void qst_sha256_update(SQstSha256 *context, const uint8_t *bytes,
                       uint64_t length);
void qst_sha256_final(SQstSha256 *context, uint8_t digest[32]);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_SHA256_H_

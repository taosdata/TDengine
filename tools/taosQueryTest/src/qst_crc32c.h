/* Internal CRC32C helper shared by immutable shared-memory formats. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_CRC32C_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_CRC32C_H_

#include <stdint.h>

uint32_t qstCrc32c(uint32_t crc, const void *data, uint64_t length);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_CRC32C_H_

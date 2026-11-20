// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#ifndef _TD_UTIL_TEST_GATE_H_
#define _TD_UTIL_TEST_GATE_H_

#include "os.h"
#include "taoserror.h"

static inline bool taosTestGatePath(char* path, size_t capacity,
                                     const char* directory,
                                     const char* name) {
  int32_t len = snprintf(path, capacity, "%s/%s", directory, name);
  return len >= 0 && (size_t)len < capacity;
}

// Require the entire file to identify this query, including its newline.
static inline bool taosTestGateArmed(const char* directory, uint64_t queryId) {
  int32_t savedErrno = terrno;
  bool matches = false;
  char arm[PATH_MAX];
  if (taosTestGatePath(arm, sizeof(arm), directory, "arm")) {
    TdFilePtr file = taosOpenPrivateFile(arm, TD_FILE_READ);
    if (file != NULL) {
      char expected[32], actual[32];
      int32_t len = snprintf(expected, sizeof(expected), "%" PRIu64 "\n",
                              queryId);
      int64_t size = taosReadFile(file, actual, sizeof(actual));
      int32_t code = taosCloseFile(&file);
      matches = code == TSDB_CODE_SUCCESS && len > 0 &&
                len < (int32_t)sizeof(expected) && size == len &&
                memcmp(actual, expected, len) == 0;
    }
  }
  terrno = savedErrno;
  return matches;
}

#endif  // _TD_UTIL_TEST_GATE_H_

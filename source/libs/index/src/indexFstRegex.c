/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "indexFstRegex.h"
#include "indexFstDfa.h"
#include "indexFstSparse.h"

FstRegex *regexCreate(const char *str) {
  FstRegex *regex = taosMemoryCalloc(1, sizeof(FstRegex));
  if (regex == NULL) {
    return NULL;
  }

  regex->orig = taosStrdup(str);

  // construct insts based on str
  SArray *insts = taosArrayInit(256, sizeof(uint8_t));
  for (int i = 0; i < strlen(str); i++) {
    uint8_t v = str[i];
    taosArrayPush(insts, &v);
  }
  FstDfaBuilder *builder = dfaBuilderCreate(insts);
  regex->dfa = dfaBuilderBuild(builder);
  return regex;
}

void regexDestroy(FstRegex *regex) {
  if (regex == NULL) return;
  taosMemoryFree(regex->orig);
  taosMemoryFree(regex);
}

#ifdef BUILD_NO_CALL
uint32_t regexAutomStart(FstRegex *regex) {
  ///// no nothing
  return 0;
}
bool regexAutomIsMatch(FstRegex *regex, uint32_t state) {
  if (regex->dfa != NULL && dfaIsMatch(regex->dfa, state)) {
    return true;
  } else {
    return false;
  }
}

bool regexAutomCanMatch(FstRegex *regex, uint32_t state, bool null) {
  // make frame happy
  return null;
}

bool regexAutomAccept(FstRegex *regex, uint32_t state, uint8_t byte, uint32_t *result) {
  if (regex->dfa == NULL) {
    return false;
  }
  return dfaAccept(regex->dfa, state, byte, result);
}
#endif
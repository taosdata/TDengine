#ifndef TDENGINE_TEP_H
#define TDENGINE_TEP_H

#include "os.h"
#include "tmsg.h"

typedef struct SCorEpSet {
  int32_t version;
  SEpSet  epSet;
} SCorEpSet;

int taosGetFqdnPortFromEp(const char *ep, char *fqdn, uint16_t *port);
bool isEpsetEqual(const SEpSet *s1, const SEpSet *s2);

void updateEpSet_s(SCorEpSet *pEpSet, SEpSet *pNewEpSet);
SEpSet getEpSet_s(SCorEpSet *pEpSet);

#endif  // TDENGINE_TEP_H

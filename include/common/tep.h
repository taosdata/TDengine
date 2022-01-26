#ifndef TDENGINE_TEP_H
#define TDENGINE_TEP_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "tmsg.h"

typedef struct SCorEpSet {
  int32_t version;
  SEpSet  epSet;
} SCorEpSet;

int  taosGetFqdnPortFromEp(const char *ep, SEp *pEp);
void addEpIntoEpSet(SEpSet *pEpSet, const char *fqdn, uint16_t port);

bool isEpsetEqual(const SEpSet *s1, const SEpSet *s2);

void   updateEpSet_s(SCorEpSet *pEpSet, SEpSet *pNewEpSet);
SEpSet getEpSet_s(SCorEpSet *pEpSet);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TEP_H

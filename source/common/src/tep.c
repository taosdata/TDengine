#include "tep.h"
#include "tglobal.h"
#include "tlockfree.h"

int taosGetFqdnPortFromEp(const char *ep, char *fqdn, uint16_t *port) {
  *port = 0;
  strcpy(fqdn, ep);

  char *temp = strchr(fqdn, ':');
  if (temp) {
    *temp = 0;
    *port = atoi(temp+1);
  }

  if (*port == 0) {
    *port = tsServerPort;
    return -1;
  }

  return 0;
}

bool isEpsetEqual(const SEpSet *s1, const SEpSet *s2) {
  if (s1->numOfEps != s2->numOfEps || s1->inUse != s2->inUse) {
    return false;
  }

  for (int32_t i = 0; i < s1->numOfEps; i++) {
    if (s1->port[i] != s2->port[i]
        || strncmp(s1->fqdn[i], s2->fqdn[i], TSDB_FQDN_LEN) != 0)
      return false;
  }
  return true;
}

void updateEpSet_s(SCorEpSet *pEpSet, SEpSet *pNewEpSet) {
  taosCorBeginWrite(&pEpSet->version);
  pEpSet->epSet = *pNewEpSet;
  taosCorEndWrite(&pEpSet->version);
}

SEpSet getEpSet_s(SCorEpSet *pEpSet) {
  SEpSet ep = {0};
  taosCorBeginRead(&pEpSet->version);
  ep = pEpSet->epSet;
  taosCorEndRead(&pEpSet->version);

  return ep;
}


#include "tep.h"
#include "tglobal.h"
#include "tlockfree.h"

int taosGetFqdnPortFromEp(const char *ep, SEp* pEp) {
  pEp->port = 0;
  strcpy(pEp->fqdn, ep);

  char *temp = strchr(pEp->fqdn, ':');
  if (temp) {
    *temp = 0;
    pEp->port = atoi(temp+1);
  }

  if (pEp->port == 0) {
    pEp->port = tsServerPort;
    return -1;
  }

  return 0;
}

void addEpIntoEpSet(SEpSet *pEpSet, const char* fqdn, uint16_t port) {
  if (pEpSet == NULL || fqdn == NULL || strlen(fqdn) == 0) {
    return;
  }

  int32_t index = pEpSet->numOfEps;
  tstrncpy(pEpSet->eps[index].fqdn, fqdn, tListLen(pEpSet->eps[index].fqdn));
  pEpSet->eps[index].port = port;
  pEpSet->numOfEps += 1;
}

bool isEpsetEqual(const SEpSet *s1, const SEpSet *s2) {
  if (s1->numOfEps != s2->numOfEps || s1->inUse != s2->inUse) {
    return false;
  }

  for (int32_t i = 0; i < s1->numOfEps; i++) {
    if (s1->eps[i].port != s2->eps[i].port
        || strncmp(s1->eps[i].fqdn, s2->eps[i].fqdn, TSDB_FQDN_LEN) != 0)
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


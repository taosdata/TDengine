#define _DEFAULT_SOURCE
#include "cluster.h"
#include "clusterDnode.h"
#include "clusterDnodeMgmt.h"

void clusterInit() {
  dnodeInitMgmtFp = dnodeInitMgmtImp;
  dnodeInitMgmtIpFp = dnodeInitMgmtIpImp;
  dnodeProcessStatusRspFp = dnodeProcessStatusRspImp;
  dnodeSendMsgToMnodeFp = dnodeSendMsgToMnodeImp;

  mgmtInitDnodeIntFp = mgmtInitDnodeIntFpImp;
  mgmtCleanUpDnodeIntFp = mgmtCleanUpDnodeIntImp;

  mgmtInitDnodesFp    = mgmtInitDnodesImp;
  mgmtCleanUpDnodesFp = mgmtCleanUpDnodesImp;
  mgmtGetDnodeFp      = mgmtGetDnodeFp;
  mgmtGetDnodesNumFp  = mgmtGetDnodesNumImp;
  mgmtGetNextDnodeFp  = mgmtGetNextDnodeImp;
}
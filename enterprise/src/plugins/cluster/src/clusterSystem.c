#define _DEFAULT_SOURCE
#include "cluster.h"
#include "clusterDnode.h"
#include "clusterMgmtConn.h"
#include "clusterDnodeConn.h"

void clusterInit() {
  // dnodeMgmt & clusterDnodeConn
  dnodeInitMgmtFp         = dnodeInitMgmtImp;
  dnodeCleanUpMgmtFp      = dnodeCleanUpMgmtImp;
  dnodeInitMgmtIpFp       = dnodeInitMgmtIpImp;
  dnodeProcessStatusRspFp = dnodeProcessStatusRspImp;
  dnodeSendMsgToMnodeFp   = dnodeSendMsgToMnodeImp;
  dnodeSendRspToMnodeFp   = dnodeSendRspToMnodeImp;

  // mgmtDnodeInt & clusterMgmtConn
  mgmtInitDnodeIntFp    = mgmtInitDnodeIntImp;
  mgmtCleanUpDnodeIntFp = mgmtCleanUpDnodeIntImp;
  mgmtSendMsgToDnodeFp  = mgmtSendMsgToDnodeImp;
  mgmtSendRspToDnodeFp  = mgmtSendRspToDnodeImp;

  // mgmtDnode & clusterDnode
  mgmtInitDnodesFp       = mgmtInitDnodesImp;
  mgmtCleanUpDnodesFp    = mgmtCleanUpDnodesImp;
  mgmtGetDnodeFp         = mgmtGetDnodeFp;
  mgmtGetDnodesNumFp     = mgmtGetDnodesNumImp;
  mgmtUpdateDnodeFp      = mgmtUpdateDnodeImp;
  mgmtGetNextDnodeFp     = mgmtGetNextDnodeImp;
  mgmtCreateDnodeFp      = mgmtCreateDnode;
  mgmtDropDnodeByIpFp    = mgmtDropDnodeByIp;
}

#define _DEFAULT_SOURCE
#include "cluster.h"
#include "clusterDnode.h"
#include "clusterMgmtConn.h"
#include "clusterDnodeConn.h"

void clusterInit() {
  dnodeInitMgmtFp = dnodeInitMgmtImp;
  dnodeCleanUpMgmtFp = dnodeCleanUpMgmtImp;
  dnodeInitMgmtIpFp = dnodeInitMgmtIpImp;
  dnodeProcessStatusRspFp = dnodeProcessStatusRspImp;
  dnodeSendMsgToMnodeFp = dnodeSendMsgToMnodeImp;
  dnodeSendRspToMnodeFp = dnodeSendRspToMnodeImp;


  mgmtInitDnodeIntFp = mgmtInitDnodeIntImp;
  mgmtCleanUpDnodeIntFp = mgmtCleanUpDnodeIntImp;

  mgmtInitDnodeIntFp    = mgmtInitDnodeIntImp;
  mgmtCleanUpDnodeIntFp = mgmtCleanUpDnodeIntImp;
  mgmtSendMsgToDnodeFp  = mgmtSendMsgToDnodeImp;
  mgmtSendRspToDnodeFp  = mgmtSendRspToDnodeImp;

  mgmtInitDnodesFp       = mgmtInitDnodesImp;
  mgmtCleanUpDnodesFp    = mgmtCleanUpDnodesImp;
  mgmtGetDnodeFp         = mgmtGetDnodeFp;
  mgmtGetDnodesNumFp     = mgmtGetDnodesNumImp;
  mgmtUpdateDnodeFp      = mgmtUpdateDnodeImp;
  mgmtGetNextDnodeFp     = mgmtGetNextDnodeImp;
  mgmtSetDnodeUnRemoveFp = mgmtSetDnodeUnRemoveImp;
  mgmtCreateDnodeFp      = mgmtCreateDnode;
  mgmtDropDnodeByIpFp    = mgmtDropDnodeByIp;
}

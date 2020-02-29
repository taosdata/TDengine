#define _DEFAULT_SOURCE
#include "cluster.h"
#include "clusterDnodeMgmt.h"

void clusterInit() {
  dnodeInitMgmtFp = dnodeInitMgmtImp;
  dnodeInitMgmtIpFp = dnodeInitMgmtIpImp;
  dnodeProcessStatusRspFp = dnodeProcessStatusRspImp;
  dnodeSendMsgToMnodeFp = dnodeSendMsgToMnodeImp;
}
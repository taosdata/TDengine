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

// extConnector.c — community edition stub
//
// All public APIs return TSDB_CODE_OPS_NOT_SUPPORT in the community edition.
// extConnectorModuleInit() / extConnectorModuleDestroy() are no-ops (succeed
// silently) so that the rest of the startup/shutdown flow is not disrupted.

#ifndef TD_ENTERPRISE

#include "extConnector.h"

int32_t extConnectorModuleInit(const SExtConnectorModuleCfg *cfg) {
  (void)cfg;
  return TSDB_CODE_SUCCESS;
}

void extConnectorModuleDestroy(void) {}

int32_t extConnectorOpen(const SExtSourceCfg *cfg, SExtConnectorHandle **ppHandle) {
  (void)cfg;
  (void)ppHandle;
  uError("extConnectorOpen: operation not supported in community edition");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void extConnectorClose(SExtConnectorHandle *pHandle) { (void)pHandle; }

int32_t extConnectorGetTableSchema(SExtConnectorHandle *pHandle, const SExtTableNode *pTable,
                                   SExtTableMeta **ppOut) {
  (void)pHandle;
  (void)pTable;
  (void)ppOut;
  uError("extConnectorGetTableSchema: operation not supported in community edition");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void extConnectorFreeTableSchema(SExtTableMeta *pMeta) { (void)pMeta; }

SExtTableMeta* extConnectorCloneTableSchema(const SExtTableMeta *pMeta) {
  (void)pMeta;
  return NULL;
}

int32_t extConnectorGetCapabilities(SExtConnectorHandle *pHandle, const SExtTableNode *pTable,
                                    SExtSourceCapability *pOut) {
  (void)pHandle;
  (void)pTable;
  (void)pOut;
  uError("extConnectorGetCapabilities: operation not supported in community edition");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int32_t extConnectorExecQuery(SExtConnectorHandle *pHandle, const SFederatedScanPhysiNode *pNode,
                              const char *pSQL,
                              SExtQueryHandle **ppQHandle, SExtConnectorError *pOutErr) {
  (void)pHandle;
  (void)pNode;
  (void)pSQL;
  (void)ppQHandle;
  (void)pOutErr;
  uError("extConnectorExecQuery: operation not supported in community edition");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int32_t extConnectorFetchBlock(SExtQueryHandle *pQHandle, const SExtColTypeMapping *pColMappings,
                               int32_t numColMappings, SSDataBlock **ppOut,
                               SExtConnectorError *pOutErr) {
  (void)pQHandle;
  (void)pColMappings;
  (void)numColMappings;
  (void)ppOut;
  (void)pOutErr;
  uError("extConnectorFetchBlock: operation not supported in community edition");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void extConnectorCloseQuery(SExtQueryHandle *pQHandle) { (void)pQHandle; }

#endif  // !TD_ENTERPRISE

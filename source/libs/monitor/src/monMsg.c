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

#define _DEFAULT_SOURCE
#include "monInt.h"
#include "tcoding.h"
#include "tencode.h"

static int32_t tEncodeSMonSysInfo(SEncoder *encoder, const SMonSysInfo *pInfo) {
  if (tEncodeDouble(encoder, pInfo->cpu_engine) < 0) return -1;
  if (tEncodeDouble(encoder, pInfo->cpu_system) < 0) return -1;
  if (tEncodeFloat(encoder, pInfo->cpu_cores) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->mem_engine) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->mem_system) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->mem_total) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->disk_engine) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->disk_used) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->disk_total) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->net_in) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->net_out) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->io_read) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->io_write) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->io_read_disk) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->io_write_disk) < 0) return -1;
  return 0;
}

static int32_t tDecodeSMonSysInfo(SDecoder *decoder, SMonSysInfo *pInfo) {
  if (tDecodeDouble(decoder, &pInfo->cpu_engine) < 0) return -1;
  if (tDecodeDouble(decoder, &pInfo->cpu_system) < 0) return -1;
  if (tDecodeFloat(decoder, &pInfo->cpu_cores) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->mem_engine) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->mem_system) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->mem_total) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->disk_engine) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->disk_used) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->disk_total) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->net_in) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->net_out) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->io_read) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->io_write) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->io_read_disk) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->io_write_disk) < 0) return -1;
  return 0;
}

int32_t tEncodeSMonLogs(SEncoder *encoder, const SMonLogs *pInfo) {
  if (tEncodeI32(encoder, pInfo->numOfErrorLogs) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->numOfInfoLogs) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->numOfDebugLogs) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->numOfTraceLogs) < 0) return -1;
  if (tEncodeI32(encoder, taosArrayGetSize(pInfo->logs)) < 0) return -1;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->logs); ++i) {
    SMonLogItem *pLog = taosArrayGet(pInfo->logs, i);
    if (tEncodeI64(encoder, pLog->ts) < 0) return -1;
    if (tEncodeI8(encoder, pLog->level) < 0) return -1;
    if (tEncodeCStr(encoder, pLog->content) < 0) return -1;
  }
  return 0;
}

static int32_t tDecodeSMonLogs(SDecoder *decoder, SMonLogs *pInfo) {
  if (tDecodeI32(decoder, &pInfo->numOfErrorLogs) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->numOfInfoLogs) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->numOfDebugLogs) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->numOfTraceLogs) < 0) return -1;

  int32_t arraySize = 0;
  if (tDecodeI32(decoder, &arraySize) < 0) return -1;

  pInfo->logs = taosArrayInit(arraySize, sizeof(SMonLogItem));
  if (pInfo->logs == NULL) return -1;

  for (int32_t i = 0; i < arraySize; ++i) {
    SMonLogItem desc = {0};
    if (tDecodeI64(decoder, &desc.ts) < 0) return -1;
    int8_t level = 0;
    if (tDecodeI8(decoder, &level) < 0) return -1;
    desc.level = level;
    if (tDecodeCStrTo(decoder, desc.content) < 0) return -1;
    taosArrayPush(pInfo->logs, &desc);
  }

  return 0;
}

int32_t tEncodeSMonClusterInfo(SEncoder *encoder, const SMonClusterInfo *pInfo) {
  if (tEncodeCStr(encoder, pInfo->first_ep) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->first_ep_dnode_id) < 0) return -1;
  if (tEncodeCStr(encoder, pInfo->version) < 0) return -1;
  if (tEncodeFloat(encoder, pInfo->master_uptime) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->monitor_interval) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->dbs_total) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->stbs_total) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->tbs_total) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->vgroups_total) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->vgroups_alive) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->vnodes_total) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->vnodes_alive) < 0) return -1;
  if (tEncodeI32(encoder, pInfo->connections_total) < 0) return -1;
  if (tEncodeI32(encoder, taosArrayGetSize(pInfo->dnodes)) < 0) return -1;
  if (tEncodeI32(encoder, taosArrayGetSize(pInfo->mnodes)) < 0) return -1;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->dnodes); ++i) {
    SMonDnodeDesc *pDesc = taosArrayGet(pInfo->dnodes, i);
    if (tEncodeI32(encoder, pDesc->dnode_id) < 0) return -1;
    if (tEncodeCStr(encoder, pDesc->dnode_ep) < 0) return -1;
    if (tEncodeCStr(encoder, pDesc->status) < 0) return -1;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->mnodes); ++i) {
    SMonMnodeDesc *pDesc = taosArrayGet(pInfo->mnodes, i);
    if (tEncodeI32(encoder, pDesc->mnode_id) < 0) return -1;
    if (tEncodeCStr(encoder, pDesc->mnode_ep) < 0) return -1;
    if (tEncodeCStr(encoder, pDesc->role) < 0) return -1;
  }
  return 0;
}

int32_t tDecodeSMonClusterInfo(SDecoder *decoder, SMonClusterInfo *pInfo) {
  if (tDecodeCStrTo(decoder, pInfo->first_ep) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->first_ep_dnode_id) < 0) return -1;
  if (tDecodeCStrTo(decoder, pInfo->version) < 0) return -1;
  if (tDecodeFloat(decoder, &pInfo->master_uptime) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->monitor_interval) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->dbs_total) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->stbs_total) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->tbs_total) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->vgroups_total) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->vgroups_alive) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->vnodes_total) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->vnodes_alive) < 0) return -1;
  if (tDecodeI32(decoder, &pInfo->connections_total) < 0) return -1;

  int32_t dnodesSize = 0;
  int32_t mnodesSize = 0;
  if (tDecodeI32(decoder, &dnodesSize) < 0) return -1;
  if (tDecodeI32(decoder, &mnodesSize) < 0) return -1;

  pInfo->dnodes = taosArrayInit(dnodesSize, sizeof(SMonDnodeDesc));
  pInfo->mnodes = taosArrayInit(mnodesSize, sizeof(SMonMnodeDesc));
  if (pInfo->dnodes == NULL || pInfo->mnodes == NULL) return -1;

  for (int32_t i = 0; i < dnodesSize; ++i) {
    SMonDnodeDesc desc = {0};
    if (tDecodeI32(decoder, &desc.dnode_id) < 0) return -1;
    if (tDecodeCStrTo(decoder, desc.dnode_ep) < 0) return -1;
    if (tDecodeCStrTo(decoder, desc.status) < 0) return -1;
    taosArrayPush(pInfo->dnodes, &desc);
  }

  for (int32_t i = 0; i < mnodesSize; ++i) {
    SMonMnodeDesc desc = {0};
    if (tDecodeI32(decoder, &desc.mnode_id) < 0) return -1;
    if (tDecodeCStrTo(decoder, desc.mnode_ep) < 0) return -1;
    if (tDecodeCStrTo(decoder, desc.role) < 0) return -1;
    taosArrayPush(pInfo->mnodes, &desc);
  }
  return 0;
}

int32_t tEncodeSMonVgroupInfo(SEncoder *encoder, const SMonVgroupInfo *pInfo) {
  if (tEncodeI32(encoder, taosArrayGetSize(pInfo->vgroups)) < 0) return -1;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->vgroups); ++i) {
    SMonVgroupDesc *pDesc = taosArrayGet(pInfo->vgroups, i);
    if (tEncodeI32(encoder, pDesc->vgroup_id) < 0) return -1;
    if (tEncodeI32(encoder, pDesc->tables_num) < 0) return -1;
    if (tEncodeCStr(encoder, pDesc->database_name) < 0) return -1;
    if (tEncodeCStr(encoder, pDesc->status) < 0) return -1;
    for (int32_t j = 0; j < TSDB_MAX_REPLICA; ++j) {
      SMonVnodeDesc *pVDesc = &pDesc->vnodes[j];
      if (tEncodeI32(encoder, pVDesc->dnode_id) < 0) return -1;
      if (tEncodeCStr(encoder, pVDesc->vnode_role) < 0) return -1;
    }
  }
  return 0;
}

int32_t tDecodeSMonVgroupInfo(SDecoder *decoder, SMonVgroupInfo *pInfo) {
  int32_t arraySize = 0;
  if (tDecodeI32(decoder, &arraySize) < 0) return -1;

  pInfo->vgroups = taosArrayInit(arraySize, sizeof(SMonVgroupDesc));
  if (pInfo->vgroups == NULL) return -1;

  for (int32_t i = 0; i < arraySize; ++i) {
    SMonVgroupDesc desc = {0};
    if (tDecodeI32(decoder, &desc.vgroup_id) < 0) return -1;
    if (tDecodeI32(decoder, &desc.tables_num) < 0) return -1;
    if (tDecodeCStrTo(decoder, desc.database_name) < 0) return -1;
    if (tDecodeCStrTo(decoder, desc.status) < 0) return -1;
    for (int32_t j = 0; j < TSDB_MAX_REPLICA; ++j) {
      SMonVnodeDesc *pVDesc = &desc.vnodes[j];
      if (tDecodeI32(decoder, &pVDesc->dnode_id) < 0) return -1;
      if (tDecodeCStrTo(decoder, pVDesc->vnode_role) < 0) return -1;
    }
    taosArrayPush(pInfo->vgroups, &desc);
  }
  return 0;
}

int32_t tEncodeSMonStbInfo(SEncoder *encoder, const SMonStbInfo *pInfo) {
  if (tEncodeI32(encoder, taosArrayGetSize(pInfo->stbs)) < 0) return -1;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->stbs); ++i) {
    SMonStbDesc *pDesc = taosArrayGet(pInfo->stbs, i);
    if (tEncodeCStr(encoder, pDesc->stb_name) < 0) return -1;
    if (tEncodeCStr(encoder, pDesc->database_name) < 0) return -1;
  }
  return 0;
}

int32_t tDecodeSMonStbInfo(SDecoder *decoder, SMonStbInfo *pInfo) {
  int32_t arraySize = 0;
  if (tDecodeI32(decoder, &arraySize) < 0) return -1;

  pInfo->stbs = taosArrayInit(arraySize, sizeof(SMonStbDesc));
  if (pInfo->stbs == NULL) return -1;

  for (int32_t i = 0; i < arraySize; ++i) {
    SMonStbDesc desc = {0};
    if (tDecodeCStrTo(decoder, desc.stb_name) < 0) return -1;
    if (tDecodeCStrTo(decoder, desc.database_name) < 0) return -1;
    taosArrayPush(pInfo->stbs, &desc);
  }
  return 0;
}

int32_t tEncodeSMonGrantInfo(SEncoder *encoder, const SMonGrantInfo *pInfo) {
  if (tEncodeI32(encoder, pInfo->expire_time) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->timeseries_used) < 0) return -1;
  if (tEncodeI64(encoder, pInfo->timeseries_total) < 0) return -1;
  return 0;
}

int32_t tDecodeSMonGrantInfo(SDecoder *decoder, SMonGrantInfo *pInfo) {
  if (tDecodeI32(decoder, &pInfo->expire_time) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->timeseries_used) < 0) return -1;
  if (tDecodeI64(decoder, &pInfo->timeseries_total) < 0) return -1;
  return 0;
}

int32_t tSerializeSMonMmInfo(void *buf, int32_t bufLen, SMonMmInfo *pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSMonClusterInfo(&encoder, &pInfo->cluster) < 0) return -1;
  if (tEncodeSMonVgroupInfo(&encoder, &pInfo->vgroup) < 0) return -1;
  if (tEncodeSMonStbInfo(&encoder, &pInfo->stb) < 0) return -1;
  if (tEncodeSMonGrantInfo(&encoder, &pInfo->grant) < 0) return -1;
  if (tEncodeSMonSysInfo(&encoder, &pInfo->sys) < 0) return -1;
  if (tEncodeSMonLogs(&encoder, &pInfo->log) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMonMmInfo(void *buf, int32_t bufLen, SMonMmInfo *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSMonClusterInfo(&decoder, &pInfo->cluster) < 0) return -1;
  if (tDecodeSMonVgroupInfo(&decoder, &pInfo->vgroup) < 0) return -1;
  if (tDecodeSMonStbInfo(&decoder, &pInfo->stb) < 0) return -1;
  if (tDecodeSMonGrantInfo(&decoder, &pInfo->grant) < 0) return -1;
  if (tDecodeSMonSysInfo(&decoder, &pInfo->sys) < 0) return -1;
  if (tDecodeSMonLogs(&decoder, &pInfo->log) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMonMmInfo(SMonMmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  taosArrayDestroy(pInfo->cluster.mnodes);
  taosArrayDestroy(pInfo->cluster.dnodes);
  taosArrayDestroy(pInfo->vgroup.vgroups);
  taosArrayDestroy(pInfo->stb.stbs);
  pInfo->cluster.mnodes = NULL;
  pInfo->cluster.dnodes = NULL;
  pInfo->vgroup.vgroups = NULL;
  pInfo->stb.stbs = NULL;
  pInfo->log.logs = NULL;
}

int32_t tEncodeSMonDiskDesc(SEncoder *encoder, const SMonDiskDesc *pDesc) {
  if (tEncodeCStr(encoder, pDesc->name) < 0) return -1;
  if (tEncodeI8(encoder, pDesc->level) < 0) return -1;
  if (tEncodeI64(encoder, pDesc->size.total) < 0) return -1;
  if (tEncodeI64(encoder, pDesc->size.used) < 0) return -1;
  if (tEncodeI64(encoder, pDesc->size.avail) < 0) return -1;
  return 0;
}

static int32_t tDecodeSMonDiskDesc(SDecoder *decoder, SMonDiskDesc *pDesc) {
  if (tDecodeCStrTo(decoder, pDesc->name) < 0) return -1;
  if (tDecodeI8(decoder, &pDesc->level) < 0) return -1;
  if (tDecodeI64(decoder, &pDesc->size.total) < 0) return -1;
  if (tDecodeI64(decoder, &pDesc->size.used) < 0) return -1;
  if (tDecodeI64(decoder, &pDesc->size.avail) < 0) return -1;
  return 0;
}

int32_t tEncodeSMonDiskInfo(SEncoder *encoder, const SMonDiskInfo *pInfo) {
  if (tEncodeI32(encoder, taosArrayGetSize(pInfo->datadirs)) < 0) return -1;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->datadirs); ++i) {
    SMonDiskDesc *pDesc = taosArrayGet(pInfo->datadirs, i);
    if (tEncodeSMonDiskDesc(encoder, pDesc) < 0) return -1;
  }
  return 0;
}

static int32_t tDecodeSMonDiskInfo(SDecoder *decoder, SMonDiskInfo *pInfo) {
  int32_t arraySize = 0;
  if (tDecodeI32(decoder, &arraySize) < 0) return -1;

  pInfo->datadirs = taosArrayInit(arraySize, sizeof(SMonDiskDesc));
  if (pInfo->datadirs == NULL) return -1;

  for (int32_t i = 0; i < arraySize; ++i) {
    SMonDiskDesc desc = {0};
    if (tDecodeSMonDiskDesc(decoder, &desc) < 0) return -1;
    taosArrayPush(pInfo->datadirs, &desc);
  }

  return 0;
}

int32_t tEncodeSVnodesStat(SEncoder *encoder, const SVnodesStat *pStat) {
  if (tEncodeI32(encoder, pStat->openVnodes) < 0) return -1;
  if (tEncodeI32(encoder, pStat->totalVnodes) < 0) return -1;
  if (tEncodeI32(encoder, pStat->masterNum) < 0) return -1;
  if (tEncodeI64(encoder, pStat->numOfSelectReqs) < 0) return -1;
  if (tEncodeI64(encoder, pStat->numOfInsertReqs) < 0) return -1;
  if (tEncodeI64(encoder, pStat->numOfInsertSuccessReqs) < 0) return -1;
  if (tEncodeI64(encoder, pStat->numOfBatchInsertReqs) < 0) return -1;
  if (tEncodeI64(encoder, pStat->numOfBatchInsertSuccessReqs) < 0) return -1;
  if (tEncodeI64(encoder, pStat->errors) < 0) return -1;
  return 0;
}

static int32_t tDecodeSVnodesStat(SDecoder *decoder, SVnodesStat *pStat) {
  if (tDecodeI32(decoder, &pStat->openVnodes) < 0) return -1;
  if (tDecodeI32(decoder, &pStat->totalVnodes) < 0) return -1;
  if (tDecodeI32(decoder, &pStat->masterNum) < 0) return -1;
  if (tDecodeI64(decoder, &pStat->numOfSelectReqs) < 0) return -1;
  if (tDecodeI64(decoder, &pStat->numOfInsertReqs) < 0) return -1;
  if (tDecodeI64(decoder, &pStat->numOfInsertSuccessReqs) < 0) return -1;
  if (tDecodeI64(decoder, &pStat->numOfBatchInsertReqs) < 0) return -1;
  if (tDecodeI64(decoder, &pStat->numOfBatchInsertSuccessReqs) < 0) return -1;
  if (tDecodeI64(decoder, &pStat->errors) < 0) return -1;
  return 0;
}

int32_t tSerializeSMonVmInfo(void *buf, int32_t bufLen, SMonVmInfo *pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSMonDiskInfo(&encoder, &pInfo->tfs) < 0) return -1;
  if (tEncodeSVnodesStat(&encoder, &pInfo->vstat) < 0) return -1;
  if (tEncodeSMonSysInfo(&encoder, &pInfo->sys) < 0) return -1;
  if (tEncodeSMonLogs(&encoder, &pInfo->log) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMonVmInfo(void *buf, int32_t bufLen, SMonVmInfo *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSMonDiskInfo(&decoder, &pInfo->tfs) < 0) return -1;
  if (tDecodeSVnodesStat(&decoder, &pInfo->vstat) < 0) return -1;
  if (tDecodeSMonSysInfo(&decoder, &pInfo->sys) < 0) return -1;
  if (tDecodeSMonLogs(&decoder, &pInfo->log) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMonVmInfo(SMonVmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  taosArrayDestroy(pInfo->tfs.datadirs);
  pInfo->log.logs = NULL;
  pInfo->tfs.datadirs = NULL;
}

int32_t tSerializeSMonQmInfo(void *buf, int32_t bufLen, SMonQmInfo *pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSMonSysInfo(&encoder, &pInfo->sys) < 0) return -1;
  if (tEncodeSMonLogs(&encoder, &pInfo->log) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMonQmInfo(void *buf, int32_t bufLen, SMonQmInfo *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSMonSysInfo(&decoder, &pInfo->sys) < 0) return -1;
  if (tDecodeSMonLogs(&decoder, &pInfo->log) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMonQmInfo(SMonQmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  pInfo->log.logs = NULL;
}

int32_t tSerializeSMonSmInfo(void *buf, int32_t bufLen, SMonSmInfo *pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSMonSysInfo(&encoder, &pInfo->sys) < 0) return -1;
  if (tEncodeSMonLogs(&encoder, &pInfo->log) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMonSmInfo(void *buf, int32_t bufLen, SMonSmInfo *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSMonSysInfo(&decoder, &pInfo->sys) < 0) return -1;
  if (tDecodeSMonLogs(&decoder, &pInfo->log) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMonSmInfo(SMonSmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  pInfo->log.logs = NULL;
}

int32_t tSerializeSMonBmInfo(void *buf, int32_t bufLen, SMonBmInfo *pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeSMonSysInfo(&encoder, &pInfo->sys) < 0) return -1;
  if (tEncodeSMonLogs(&encoder, &pInfo->log) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMonBmInfo(void *buf, int32_t bufLen, SMonBmInfo *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeSMonSysInfo(&decoder, &pInfo->sys) < 0) return -1;
  if (tDecodeSMonLogs(&decoder, &pInfo->log) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMonBmInfo(SMonBmInfo *pInfo) {
  taosArrayDestroy(pInfo->log.logs);
  pInfo->log.logs = NULL;
}

int32_t tSerializeSMonVloadInfo(void *buf, int32_t bufLen, SMonVloadInfo *pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, taosArrayGetSize(pInfo->pVloads)) < 0) return -1;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pVloads); ++i) {
    SVnodeLoad *pLoad = taosArrayGet(pInfo->pVloads, i);
    if (tEncodeI32(&encoder, pLoad->vgId) < 0) return -1;
    if (tEncodeI32(&encoder, pLoad->syncState) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->numOfTables) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->numOfTimeSeries) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->totalStorage) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->compStorage) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->pointsWritten) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->numOfSelectReqs) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->numOfInsertReqs) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->numOfInsertSuccessReqs) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->numOfBatchInsertReqs) < 0) return -1;
    if (tEncodeI64(&encoder, pLoad->numOfBatchInsertSuccessReqs) < 0) return -1;
  }
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMonVloadInfo(void *buf, int32_t bufLen, SMonVloadInfo *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  int32_t arraySize = 0;
  if (tDecodeI32(&decoder, &arraySize) < 0) return -1;

  pInfo->pVloads = taosArrayInit(arraySize, sizeof(SVnodeLoad));
  if (pInfo->pVloads == NULL) return -1;

  for (int32_t i = 0; i < arraySize; ++i) {
    SVnodeLoad load = {0};
    if (tDecodeI32(&decoder, &load.vgId) < 0) return -1;
    if (tDecodeI32(&decoder, &load.syncState) < 0) return -1;
    if (tDecodeI64(&decoder, &load.numOfTables) < 0) return -1;
    if (tDecodeI64(&decoder, &load.numOfTimeSeries) < 0) return -1;
    if (tDecodeI64(&decoder, &load.totalStorage) < 0) return -1;
    if (tDecodeI64(&decoder, &load.compStorage) < 0) return -1;
    if (tDecodeI64(&decoder, &load.pointsWritten) < 0) return -1;
    if (tDecodeI64(&decoder, &load.numOfSelectReqs) < 0) return -1;
    if (tDecodeI64(&decoder, &load.numOfInsertReqs) < 0) return -1;
    if (tDecodeI64(&decoder, &load.numOfInsertSuccessReqs) < 0) return -1;
    if (tDecodeI64(&decoder, &load.numOfBatchInsertReqs) < 0) return -1;
    if (tDecodeI64(&decoder, &load.numOfBatchInsertSuccessReqs) < 0) return -1;
    taosArrayPush(pInfo->pVloads, &load);
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

void tFreeSMonVloadInfo(SMonVloadInfo *pInfo) {
  taosArrayDestroy(pInfo->pVloads);
  pInfo->pVloads = NULL;
}

int32_t tSerializeSMonMloadInfo(void *buf, int32_t bufLen, SMonMloadInfo *pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI8(&encoder, pInfo->isMnode) < 0) return -1;
  if (tEncodeI32(&encoder, pInfo->load.syncState) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSMonMloadInfo(void *buf, int32_t bufLen, SMonMloadInfo *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI8(&decoder, &pInfo->isMnode) < 0) return -1;
  if (tDecodeI32(&decoder, &pInfo->load.syncState) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}


int32_t tSerializeSQnodeLoad(void *buf, int32_t bufLen, SQnodeLoad *pInfo) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->numOfProcessedQuery) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->numOfProcessedCQuery) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->numOfProcessedFetch) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->numOfProcessedDrop) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->numOfProcessedHb) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->numOfProcessedDelete) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->cacheDataSize) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->numOfQueryInQueue) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->numOfFetchInQueue) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->timeInQueryQueue) < 0) return -1;
  if (tEncodeI64(&encoder, pInfo->timeInFetchQueue) < 0) return -1;
  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeSQnodeLoad(void *buf, int32_t bufLen, SQnodeLoad *pInfo) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->numOfProcessedQuery) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->numOfProcessedCQuery) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->numOfProcessedFetch) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->numOfProcessedDrop) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->numOfProcessedHb) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->numOfProcessedDelete) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->cacheDataSize) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->numOfQueryInQueue) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->numOfFetchInQueue) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->timeInQueryQueue) < 0) return -1;
  if (tDecodeI64(&decoder, &pInfo->timeInFetchQueue) < 0) return -1;
  tEndDecode(&decoder);

  tDecoderClear(&decoder);
  return 0;
}



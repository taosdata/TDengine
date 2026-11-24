/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#ifndef TDENGINE_STREAM_TIME_ALIGN_H
#define TDENGINE_STREAM_TIME_ALIGN_H
#include "fillPrev.h"
#include "streamMsg.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  col_id_t virtualColId;
  col_id_t sourceColId;
  int32_t  sourceBytes;
} SSTimeAlignColumnRef;

typedef struct {
  int64_t readerTaskId, uid, suid, maxVer;
  SArray* pColRefs;
  bool    emptyRange;
} SSTimeAlignSourceBound;

typedef struct SSTimeAlignSeed SSTimeAlignSeed;

int32_t stTimeAlignSeedCreate(const SStreamTimeAlignSpec* spec, uint64_t virtualUid, uint64_t generation,
                              int64_t beforeTs, const SArray* bounds, SSTimeAlignSeed** seed);
int32_t stTimeAlignSeedConsume(SSTimeAlignSeed* seed, uint64_t sourceUid, const SSDataBlock* block, bool done,
                               bool* ready);
bool    stTimeAlignSeedNeedsSource(const SSTimeAlignSeed* seed, uint64_t uid);
const SFillPrevValue* stTimeAlignSeedValue(const SSTimeAlignSeed* seed, col_id_t virtualId);
void                  stTimeAlignSeedDestroy(SSTimeAlignSeed** seed);

typedef struct SSTimeAlignState  SSTimeAlignState;
typedef struct SSTimeAlignConfig SSTimeAlignConfig;
int32_t                          stTimeAlignConfigCreate(const SStreamTimeAlignSpec* spec, SSTimeAlignConfig** config);
void                             stTimeAlignConfigDestroy(SSTimeAlignConfig** config);
// Returns the earliest useful timestamp across all rules for this virtual source column.
bool stTimeAlignSeedFloor(const SSTimeAlignConfig* config, col_id_t cid, int64_t firstTs, int64_t* floor);
// Each direction has one immutable deployed layout. Groups borrow its slots for the lifetime of the config.
int32_t       stTimeAlignTriggerConfigCreate(const SStreamTimeAlignSpec* spec, const SSDataBlock* live,
                                             const SSDataBlock* history, SSTimeAlignConfig** config);
const SArray* stTimeAlignSlots(const SSTimeAlignConfig* config, bool history);
// Fixed durations are inclusive. Calendar rules need a common raw successor for a range of changed candidates.
bool stTimeAlignRuleExpiryAfter(const SSTimeAlignConfig* config, int32_t ruleIndex, int64_t changeEnd,
                                int64_t* stableFrom);
// The caller keeps the immutable config alive until all group states are destroyed.
int32_t stTimeAlignCreateFromConfig(const SSTimeAlignConfig* config, SSTimeAlignState** state);
typedef struct {
  col_id_t srcColId;
  int32_t  inputSlot;
  int32_t  outputSlot;
} SSTimeAlignSlot;

int32_t stTimeAlignCreate(const SStreamTimeAlignSpec* spec, SSTimeAlignState** state);
int32_t stTimeAlignRegisterTable(SSTimeAlignState* state, uint64_t uid, uint64_t generation);
int32_t stTimeAlignInstallSeed(SSTimeAlignState* state, const SSTimeAlignSeed* seed, bool* applied);
int32_t stTimeAlignApply(SSTimeAlignState* state, uint64_t uid, const SArray* slots, int32_t tsSlot, SSDataBlock* block,
                         int32_t firstRow, int32_t numRows);
struct SFilterInfo;
int32_t stTimeAlignApplyFiltered(SSTimeAlignState* state, uint64_t uid, const SArray* slots, int32_t tsSlot,
                                 SSDataBlock* block, struct SFilterInfo* filter, const SArray* sourceTimes,
                                 int32_t timeOffset);
// Repeated realtime sliding passes read the same raw slice from its initial candidate.
int32_t stTimeAlignSavePassStart(SSTimeAlignState* state, uint64_t uid);
void    stTimeAlignFinishPass(SSTimeAlignState* state, bool replay);
// A completed table owns only raw values and their physical column bindings.
SArray* stTimeAlignTakeCandidates(SSTimeAlignState* state, uint64_t uid);
void    stTimeAlignDestroyCandidates(SArray* candidates);
int32_t stTimeAlignMergeCandidates(SSTimeAlignSeed* seed, const SArray* candidates);
int32_t stTimeAlignCollectWalProbes(const SArray* candidates, int64_t reader, int64_t uid, STimeWindow range,
                                    int64_t version, SArray** probes);
int32_t stTimeAlignCollectLiveWalProbes(SSTimeAlignState* state, uint64_t virtualUid, int64_t reader, int64_t uid,
                                        STimeWindow range, int64_t version, SArray** probes);
void    stTimeAlignDiscardWalWrites(SArray* candidates, int64_t reader, int64_t uid, STimeWindow range, int64_t version,
                                    const SArray* probes);
void    stTimeAlignDiscardCandidates(SArray* candidates, int64_t reader, int64_t uid, STimeWindow window);
typedef struct {
  int64_t  session, gid;
  uint64_t uid;
} STimeAlignGroupKey;

typedef struct {
  int64_t  reader, uid;
  col_id_t physicalId;
} STimeAlignPhysicalBinding;
void    stTimeAlignRetainColumnBindings(SArray* candidates, SSHashObj* bindings);
void    stTimeAlignForgetTable(SSTimeAlignState* state, uint64_t uid);
void    stTimeAlignDestroy(SSTimeAlignState** state);

#ifdef __cplusplus
}
#endif
#endif

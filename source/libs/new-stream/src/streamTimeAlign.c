/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "streamTimeAlign.h"
#include "filter.h"
#include "tdatablock.h"
#include "tsimplehash.h"
#include "ttime.h"

typedef struct {
  int64_t  reader, uid;
  col_id_t virtualId, physicalId;
  int32_t  sourceBytes;
} SAlignBinding;

typedef struct {
  SAlignBinding  binding;
  SDataType      type;
  SFillPrevValue prev;
  int64_t        physicalTs;
  bool           hasPhysicalTs;
} SSeedColumn;

typedef struct {
  SSeedColumn* column;
  col_id_t     physicalId;
  int32_t      slot;
} SSeedMapping;

typedef struct {
  SArray* mappings;
  int32_t tsSlot;
  bool    exhausted;
} SSeedSource;

struct SSTimeAlignSeed {
  uint64_t virtualUid, generation;
  int64_t  beforeTs;
  SSHashObj* sources;
  int32_t    pendingSources;
  SArray*    columns;
  SSHashObj* columnById;
};

static SSeedColumn* seedColumn(const SSTimeAlignSeed* seed, col_id_t id) {
  const int32_t* slot = tSimpleHashGet(seed->columnById, &id, sizeof(id));
  return slot ? taosArrayGet(seed->columns, *slot) : NULL;
}

const SFillPrevValue* stTimeAlignSeedValue(const SSTimeAlignSeed* seed, col_id_t virtualId) {
  SSeedColumn* col = seed ? seedColumn(seed, virtualId) : NULL;
  return col ? &col->prev : NULL;
}

static void destroySeedColumn(void* item) { fillPrevValueReset(&((SSeedColumn*)item)->prev); }

static void destroySeedSource(void* item) { taosArrayDestroy(((SSeedSource*)item)->mappings); }

void stTimeAlignSeedDestroy(SSTimeAlignSeed** seed) {
  if (!seed || !*seed) return;
  tSimpleHashCleanup((*seed)->sources);
  taosArrayDestroyEx((*seed)->columns, destroySeedColumn);
  tSimpleHashCleanup((*seed)->columnById);
  taosMemoryFreeClear(*seed);
}

int32_t stTimeAlignSeedCreate(const SStreamTimeAlignSpec* spec, uint64_t virtualUid, uint64_t generation,
                              int64_t beforeTs, const SArray* bounds, SSTimeAlignSeed** result) {
  if (!spec || !result || !bounds) return TSDB_CODE_INVALID_PARA;
  *result = NULL;
  SSTimeAlignSeed* seed = taosMemoryCalloc(1, sizeof(*seed));
  if (!seed) return terrno;
  int32_t code = 0, lino = 0;
  seed->virtualUid = virtualUid;
  seed->generation = generation;
  seed->beforeTs = beforeTs;
  seed->sources = tSimpleHashInit(taosArrayGetSize(bounds), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  TSDB_CHECK_NULL(seed->sources, code, lino, _exit, terrno);
  tSimpleHashSetFreeFp(seed->sources, destroySeedSource);
  seed->columns = taosArrayInit(taosArrayGetSize(spec->pColumns), sizeof(SSeedColumn));
  TSDB_CHECK_NULL(seed->columns, code, lino, _exit, terrno);
  seed->columnById =
      tSimpleHashInit(taosArrayGetSize(spec->pColumns), taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
  TSDB_CHECK_NULL(seed->columnById, code, lino, _exit, terrno);
  for (size_t i = 0; i < taosArrayGetSize(spec->pColumns); ++i) {
    const SStreamTimeAlignColumn* def = taosArrayGet(spec->pColumns, i);
    if (seedColumn(seed, def->srcColId)) continue;
    SSeedColumn col = {.type = def->type};
    int32_t     slot = taosArrayGetSize(seed->columns);
    TSDB_CHECK_NULL(taosArrayPush(seed->columns, &col), code, lino, _exit, terrno);
    TAOS_CHECK_EXIT(tSimpleHashPut(seed->columnById, &def->srcColId, sizeof(def->srcColId), &slot, sizeof(slot)));
  }
  for (size_t i = 0; i < taosArrayGetSize(bounds); ++i) {
    const SSTimeAlignSourceBound* bound = taosArrayGet(bounds, i);
    if (bound->maxVer < 0 || !bound->pColRefs) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PARA);
    SSeedSource* added = tSimpleHashGet(seed->sources, &bound->uid, sizeof(bound->uid));
    if (!added) {
      SSeedSource source = {.tsSlot = -1, .exhausted = true};
      TAOS_CHECK_EXIT(tSimpleHashPut(seed->sources, &bound->uid, sizeof(bound->uid), &source, sizeof(source)));
      added = tSimpleHashGet(seed->sources, &bound->uid, sizeof(bound->uid));
      added->mappings = taosArrayInit(taosArrayGetSize(bound->pColRefs), sizeof(SSeedMapping));
      TSDB_CHECK_NULL(added->mappings, code, lino, _exit, terrno);
    }
    if (added->exhausted && !bound->emptyRange && beforeTs != INT64_MIN) {
      added->exhausted = false;
      ++seed->pendingSources;
    }
    for (size_t j = 0; j < taosArrayGetSize(bound->pColRefs); ++j) {
      const SSTimeAlignColumnRef* ref = taosArrayGet(bound->pColRefs, j);
      SSeedColumn*                col = seedColumn(seed, ref->virtualColId);
      if (!col) continue;
      int32_t bytes =
          IS_VAR_DATA_TYPE(col->type.type) && ref->sourceBytes > 0 ? TMAX(col->type.bytes, ref->sourceBytes) : 0;
      if (bytes) col->type.bytes = bytes;
      col->binding = (SAlignBinding){bound->readerTaskId, bound->uid, ref->virtualColId, ref->sourceColId, bytes};
      // The columns array is fully built before any mapping borrows a column.
      SSeedMapping mapping = {.column = col, .physicalId = ref->sourceColId, .slot = -1};
      TSDB_CHECK_NULL(taosArrayPush(added->mappings, &mapping), code, lino, _exit, terrno);
    }
  }
  *result = seed;
  return 0;
_exit:
  stTimeAlignSeedDestroy(&seed);
  return code;
}

static int32_t seedBlockColumn(const SSDataBlock* block, col_id_t id, int32_t* slot, SSHashObj** layout,
                               const SColumnInfoData** out) {
  int32_t                count = taosArrayGetSize(block->pDataBlock);
  const SColumnInfoData* col = *slot >= 0 && *slot < count ? taosArrayGet(block->pDataBlock, *slot) : NULL;
  if (!col || col->info.colId != id) {
    if (!*layout) {
      *layout = tSimpleHashInit(count, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
      if (!*layout) return terrno;
      for (int32_t i = 0; i < count; ++i) {
        col = taosArrayGet(block->pDataBlock, i);
        if (!tSimpleHashGet(*layout, &col->info.colId, sizeof(col->info.colId)))
          TAOS_CHECK_RETURN(tSimpleHashPut(*layout, &col->info.colId, sizeof(col->info.colId), &i, sizeof(i)));
      }
    }
    const int32_t* found = tSimpleHashGet(*layout, &id, sizeof(id));
    if (!found) return TSDB_CODE_INVALID_MSG;
    *slot = *found;
    col = taosArrayGet(block->pDataBlock, *slot);
  }
  *out = col;
  return 0;
}

static int32_t consumeSeedBlock(SSTimeAlignSeed* seed, SSeedSource* source, const SSDataBlock* block) {
  SSHashObj*             layout = NULL;
  const SColumnInfoData* ts = NULL;
  int32_t                code = 0, lino = 0;
  bool pending = false;
  const SColumnInfoData* native = taosArrayGetLast(block->pDataBlock);
  if (native && native->info.colId == STREAM_WAL_PHYSICAL_TS_COL_ID &&
      (native->info.type != TSDB_DATA_TYPE_TIMESTAMP || native->info.bytes != sizeof(int64_t)))
    return TSDB_CODE_INVALID_MSG;
  TAOS_CHECK_EXIT(seedBlockColumn(block, PRIMARYKEY_TIMESTAMP_COL_ID, &source->tsSlot, &layout, &ts));
  if (ts->info.type != TSDB_DATA_TYPE_TIMESTAMP) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  for (size_t i = 0; i < taosArrayGetSize(source->mappings); ++i) {
    SSeedMapping* mapping = taosArrayGet(source->mappings, i);
    SSeedColumn*  col = mapping->column;
    if (col->prev.nonnull) continue;
    const SColumnInfoData* data = NULL;
    TAOS_CHECK_EXIT(seedBlockColumn(block, mapping->physicalId, &mapping->slot, &layout, &data));
    if (data->info.type != col->type.type) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    for (int32_t row = 0; row < block->info.rows; ++row) {
      if (colDataIsNull_s(ts, row)) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
      int64_t time;
      memcpy(&time, colDataGetData(ts, row), sizeof(time));
      if (time >= seed->beforeTs || colDataIsNull_s(data, row)) continue;
      TAOS_CHECK_EXIT(fillPrevValueSet(&col->prev, &col->type, colDataGetData(data, row), time));
      col->hasPhysicalTs = native && native->info.colId == STREAM_WAL_PHYSICAL_TS_COL_ID;
      if (col->hasPhysicalTs) {
        if (colDataIsNull_s(native, row)) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
        memcpy(&col->physicalTs, colDataGetData(native, row), sizeof(col->physicalTs));
      }
      break;
    }
    if (!col->prev.nonnull) pending = true;
  }
  source->exhausted = !pending;
_exit:
  tSimpleHashCleanup(layout);
  return code;
}

bool stTimeAlignSeedNeedsSource(const SSTimeAlignSeed* seed, uint64_t uid) {
  const SSeedSource* source = seed ? tSimpleHashGet(seed->sources, &uid, sizeof(uid)) : NULL;
  return source && !source->exhausted;
}

int32_t stTimeAlignSeedConsume(SSTimeAlignSeed* seed, uint64_t sourceUid, const SSDataBlock* block, bool done,
                               bool* ready) {
  if (!ready) return TSDB_CODE_INVALID_PARA;
  *ready = false;
  if (!seed || (!block && !done)) return TSDB_CODE_INVALID_PARA;
  SSeedSource* source = tSimpleHashGet(seed->sources, &sourceUid, sizeof(sourceUid));
  if (!source) return TSDB_CODE_INVALID_PARA;
  if (!source->exhausted) {
    if (block) TAOS_CHECK_RETURN(consumeSeedBlock(seed, source, block));
    source->exhausted |= done;
    if (source->exhausted) --seed->pendingSources;
  }
  *ready = seed->pendingSources == 0;
  return 0;
}

typedef struct {
  col_id_t  id;
  SDataType type;
  int32_t   firstRule;
  uint64_t  seedTicks;  // UINT64_MAX includes every possible predecessor.
} SAlignSource;

typedef struct {
  int32_t        source;
  int32_t        calendarSlot;
  SFillPrevRange range;
} SAlignRule;

typedef struct {
  bool           ready;
  SAlignBinding  binding;
  SFillPrevValue value;
  int64_t        physicalTs;
  bool           hasPhysicalTs;
} SAlignCandidate;

typedef struct {
  SAlignCandidate candidate;
  bool            changed;
} SAlignChange;

typedef struct {
  int64_t sourceTs;
  int64_t end;
  bool    valid;
} SAlignCalendarEnd;

typedef struct {
  SAlignCalendarEnd* calendarEnds;
  uint64_t generation;
  SArray*  candidates;
  SArray*  passStart;
} SAlignTable;

struct SSTimeAlignConfig {
  int32_t    numCalendarRules;
  timezone_t tz;
  SArray*    sources;
  SArray*    rules;
  SSHashObj* sourceById;
  SArray*    slots[2];  // independent live and history layouts
};

struct SSTimeAlignState {
  const SSTimeAlignConfig* config;
  SSTimeAlignConfig*       ownedConfig;
  SSHashObj*               tables;
};

static void disposeCandidate(void* p) { fillPrevValueReset(&((SAlignCandidate*)p)->value); }
static void disposeChange(void* p) { disposeCandidate(&((SAlignChange*)p)->candidate); }
static void disposeAlignTable(void* p) {
  SAlignTable* table = p;
  taosMemoryFree(table->calendarEnds);
  taosArrayDestroyEx(table->candidates, disposeCandidate);
  taosArrayDestroyEx(table->passStart, disposeChange);
}

void stTimeAlignDestroy(SSTimeAlignState** state) {
  if (!state || !*state) return;
  tSimpleHashCleanup((*state)->tables);
  stTimeAlignConfigDestroy(&(*state)->ownedConfig);
  taosMemoryFreeClear(*state);
}

void stTimeAlignConfigDestroy(SSTimeAlignConfig** config) {
  if (!config || !*config) return;
  taosArrayDestroy((*config)->rules);
  if ((*config)->tz) tzfree((*config)->tz);
  taosArrayDestroy((*config)->sources);
  taosArrayDestroy((*config)->slots[0]);
  taosArrayDestroy((*config)->slots[1]);
  tSimpleHashCleanup((*config)->sourceById);
  taosMemoryFreeClear(*config);
}

int32_t stTimeAlignConfigCreate(const SStreamTimeAlignSpec* spec, SSTimeAlignConfig** out) {
  if (!out) return TSDB_CODE_INVALID_PARA;
  *out = NULL;
  if (!spec || !taosArrayGetSize(spec->pColumns)) return TSDB_CODE_INVALID_PARA;
  int32_t            code = 0;
  int32_t            n = taosArrayGetSize(spec->pColumns);
  SSTimeAlignConfig* config = taosMemoryCalloc(1, sizeof(*config));
  if (!config) return terrno;
  config->sources = taosArrayInit(n, sizeof(SAlignSource));
  config->rules = taosArrayInit(n, sizeof(SAlignRule));
  config->sourceById = tSimpleHashInit(n, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT));
  if (!config->sources || !config->rules || !config->sourceById) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }
  for (int32_t i = 0; i < n; ++i) {
    const SStreamTimeAlignColumn* def = taosArrayGet(spec->pColumns, i);
    const int32_t*                index = tSimpleHashGet(config->sourceById, &def->srcColId, sizeof(def->srcColId));
    int32_t                       src = index ? *index : taosArrayGetSize(config->sources);
    if (!index) {
      code = tSimpleHashPut(config->sourceById, &def->srcColId, sizeof(def->srcColId), &src, sizeof(src));
      if (code) goto end;
      SAlignSource source = {.id = def->srcColId, .type = def->type, .firstRule = i};
      if (!taosArrayPush(config->sources, &source)) {
        code = terrno;
        goto end;
      }
    }
    SAlignRule rule = {.source = src, .calendarSlot = -1};
    code = fillPrevRangeInitShared(&rule.range, def->hasSpan, def->amount, def->unit, spec->precision, spec->timezone,
                                   &config->tz);
    if (code) goto end;
    if (rule.range.hasSpan && IS_CALENDAR_TIME_DURATION(rule.range.unit))
      rule.calendarSlot = config->numCalendarRules++;
    SAlignSource* source = taosArrayGet(config->sources, src);
    uint64_t      ticks = rule.range.hasSpan ? rule.range.ticks : UINT64_MAX;
    source->seedTicks = TMAX(source->seedTicks, ticks);
    if (!taosArrayPush(config->rules, &rule)) {
      code = terrno;
      goto end;
    }
  }
end:
  if (code) stTimeAlignConfigDestroy(&config);
  *out = config;
  return code;
}

bool stTimeAlignSeedFloor(const SSTimeAlignConfig* config, col_id_t cid, int64_t firstTs, int64_t* floor) {
  const int32_t* index = config ? tSimpleHashGet(config->sourceById, &cid, sizeof(cid)) : NULL;
  if (!index) return false;
  const SAlignSource* source = taosArrayGet(config->sources, *index);
  uint64_t            available = (uint64_t)firstTs - (uint64_t)INT64_MIN;
  *floor = source->seedTicks >= available ? INT64_MIN : (int64_t)((uint64_t)firstTs - source->seedTicks);
  return true;
}

static int32_t buildAlignSlots(const SSTimeAlignConfig* config, const SSDataBlock* layout, SArray** out) {
  int32_t code = 0, lino = 0;
  int32_t rules = taosArrayGetSize(config->rules);
  int32_t raw = blockDataGetNumOfCols(layout);
  SArray* slots = taosArrayInit(rules, sizeof(SSTimeAlignSlot));
  if (!slots) return terrno;
  int32_t* input = taosMemoryCalloc(taosArrayGetSize(config->sources), sizeof(int32_t));
  TSDB_CHECK_NULL(input, code, lino, _exit, terrno);
  for (int32_t i = 0; i < raw; ++i) {
    const SColumnInfoData* col = taosArrayGet(layout->pDataBlock, i);
    const int32_t*         source = tSimpleHashGet(config->sourceById, &col->info.colId, sizeof(col->info.colId));
    if (source && !input[*source]) input[*source] = i + 1;
  }
  for (int32_t i = 0; i < rules; ++i) {
    const SAlignRule* rule = taosArrayGet(config->rules, i);
    if (!input[rule->source]) TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
    const SAlignSource* source = taosArrayGet(config->sources, rule->source);
    SSTimeAlignSlot     slot = {.srcColId = source->id, .inputSlot = input[rule->source] - 1, .outputSlot = raw + i};
    TSDB_CHECK_NULL(taosArrayPush(slots, &slot), code, lino, _exit, terrno);
  }
  *out = slots;
_exit:
  taosMemoryFree(input);
  if (code) taosArrayDestroy(slots);
  return code;
}

int32_t stTimeAlignTriggerConfigCreate(const SStreamTimeAlignSpec* spec, const SSDataBlock* live,
                                       const SSDataBlock* history, SSTimeAlignConfig** config) {
  TAOS_CHECK_RETURN(stTimeAlignConfigCreate(spec, config));
  int32_t code = live ? buildAlignSlots(*config, live, &(*config)->slots[0]) : 0;
  if (!code && history) code = buildAlignSlots(*config, history, &(*config)->slots[1]);
  if (code) stTimeAlignConfigDestroy(config);
  return code;
}

const SArray* stTimeAlignSlots(const SSTimeAlignConfig* config, bool history) {
  return config ? config->slots[history] : NULL;
}

bool stTimeAlignRuleExpiryAfter(const SSTimeAlignConfig* config, int32_t ruleIndex, int64_t changeEnd,
                                int64_t* stableFrom) {
  const SAlignRule* rule = config ? taosArrayGet(config->rules, ruleIndex) : NULL;
  if (!rule || !stableFrom || !rule->range.hasSpan || IS_CALENDAR_TIME_DURATION(rule->range.unit) ||
      rule->range.ticks >= (uint64_t)INT64_MAX - (uint64_t)changeEnd)
    return false;
  *stableFrom = (int64_t)((uint64_t)changeEnd + rule->range.ticks + 1);
  return true;
}

int32_t stTimeAlignCreateFromConfig(const SSTimeAlignConfig* config, SSTimeAlignState** out) {
  if (!out || !config) return TSDB_CODE_INVALID_PARA;
  *out = NULL;
  SSTimeAlignState* state = taosMemoryCalloc(1, sizeof(*state));
  if (!state) return terrno;
  state->config = config;
  state->tables = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT));
  if (!state->tables) {
    taosMemoryFree(state);
    return terrno;
  }
  tSimpleHashSetFreeFp(state->tables, disposeAlignTable);
  *out = state;
  return 0;
}

int32_t stTimeAlignCreate(const SStreamTimeAlignSpec* spec, SSTimeAlignState** out) {
  if (!out) return TSDB_CODE_INVALID_PARA;
  *out = NULL;
  SSTimeAlignConfig* config = NULL;
  TAOS_CHECK_RETURN(stTimeAlignConfigCreate(spec, &config));
  int32_t code = stTimeAlignCreateFromConfig(config, out);
  if (code)
    stTimeAlignConfigDestroy(&config);
  else
    (*out)->ownedConfig = config;
  return code;
}

int32_t stTimeAlignRegisterTable(SSTimeAlignState* state, uint64_t uid, uint64_t generation) {
  if (!state) return TSDB_CODE_INVALID_PARA;
  SAlignTable* existing = tSimpleHashGet(state->tables, &uid, sizeof(uid));
  if (existing) return existing->generation == generation ? 0 : TSDB_CODE_INVALID_PARA;
  int32_t     count = taosArrayGetSize(state->config->sources);
  SAlignTable table = {.generation = generation, .candidates = taosArrayInit_s(sizeof(SAlignCandidate), count)};
  if (!table.candidates) return terrno;
  if (state->config->numCalendarRules) {
    table.calendarEnds = taosMemoryCalloc(state->config->numCalendarRules, sizeof(*table.calendarEnds));
    if (!table.calendarEnds) {
      disposeAlignTable(&table);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  int32_t code = tSimpleHashPut(state->tables, &uid, sizeof(uid), &table, sizeof(table));
  if (code) disposeAlignTable(&table);
  return code;
}

void stTimeAlignForgetTable(SSTimeAlignState* state, uint64_t uid) {
  if (state) TAOS_UNUSED(tSimpleHashRemove(state->tables, &uid, sizeof(uid)));
}

static int32_t copyAlignValue(SFillPrevValue* dst, const SFillPrevValue* src) {
  if (!src->nonnull) return 0;
  const void* data = IS_VAR_DATA_TYPE(src->type.type) ? (const void*)src->data : (const void*)src->fixed;
  return fillPrevValueSet(dst, &src->type, data, src->sourceTs);
}

// Exchange only changed candidates. The first commit in a sliding pass moves the previous owner
// into passStart, so both the batch and repeated passes can roll back without copying unchanged values.
static void commitAlignChanges(SAlignTable* table, SArray* changes) {
  for (int32_t i = 0; i < taosArrayGetSize(changes); ++i) {
    SAlignChange* update = taosArrayGet(changes, i);
    if (!update->changed) continue;
    SAlignCandidate* current = taosArrayGet(table->candidates, i);
    SAlignChange*    original = table->passStart ? taosArrayGet(table->passStart, i) : NULL;
    if (original && !original->changed) {
      original->candidate = *current;
      original->changed = true;
    } else {
      disposeCandidate(current);
    }
    *current = update->candidate;
    update->candidate.value.data = NULL;
  }
}

int32_t stTimeAlignSavePassStart(SSTimeAlignState* state, uint64_t uid) {
  SAlignTable* table = tSimpleHashGet(state->tables, &uid, sizeof(uid));
  if (!table) return TSDB_CODE_STREAM_NO_CONTEXT;
  if (!table->passStart) table->passStart = taosArrayInit_s(sizeof(SAlignChange), taosArrayGetSize(table->candidates));
  return table->passStart ? 0 : terrno;
}

void stTimeAlignFinishPass(SSTimeAlignState* state, bool replay) {
  int32_t      iter = 0;
  SAlignTable* table = NULL;
  while ((table = tSimpleHashIterate(state->tables, table, &iter))) {
    SArray* original = table->passStart;
    if (!original) continue;
    table->passStart = NULL;
    if (replay) commitAlignChanges(table, original);
    taosArrayDestroyEx(original, disposeChange);
  }
}

int32_t stTimeAlignInstallSeed(SSTimeAlignState* state, const SSTimeAlignSeed* seed, bool* applied) {
  if (!state || !seed || !applied) return TSDB_CODE_INVALID_PARA;
  *applied = false;
  SAlignTable* table = tSimpleHashGet(state->tables, &seed->virtualUid, sizeof(seed->virtualUid));
  if (!table || table->generation != seed->generation) return 0;
  if (seed->pendingSources) return TSDB_CODE_STREAM_NO_CONTEXT;
  SArray* changes = taosArrayInit_s(sizeof(SAlignChange), taosArrayGetSize(table->candidates));
  if (!changes) return terrno;
  int32_t code = 0;
  for (int32_t i = 0; i < taosArrayGetSize(state->config->sources); ++i) {
    const SAlignCandidate* current = taosArrayGet(table->candidates, i);
    if (current->ready) continue;
    const SAlignSource* src = taosArrayGet(state->config->sources, i);
    const SSeedColumn*  col = seedColumn(seed, src->id);
    SAlignChange*       update = taosArrayGet(changes, i);
    code = col ? copyAlignValue(&update->candidate.value, &col->prev) : TSDB_CODE_INVALID_PARA;
    if (code) break;
    update->candidate.binding = col->binding;
    update->candidate.hasPhysicalTs = col->hasPhysicalTs;
    update->candidate.physicalTs = col->physicalTs;
    update->candidate.ready = true;
    update->changed = true;
  }
  if (!code) commitAlignChanges(table, changes);
  taosArrayDestroyEx(changes, disposeChange);
  if (code) return code;
  *applied = true;
  return 0;
}

static int32_t alignedRangeContains(const SAlignRule* rule, SAlignTable* table, int64_t sourceTs, int64_t time,
                                    bool* valid) {
  if (rule->calendarSlot < 0 || sourceTs >= time) return fillPrevRangeContains(&rule->range, sourceTs, time, valid);
  SAlignCalendarEnd* cached = &table->calendarEnds[rule->calendarSlot];
  if (!cached->valid || cached->sourceTs != sourceTs) {
    int64_t end;
    TAOS_CHECK_RETURN(fillPrevCalendarEnd(&rule->range, sourceTs, &end));
    *cached = (SAlignCalendarEnd){.sourceTs = sourceTs, .end = end, .valid = true};
  }
  *valid = time <= cached->end;
  return TSDB_CODE_SUCCESS;
}

static int32_t applyAlignedRows(SSTimeAlignState* state, SAlignTable* table, SArray* changes, const SArray* slots,
                                SSDataBlock* block, const SColumnInfoData* times, int32_t first, int32_t last,
                                const SArray* sourceTimes, int32_t timeOffset) {
  for (int32_t row = first; row < last; ++row) {
    if (colDataIsNull_s(times, row)) return TSDB_CODE_INVALID_MSG;
    int64_t time;
    memcpy(&time, colDataGetData(times, row), sizeof(time));
    for (int32_t i = 0; i < taosArrayGetSize(state->config->sources); ++i) {
      const SAlignSource*    source = taosArrayGet(state->config->sources, i);
      const SSTimeAlignSlot* slot = taosArrayGet(slots, source->firstRule);
      const SColumnInfoData* col = taosArrayGet(block->pDataBlock, slot->inputSlot);
      const SAlignCandidate* candidate = taosArrayGet(table->candidates, i);
      if (!candidate->ready) return TSDB_CODE_STREAM_NO_CONTEXT;
      if (!colDataIsNull_s(col, row)) {
        SAlignChange* update = taosArrayGet(changes, i);
        SDataType     type = source->type;
        if (candidate->binding.sourceBytes > 0) type.bytes = candidate->binding.sourceBytes;
        TAOS_CHECK_RETURN(fillPrevValueSet(&update->candidate.value, &type, colDataGetData(col, row), time));
        update->candidate.ready = true;
        update->candidate.binding = candidate->binding;
        const SColumnInfoData* native = sourceTimes ? taosArrayGet(sourceTimes, slot->inputSlot) : NULL;
        update->candidate.hasPhysicalTs = native && native->pData && !colDataIsNull_s(native, row + timeOffset);
        if (update->candidate.hasPhysicalTs)
          memcpy(&update->candidate.physicalTs, colDataGetData(native, row + timeOffset), sizeof(int64_t));
        update->changed = true;
      }
    }
    for (int32_t i = 0; i < taosArrayGetSize(state->config->rules); ++i) {
      const SAlignRule*      rule = taosArrayGet(state->config->rules, i);
      const SSTimeAlignSlot* slot = taosArrayGet(slots, i);
      SColumnInfoData*       col = taosArrayGet(block->pDataBlock, slot->outputSlot);
      const SAlignChange*    update = taosArrayGet(changes, rule->source);
      const SAlignCandidate* candidate =
          update->changed ? &update->candidate : taosArrayGet(table->candidates, rule->source);
      bool                   valid = candidate->value.nonnull;
      if (valid) TAOS_CHECK_RETURN(alignedRangeContains(rule, table, candidate->value.sourceTs, time, &valid));
      TAOS_CHECK_RETURN(fillPrevValueWrite(&candidate->value, valid, col, row));
    }
  }
  return 0;
}

static int32_t filterAlignedRows(SFilterInfo* filter, SSDataBlock* block) {
  if (!filter) return 0;
  SFilterColumnParam param = {.numOfCols = blockDataGetNumOfCols(block), .pDataBlock = block->pDataBlock};
  TAOS_CHECK_RETURN(filterSetDataFromSlotId(filter, &param));
  SColumnInfoData* mask = NULL;
  int32_t          status = 0;
  int32_t          code = filterExecute(filter, block, &mask, NULL, param.numOfCols, &status);
  if (!code && status == FILTER_RESULT_NONE_QUALIFIED) block->info.rows = 0;
  if (!code && status == FILTER_RESULT_PARTIAL_QUALIFIED)
    code = trimDataBlock(block, block->info.rows, (const bool*)mask->pData);
  if (mask) {
    colDataDestroy(mask);
    taosMemoryFree(mask);
  }
  return code;
}

static int32_t applyTimeAlignBatch(SSTimeAlignState* state, uint64_t uid, const SArray* slots, int32_t tsSlot,
                                   SSDataBlock* block, int32_t firstRow, int32_t numRows, SFilterInfo* filter,
                                   const SArray* sourceTimes, int32_t timeOffset) {
  if (!state || !block || taosArrayGetSize(slots) != taosArrayGetSize(state->config->rules) || firstRow < 0 ||
      numRows < 0 || firstRow > block->info.rows || numRows > block->info.rows - firstRow)
    return TSDB_CODE_INVALID_PARA;
  const SColumnInfoData* times = taosArrayGet(block->pDataBlock, tsSlot);
  if (!times || times->info.type != TSDB_DATA_TYPE_TIMESTAMP) return TSDB_CODE_INVALID_MSG;
  SAlignTable* table = tSimpleHashGet(state->tables, &uid, sizeof(uid));
  if (!table) return TSDB_CODE_STREAM_NO_CONTEXT;
  for (int32_t i = 0; i < taosArrayGetSize(slots); ++i) {
    const SSTimeAlignSlot* slot = taosArrayGet(slots, i);
    const SAlignRule*      rule = taosArrayGet(state->config->rules, i);
    const SAlignSource*    source = taosArrayGet(state->config->sources, rule->source);
    SColumnInfoData*       raw = taosArrayGet(block->pDataBlock, slot->inputSlot);
    SColumnInfoData*       aligned = taosArrayGet(block->pDataBlock, slot->outputSlot);
    if (slot->srcColId != source->id || !raw || !aligned || raw == aligned || raw->info.type != source->type.type ||
        aligned->info.type != source->type.type)
      return TSDB_CODE_INVALID_MSG;
    const SAlignCandidate* candidate = taosArrayGet(table->candidates, rule->source);
    if (IS_VAR_DATA_TYPE(source->type.type) && candidate->binding.sourceBytes > 0) {
      if (raw->info.bytes != candidate->binding.sourceBytes || aligned->info.bytes != candidate->binding.sourceBytes) {
        raw->info.bytes = aligned->info.bytes = candidate->binding.sourceBytes;
        block->info.rowSize = 0;
      }
    } else if (aligned->info.bytes < source->type.bytes) {
      return TSDB_CODE_INVALID_MSG;
    }
  }
  if (numRows == 0) return 0;
  SArray* changes = taosArrayInit_s(sizeof(SAlignChange), taosArrayGetSize(table->candidates));
  if (!changes) return terrno;
  int32_t code = applyAlignedRows(state, table, changes, slots, block, times, firstRow, firstRow + numRows, sourceTimes,
                                  timeOffset);
  if (!code && filter) code = filterAlignedRows(filter, block);
  if (!code) commitAlignChanges(table, changes);
  taosArrayDestroyEx(changes, disposeChange);
  return code;
}

int32_t stTimeAlignApply(SSTimeAlignState* state, uint64_t uid, const SArray* slots, int32_t tsSlot, SSDataBlock* block,
                         int32_t firstRow, int32_t numRows) {
  return applyTimeAlignBatch(state, uid, slots, tsSlot, block, firstRow, numRows, NULL, NULL, 0);
}

int32_t stTimeAlignApplyFiltered(SSTimeAlignState* state, uint64_t uid, const SArray* slots, int32_t tsSlot,
                                 SSDataBlock* block, SFilterInfo* filter, const SArray* sourceTimes,
                                 int32_t timeOffset) {
  if (!block) return TSDB_CODE_INVALID_PARA;
  return applyTimeAlignBatch(state, uid, slots, tsSlot, block, 0, block->info.rows, filter, sourceTimes, timeOffset);
}

SArray* stTimeAlignTakeCandidates(SSTimeAlignState* state, uint64_t uid) {
  SAlignTable* table = state ? tSimpleHashGet(state->tables, &uid, sizeof(uid)) : NULL;
  if (!table || !table->candidates) return NULL;
  // An unfinished seed cannot replace the last completed input.
  SAlignCandidate* first = taosArrayGet(table->candidates, 0);
  if (!first || !first->ready) return NULL;
  SArray* candidates = table->candidates;
  table->candidates = NULL;
  stTimeAlignForgetTable(state, uid);
  return candidates;
}

void stTimeAlignDestroyCandidates(SArray* candidates) { taosArrayDestroyEx(candidates, disposeCandidate); }

int32_t stTimeAlignMergeCandidates(SSTimeAlignSeed* seed, const SArray* candidates) {
  for (size_t i = 0; i < taosArrayGetSize(candidates); ++i) {
    const SAlignCandidate* prev = taosArrayGet(candidates, i);
    if (!prev->value.nonnull || !prev->binding.uid || prev->value.sourceTs >= seed->beforeTs) continue;
    SSeedColumn* col = seedColumn(seed, prev->binding.virtualId);
    if (!col || col->binding.reader != prev->binding.reader || col->binding.uid != prev->binding.uid ||
        col->binding.physicalId != prev->binding.physicalId)
      continue;
    if (!col->prev.nonnull || prev->value.sourceTs > col->prev.sourceTs ||
        (prev->value.sourceTs == col->prev.sourceTs && prev->hasPhysicalTs && col->hasPhysicalTs &&
         prev->physicalTs > col->physicalTs)) {
      TAOS_CHECK_RETURN(copyAlignValue(&col->prev, &prev->value));
      col->hasPhysicalTs = prev->hasPhysicalTs;
      col->physicalTs = prev->physicalTs;
    }
  }
  return 0;
}

int32_t stTimeAlignCollectWalProbes(const SArray* candidates, int64_t reader, int64_t uid, STimeWindow range,
                                    int64_t version, SArray** probes) {
  for (size_t i = 0; i < taosArrayGetSize(candidates); ++i) {
    const SAlignCandidate* prev = taosArrayGet(candidates, i);
    if (!prev->value.nonnull || prev->binding.reader != reader || prev->binding.uid != uid ||
        prev->value.sourceTs < range.skey || prev->value.sourceTs > range.ekey)
      continue;
    if (!*probes && !(*probes = taosArrayInit(4, sizeof(STimeAlignWalProbe)))) return terrno;
    STimeAlignWalProbe probe = {.version = version,
                                .uid = uid,
                                .ts = prev->hasPhysicalTs ? prev->physicalTs : prev->value.sourceTs,
                                .cid = prev->binding.physicalId,
                                .physical = prev->hasPhysicalTs};
    if (!taosArrayPush(*probes, &probe)) return terrno;
  }
  return 0;
}

int32_t stTimeAlignCollectLiveWalProbes(SSTimeAlignState* state, uint64_t virtualUid, int64_t reader, int64_t uid,
                                        STimeWindow range, int64_t version, SArray** probes) {
  SAlignTable* table = state ? tSimpleHashGet(state->tables, &virtualUid, sizeof(virtualUid)) : NULL;
  return table ? stTimeAlignCollectWalProbes(table->candidates, reader, uid, range, version, probes) : 0;
}

void stTimeAlignDiscardWalWrites(SArray* candidates, int64_t reader, int64_t uid, STimeWindow range, int64_t version,
                                 const SArray* probes) {
  if (!taosArrayGetSize(probes)) return;
  for (size_t i = 0; i < taosArrayGetSize(candidates); ++i) {
    SAlignCandidate* prev = taosArrayGet(candidates, i);
    if (prev->binding.reader != reader || prev->binding.uid != uid || !prev->value.nonnull ||
        prev->value.sourceTs < range.skey || prev->value.sourceTs > range.ekey)
      continue;
    STimeAlignWalProbe        key = {.version = version,
                                     .uid = uid,
                                     .ts = prev->hasPhysicalTs ? prev->physicalTs : prev->value.sourceTs,
                                     .cid = prev->binding.physicalId,
                                     .physical = prev->hasPhysicalTs};
    const STimeAlignWalProbe* probe = taosArraySearch(probes, &key, tCompareTimeAlignWalProbe, TD_EQ);
    if (probe && (probe->written || probe->unavailable)) fillPrevValueReset(&prev->value);
  }
}

void stTimeAlignDiscardCandidates(SArray* candidates, int64_t reader, int64_t uid, STimeWindow window) {
  for (size_t i = 0; i < taosArrayGetSize(candidates); ++i) {
    SAlignCandidate* prev = taosArrayGet(candidates, i);
    if (prev->binding.reader != reader || (uid && prev->binding.uid != uid) || !prev->value.nonnull ||
        prev->value.sourceTs < window.skey || prev->value.sourceTs > window.ekey)
      continue;
    fillPrevValueReset(&prev->value);
  }
}

void stTimeAlignRetainColumnBindings(SArray* candidates, SSHashObj* bindings) {
  for (size_t i = 0; i < taosArrayGetSize(candidates); ++i) {
    SAlignCandidate* prev = taosArrayGet(candidates, i);
    if (!prev->value.nonnull || !prev->binding.uid) continue;
    const STimeAlignPhysicalBinding* binding =
        tSimpleHashGet(bindings, &prev->binding.virtualId, sizeof(prev->binding.virtualId));
    if (!binding || binding->reader != prev->binding.reader || binding->uid != prev->binding.uid ||
        binding->physicalId != prev->binding.physicalId)
      fillPrevValueReset(&prev->value);
  }
}

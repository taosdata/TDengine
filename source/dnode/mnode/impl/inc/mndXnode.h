/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef _TD_MND_XNODE_H_
#define _TD_MND_XNODE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  XNODE_SHOW_OUTPUT_PLAINTEXT = 0,
  XNODE_SHOW_OUTPUT_MASKED = 1,
  XNODE_SHOW_OUTPUT_ENCRYPTED = 2,
} EXnodeShowOutputMode;

int32_t mndXnodeResolveShowMode(int32_t protection, int8_t requestMode, int8_t *effectiveMode);
bool    mndXnodeShowFilterMatches(int8_t filterType, int64_t filterValue, int32_t taskId, int32_t jobTaskId);
int32_t mndXnodeMaskDsnForShow(const char *dsn, int32_t dsnLen, char **masked, int32_t *maskedLen);
int32_t mndXnodeProjectJobConfigForShow(SShowObj *pShow, const char *config, int32_t configLen, char **projected,
                                        int32_t *projectedLen);
int32_t mndXnodeFinishShowProtection(SShowObj *pShow);
void    mndXnodeCleanupShowProtection(SShowObj *pShow);

// Six-star write semantics for CREATE/ALTER XNODE TASK/JOB (xnodeDsnProtection=1).
// The numeric values are the C mapping of the stable cross-language six-star error
// registry; they must stay in one-to-one correspondence with the canonical IDs and
// are the only values tests may compare against the fixture error_id strings:
//   1 INPUT_TYPE_INVALID        <-> 40 SIXSTAR_INPUT_TYPE_INVALID
//   2 PERCENT_ENCODING_INVALID  <-> 41 SIXSTAR_PERCENT_ENCODING_INVALID
//   3 FIELD_AMBIGUOUS           <-> 42 SIXSTAR_FIELD_AMBIGUOUS
//   4 CREATE_MASKED             <-> 43 SIXSTAR_CREATE_MASKED
//   5 KEEP_MISSING              <-> 44 SIXSTAR_KEEP_MISSING
//   6 JOB_JSON_INVALID          <-> 45 SIXSTAR_JOB_JSON_INVALID
typedef enum {
  XNODE_SIXSTAR_ERROR_NONE = 0,
  XNODE_SIXSTAR_ERROR_INPUT_TYPE_INVALID = 1,
  XNODE_SIXSTAR_ERROR_PERCENT_ENCODING_INVALID = 2,
  XNODE_SIXSTAR_ERROR_FIELD_AMBIGUOUS = 3,
  XNODE_SIXSTAR_ERROR_CREATE_MASKED = 4,
  XNODE_SIXSTAR_ERROR_KEEP_MISSING = 5,
  XNODE_SIXSTAR_ERROR_JOB_JSON_INVALID = 6,
} EXnodeSixStarError;

typedef enum {
  XNODE_SIXSTAR_ROLE_USERINFO_PASSWORD = 0,
  XNODE_SIXSTAR_ROLE_QUERY_SENSITIVE = 1,
} EXnodeSixStarSlotRole;

// One sensitive field located inside a raw DSN. Wire key/value are spans of the
// scanned DSN (wireKeyOff is XNODE_SIXSTAR_WIRE_KEY_NONE for the userinfo
// password, which has no key); decoded/normalized copies are owned by the slot
// and freed by mndXnodeSixStarFreeSlots().
#define XNODE_SIXSTAR_WIRE_KEY_NONE (-1)
typedef struct {
  int32_t role;
  int32_t wireKeyOff;
  int32_t wireKeyLen;
  int32_t wireValueOff;
  int32_t wireValueLen;
  int32_t duplicateOrdinal;
  char   *decodedKey;
  int32_t decodedKeyLen;
  char   *normalizedKey;
  int32_t normalizedKeyLen;
  char   *decodedValue;
  int32_t decodedValueLen;
} SXnodeSixStarSlot;

// Three-state field: ptr == NULL means the whole field is omitted; ptr != NULL
// with len == 0 is a present but empty field, which is scanned and merged like
// any other value.
typedef struct {
  const char *ptr;
  int32_t     len;
} SXnodeSixStarField;

// isJob selects which candidate/old field pairs are consumed: isJob == true
// merges only candidateConfig/oldConfig, isJob == false merges only
// candidateFrom/oldFrom and candidateTo/oldTo. Fields of the other direction
// are ignored and must be left zeroed by the caller. create selects the write
// stage: CREATE rejects any six-star marker (XNODE_SIXSTAR_ERROR_CREATE_MASKED),
// ALTER treats an exact "******" as KEEP and requires the corresponding
// committed old field (otherwise XNODE_SIXSTAR_ERROR_KEEP_MISSING).
typedef struct {
  bool              create;
  bool              protectionEnabled;
  bool              isJob;
  SXnodeSixStarField oldFrom;
  SXnodeSixStarField oldTo;
  SXnodeSixStarField oldConfig;
  SXnodeSixStarField candidateFrom;
  SXnodeSixStarField candidateTo;
  SXnodeSixStarField candidateConfig;
  int32_t           maxFromLen;
  int32_t           maxToLen;
  int32_t           maxConfigLen;
} SXnodeSixStarMergeInput;

// Accepted merge result. Buffers are NUL-terminated, owned by the caller and
// released with mndXnodeSixStarMergeOutputFree(); a NULL pointer marks an
// omitted field.
typedef struct {
  char   *from;
  int32_t fromLen;
  char   *to;
  int32_t toLen;
  char   *config;
  int32_t configLen;
} SXnodeSixStarMergeOutput;

int32_t mndXnodeSixStarScanDsn(const char *dsn, int32_t dsnLen, SXnodeSixStarSlot **ppSlots, int32_t *pNumSlots,
                               int32_t *pSixStarError);
void    mndXnodeSixStarFreeSlots(SXnodeSixStarSlot *pSlots, int32_t numSlots);
// On failure the merge has already released and zeroed *pOutput, so the caller
// may still pass it to mndXnodeSixStarMergeOutputFree() without risking a
// double free.
int32_t mndXnodeSixStarMerge(const SXnodeSixStarMergeInput *pInput, SXnodeSixStarMergeOutput *pOutput,
                             int32_t *pSixStarError);
void    mndXnodeSixStarMergeOutputFree(SXnodeSixStarMergeOutput *pOutput);

#define TSDB_XNODE_VER_NUMBER   1

int32_t mndInitXnode(SMnode* pMnode);
void    mndCleanupXnode(SMnode* pMnode);
void    mndRetrieveTasks(SMnode* pMnode, SArray* pFc, SArray* pAd);
void    mndXnodeHandleBecomeLeader(SMnode* pMnode);
void    mndXnodeHandleBecomeNotLeader();
int32_t mndSetXnodeBlobColumn(SColumnInfoData* pColInfo, int32_t row, const char* data,
                              int32_t storedLen);
int32_t mndGetXnodeShowStatus(SXnodeObj* pObj, char* status, int32_t statusLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_ANODE_H_*/

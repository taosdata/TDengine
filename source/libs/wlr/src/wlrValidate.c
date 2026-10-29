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

/*
 * WLR required-tag table (DS 6.15.2).
 *
 * This file is the single source of truth for which TLV tags each frame must
 * carry. The recorder validates against it before writing and the parser
 * validates against it after reading, so a schema mistake is caught on the
 * producing side rather than surfacing as an unreplayable file at a customer.
 * DS 6.15.2 forbids a second hand-written copy: adding an API means editing this
 * table and nothing else.
 */

#include "os.h"
#include "wlrFormat.h"

#define WLR_INLINE_SEEN_TAGS 64

/* Fields every event frame carries regardless of API (DS 6.15.2). The object ID
 * itself is per-API, so it lives in the per-group lists below. Keep the shared
 * prefix in one macro so extending the event envelope updates every rule. */
#define WLR_COMMON_EVENT_TAGS                                                                  \
  WLR_TAG_WALL_NS, WLR_TAG_MONO_NS, WLR_TAG_ORIGIN_TID, WLR_TAG_API_TYPE, WLR_TAG_EVENT_PHASE, \
      WLR_TAG_CALL_ID

static const uint16_t kCommonEventTags[] = {WLR_COMMON_EVENT_TAGS};

/* Connect BEGIN: endpoint/user/db/port. Never a password, token or TOTP -- the
 * recorder does not read them at all (DS 6.6, FS 6).
 *
 * ENDPOINT is deliberately NOT required. taos_connect(NULL, ...) is a legal call
 * meaning "resolve from firstEp", so demanding the tag would assert something the
 * public API does not guarantee, and the recorder must not invent a value the
 * caller never supplied. An absent endpoint means "unspecified"; replay resolves
 * it from the target config, which is what --target-host does anyway.
 *
 * USER stays required: it is always non-NULL by the time the hook runs, since
 * taos_connect substitutes the default above. */
static const uint16_t kConnectBeginTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_USER,
};

/*
 * A connection option must name which option it set, so replay can reapply it.
 *
 * CONN_OPT_VALUE is NOT required: passing NULL clears the option, which is a legal
 * call, and the absent tag is how that is represented. Requiring it would force the
 * recorder to invent an empty string, a different fact from "cleared".
 */
/* Conn-opt RETURN carries only the common outcome fields, including RET_CODE. */

static const uint16_t kConnOptBeginTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_CONN_OPT_TYPE, WLR_TAG_CONN_OPT_NAME,
};

/* Query BEGIN needs the SQL text and the connection it runs on. */
static const uint16_t kQueryBeginTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_CONN_ID, WLR_TAG_SQL,
};

static const uint16_t kSelectDbBeginTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_DB,
};

static const uint16_t kQueryReturnTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_CONN_ID, WLR_TAG_RET_CODE,
};

/* Fetch is result-scoped. Row counts appear on RETURN, never the row values
 * themselves (DS 6.11). */
static const uint16_t kFetchBeginTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_RESULT_ID,
};

static const uint16_t kFetchReturnTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_RESULT_ID, WLR_TAG_RET_CODE,
};

/* Outcome-only RETURN/DONE set: the common tags plus the status.
 *
 * Used where a completion genuinely cannot carry a scope ID. The async query DONE
 * is the reason this exists: wlrAsyncDone rebuilds its token on the driver thread
 * from the fields carried on the request, which do not include the connection, so
 * wlrEmitCompletion is called with connId == 0 and omits CONN_ID entirely.
 * Validating those events against kQueryReturnTags would reject every one of them
 * for a tag the capture path cannot emit. The async fetch DONEs differ -- they do
 * carry RESULT_ID -- which is why they keep kFetchReturnTags.
 *
 * It also covers completions whose BEGIN established the scope and whose RETURN
 * adds nothing but the outcome (schemaless, free-result). Without a row of their
 * own those fall through to kCommonRule, which does not require RET_CODE, so a
 * RETURN missing its status would validate -- the same defect the conn-opt
 * comment above describes. */
static const uint16_t kOutcomeOnlyTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_RET_CODE,
};

/* STMT events are statement-scoped; replay rebuilds the state machine from them
 * (DS 6.9), so the logical statement ID is mandatory on every one.
 *
 * The capture side upholds this rather than merely happening to: WLR_API_BEGIN_STMT
 * records nothing at all when the statement ID is zero (it zeroes the token instead),
 * so a STMT BEGIN that exists in a file necessarily carries one -- a failed
 * taos_stmt_init has no ID to record and is therefore not recorded. WLR_API_END_STMT
 * is always passed the same handle's ID, which was minted before the BEGIN and stays
 * valid until close. Requiring the tag unconditionally is what keeps that invariant
 * checkable; if a future STMT path can complete with a zero ID, the emitter is what
 * needs fixing, not this rule. */
static const uint16_t kStmtTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_STMT_ID,
};

static const uint16_t kStmtCompletionTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_STMT_ID, WLR_TAG_RET_CODE,
};

static const uint16_t kStmtPrepareBeginTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_STMT_ID, WLR_TAG_SQL,
};

/* Each schemaless API variant must replay with the recorded protocol and precision,
 * so those two SML-specific tags are required in addition to the common event
 * envelope (DS 6.9). */
static const uint16_t kSmlBeginTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_SML_PROTOCOL, WLR_TAG_SML_PRECISION,
};

static const uint16_t kSmlReturnTags[] = {
    WLR_COMMON_EVENT_TAGS, WLR_TAG_CONN_ID, WLR_TAG_RET_CODE,
};

/* Chunk framing: BEGIN declares the total so a reader can tell a short run from
 * a complete one, END carries the whole-payload CRC (DS 6.13). */
static const uint16_t kChunkBeginTags[] = {
    WLR_TAG_WALL_NS,
    WLR_TAG_MONO_NS,
    WLR_TAG_TOTAL_LENGTH,
    WLR_TAG_CHUNK_COUNT,
};

static const uint16_t kChunkDataTags[] = {
    WLR_TAG_CHUNK_INDEX,
};

static const uint16_t kChunkEndTags[] = {
    WLR_TAG_PAYLOAD_CRC,
};

static const uint16_t kSessionStopTags[] = {
    WLR_TAG_WALL_NS,
    WLR_TAG_MONO_NS,
    WLR_TAG_ACTIVE_CALL_COUNT,
};

static const uint16_t kFatalTags[] = {
    WLR_TAG_WALL_NS,
    WLR_TAG_RET_CODE,
};

#define WLR_TAGS(a) (a), (int32_t)(sizeof(a) / sizeof((a)[0]))

/*
 * Rules are matched most-specific-first: a row with apiType 0 matches any API
 * and a row with phase 0 matches any phase, so specific rows must precede
 * wildcards.
 */
static const SWlrRequiredTags kRules[] = {
    /* connect group */
    {WLR_FRAME_EVENT, WLR_API_CONNECT, WLR_PHASE_BEGIN, WLR_TAGS(kConnectBeginTags)},
    /* Connect RETURN carries the outcome, but not necessarily a connection ID: a
     * failed connect never reaches wlrConnectMintId, so the emitter has no CONN_ID.
     * Successful connects do carry one, but a required-tag rule must also admit the
     * failure event that is most useful when diagnosing a connection problem. */
    {WLR_FRAME_EVENT, WLR_API_CONNECT, WLR_PHASE_RETURN, WLR_TAGS(kOutcomeOnlyTags)},
    {WLR_FRAME_EVENT, WLR_API_CONNECT_WITH, WLR_PHASE_BEGIN, WLR_TAGS(kConnectBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_CONNECT_WITH, WLR_PHASE_RETURN, WLR_TAGS(kOutcomeOnlyTags)},
    /* The three credential-bearing variants share connect's schema: the auth
     * material is absent from the file by construction, so nothing distinguishes
     * their required set. */
    {WLR_FRAME_EVENT, WLR_API_CONNECT_AUTH, WLR_PHASE_BEGIN, WLR_TAGS(kConnectBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_CONNECT_AUTH, WLR_PHASE_RETURN, WLR_TAGS(kOutcomeOnlyTags)},
    {WLR_FRAME_EVENT, WLR_API_CONNECT_TOTP, WLR_PHASE_BEGIN, WLR_TAGS(kConnectBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_CONNECT_TOTP, WLR_PHASE_RETURN, WLR_TAGS(kOutcomeOnlyTags)},
    /* Token auth passes no user -- the token carries the identity -- so USER cannot
     * be required here the way it is for the other connects. */
    {WLR_FRAME_EVENT, WLR_API_CONNECT_TOKEN, WLR_PHASE_BEGIN, WLR_TAGS(kCommonEventTags)},
    {WLR_FRAME_EVENT, WLR_API_CONNECT_TOKEN, WLR_PHASE_RETURN, WLR_TAGS(kOutcomeOnlyTags)},

    /* Connection options. CONN_ID is not required: unlike the fetch group, whose
     * hook is skipped when the result ID is 0, this event is recorded even for a
     * connection that predates recording and so has no logical ID. */
    {WLR_FRAME_EVENT, WLR_API_OPTIONS_CONNECTION, WLR_PHASE_BEGIN, WLR_TAGS(kConnOptBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_OPTIONS_CONNECTION, WLR_PHASE_RETURN, WLR_TAGS(kOutcomeOnlyTags)},

    /* query group: sync and async share the schema; async adds a DONE phase
     * that carries the same required set as RETURN. */
    {WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_BEGIN, WLR_TAGS(kQueryBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_QUERY, WLR_PHASE_RETURN, WLR_TAGS(kQueryReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_QUERY_WITH_REQID, WLR_PHASE_BEGIN, WLR_TAGS(kQueryBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_QUERY_WITH_REQID, WLR_PHASE_RETURN, WLR_TAGS(kQueryReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_QUERY_A, WLR_PHASE_BEGIN, WLR_TAGS(kQueryBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_QUERY_A, WLR_PHASE_RETURN, WLR_TAGS(kQueryReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_QUERY_A, WLR_PHASE_DONE, WLR_TAGS(kOutcomeOnlyTags)},
    {WLR_FRAME_EVENT, WLR_API_QUERY_A_WITH_REQID, WLR_PHASE_BEGIN, WLR_TAGS(kQueryBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_QUERY_A_WITH_REQID, WLR_PHASE_RETURN, WLR_TAGS(kQueryReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_QUERY_A_WITH_REQID, WLR_PHASE_DONE, WLR_TAGS(kOutcomeOnlyTags)},
    {WLR_FRAME_EVENT, WLR_API_VALIDATE_SQL, WLR_PHASE_BEGIN, WLR_TAGS(kQueryBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_VALIDATE_SQL, WLR_PHASE_RETURN, WLR_TAGS(kQueryReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SELECT_DB, WLR_PHASE_BEGIN, WLR_TAGS(kSelectDbBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SELECT_DB, WLR_PHASE_RETURN, WLR_TAGS(kOutcomeOnlyTags)},

    /* Prepare is the only statement operation whose replay requires SQL text. */
    {WLR_FRAME_EVENT, WLR_API_STMT_PREPARE, WLR_PHASE_BEGIN, WLR_TAGS(kStmtPrepareBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_STMT2_PREPARE, WLR_PHASE_BEGIN, WLR_TAGS(kStmtPrepareBeginTags)},

    /* fetch / free group */
    {WLR_FRAME_EVENT, WLR_API_FETCH_ROW, WLR_PHASE_BEGIN, WLR_TAGS(kFetchBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_ROW, WLR_PHASE_RETURN, WLR_TAGS(kFetchReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_BLOCK, WLR_PHASE_BEGIN, WLR_TAGS(kFetchBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_BLOCK, WLR_PHASE_RETURN, WLR_TAGS(kFetchReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_BLOCK_S, WLR_PHASE_BEGIN, WLR_TAGS(kFetchBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_BLOCK_S, WLR_PHASE_RETURN, WLR_TAGS(kFetchReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_RAW_BLOCK, WLR_PHASE_BEGIN, WLR_TAGS(kFetchBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_RAW_BLOCK, WLR_PHASE_RETURN, WLR_TAGS(kFetchReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_ROWS_A, WLR_PHASE_BEGIN, WLR_TAGS(kFetchBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_ROWS_A, WLR_PHASE_RETURN, WLR_TAGS(kFetchReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_ROWS_A, WLR_PHASE_DONE, WLR_TAGS(kFetchReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_RAW_BLOCK_A, WLR_PHASE_BEGIN, WLR_TAGS(kFetchBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_RAW_BLOCK_A, WLR_PHASE_RETURN, WLR_TAGS(kFetchReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_FETCH_RAW_BLOCK_A, WLR_PHASE_DONE, WLR_TAGS(kFetchReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_FREE_RESULT, WLR_PHASE_BEGIN, WLR_TAGS(kFetchBeginTags)},
    /* free-result RETURN echoes the result ID its BEGIN named, so the fetch set
     * applies here too. Without a row of its own it would fall through to
     * kCommonRule and a RETURN with no status would validate. */
    {WLR_FRAME_EVENT, WLR_API_FREE_RESULT, WLR_PHASE_RETURN, WLR_TAGS(kFetchReturnTags)},

    /* schemaless group */
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT, WLR_PHASE_BEGIN, WLR_TAGS(kSmlBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_WITH_REQID, WLR_PHASE_BEGIN, WLR_TAGS(kSmlBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW, WLR_PHASE_BEGIN, WLR_TAGS(kSmlBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW_WITH_REQID, WLR_PHASE_BEGIN, WLR_TAGS(kSmlBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_TTL, WLR_PHASE_BEGIN, WLR_TAGS(kSmlBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_TTL_WITH_REQID, WLR_PHASE_BEGIN, WLR_TAGS(kSmlBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW_TTL, WLR_PHASE_BEGIN, WLR_TAGS(kSmlBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW_TTL_WITH_REQID, WLR_PHASE_BEGIN, WLR_TAGS(kSmlBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW_TTL_WITH_REQID_TBNAME_KEY, WLR_PHASE_BEGIN,
     WLR_TAGS(kSmlBeginTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY, WLR_PHASE_BEGIN,
     WLR_TAGS(kSmlBeginTags)},

    /* Schemaless RETURN. Protocol and precision are BEGIN-only -- they describe the
     * call, not its outcome -- so this cannot reuse kSmlBeginTags. CONN_ID stays
     * required because every schemaless RETURN echoes the connection its BEGIN
     * named (smlInsertLines and the raw variant both set logicalConnId). */
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT, WLR_PHASE_RETURN, WLR_TAGS(kSmlReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_WITH_REQID, WLR_PHASE_RETURN, WLR_TAGS(kSmlReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW, WLR_PHASE_RETURN, WLR_TAGS(kSmlReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW_WITH_REQID, WLR_PHASE_RETURN, WLR_TAGS(kSmlReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_TTL, WLR_PHASE_RETURN, WLR_TAGS(kSmlReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_TTL_WITH_REQID, WLR_PHASE_RETURN, WLR_TAGS(kSmlReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW_TTL, WLR_PHASE_RETURN, WLR_TAGS(kSmlReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW_TTL_WITH_REQID, WLR_PHASE_RETURN,
     WLR_TAGS(kSmlReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_RAW_TTL_WITH_REQID_TBNAME_KEY, WLR_PHASE_RETURN,
     WLR_TAGS(kSmlReturnTags)},
    {WLR_FRAME_EVENT, WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY, WLR_PHASE_RETURN,
     WLR_TAGS(kSmlReturnTags)},

    /* chunk framing */
    {WLR_FRAME_CHUNK_BEGIN, WLR_API_UNKNOWN, 0, WLR_TAGS(kChunkBeginTags)},
    {WLR_FRAME_CHUNK_DATA, WLR_API_UNKNOWN, 0, WLR_TAGS(kChunkDataTags)},
    {WLR_FRAME_CHUNK_END, WLR_API_UNKNOWN, 0, WLR_TAGS(kChunkEndTags)},
    {WLR_FRAME_FATAL, WLR_API_UNKNOWN, 0, WLR_TAGS(kFatalTags)},
};

#define WLR_RULE_COUNT ((int32_t)(sizeof(kRules) / sizeof(kRules[0])))

static bool wlrIsStmtApi(uint16_t apiType);
static bool wlrUsesCommonSchema(uint16_t apiType);

/*
 * Check that no wildcard row in the ordered kRules table shadows a later specific row.
 *
 * wlrGetRequiredTags returns the FIRST match, so the table is only correct while every
 * wildcard (apiType == WLR_API_UNKNOWN, or phase == 0) sits after every specific row it
 * could match. That was enforced by convention alone: inserting one wildcard near the
 * top silently replaces the schema for dozens of APIs, with no compile error and no
 * runtime complaint -- the validator would simply start accepting under-specified
 * events. Exposed so a test can assert the invariant over the whole table rather than
 * over a hand-listed sample.
 *
 * Generated EVENT fallbacks are selected only after kRules is exhausted. Therefore an
 * EVENT row with a wildcard API would shadow the generated STMT/common schemas even if
 * it is the final table row; such rows are forbidden and reported directly. Returns
 * the index of the first shadowing table row, or -1 when the ordering is sound.
 */
int32_t wlrFindShadowedRule(void) {
  int32_t i, j;
  for (i = 0; i < WLR_RULE_COUNT; ++i) {
    const SWlrRequiredTags *w = &kRules[i];
    bool wildApi = (w->apiType == WLR_API_UNKNOWN);
    bool wildPhase = (w->phase == 0);
    if (w->frameType == WLR_FRAME_EVENT && wildApi) return i;
    if (w->frameType == WLR_FRAME_EVENT && wildPhase &&
        (wlrIsStmtApi(w->apiType) || wlrUsesCommonSchema(w->apiType))) {
      return i;
    }
    if (!wildApi && !wildPhase) continue; /* fully specific: shadows nothing */

    for (j = i + 1; j < WLR_RULE_COUNT; ++j) {
      const SWlrRequiredTags *s = &kRules[j];
      if (s->frameType != w->frameType) continue;
      /* Would the earlier row answer a lookup aimed at this later one? */
      if (!wildApi && s->apiType != w->apiType) continue;
      if (!wildPhase && s->phase != w->phase) continue;
      /* The later row must be strictly more specific in a wildcard dimension. */
      if ((wildApi && s->apiType != WLR_API_UNKNOWN) || (wildPhase && s->phase != 0)) return i;
    }
  }
  return -1;
}

/* STMT rules are generated rather than listed: every STMT1/STMT2 API shares
 * the same required set, so enumerating them would add dozens of near-identical rows
 * without adding information. */
WLR_LAYOUT_CHECK(stmt_api_range, WLR_API_STMT_CLOSE == WLR_API_STMT_INIT + 14);
WLR_LAYOUT_CHECK(stmt2_api_range, WLR_API_STMT2_CLOSE == WLR_API_STMT2_INIT + 8);
WLR_LAYOUT_CHECK(required_bit_is_uint16_high_bit,
                 WLR_TAG_REQUIRED_BIT == (UINT16_MAX + 1u) / 2u);
WLR_LAYOUT_CHECK(seen_tags_cover_base_tag_space,
                 WLR_TAG_PAYLOAD_CRC < WLR_TAG_REQUIRED_BIT && WLR_TAG_REQUIRED_BIT % 64u == 0u);
static bool wlrIsStmtApi(uint16_t apiType) {
  return (apiType >= WLR_API_STMT_INIT && apiType <= WLR_API_STMT_CLOSE) ||
         (apiType >= WLR_API_STMT2_INIT && apiType <= WLR_API_STMT2_CLOSE);
}

static bool wlrUsesCommonSchema(uint16_t apiType) {
  switch (apiType) {
    case WLR_API_CLOSE:
    case WLR_API_SELECT_DB:
    case WLR_API_RESET_CURRENT_DB:
    case WLR_API_TXN_BEGIN:
    case WLR_API_TXN_COMMIT:
    case WLR_API_TXN_ROLLBACK:
    case WLR_API_KILL_QUERY:
    case WLR_API_STOP_QUERY:
      return true;
    default:
      return false;
  }
}

static const SWlrRequiredTags kStmtRule = {WLR_FRAME_EVENT, WLR_API_UNKNOWN, 0, WLR_TAGS(kStmtTags)};
static const SWlrRequiredTags kStmtCompletionRule = {WLR_FRAME_EVENT, WLR_API_UNKNOWN, 0,
                                                     WLR_TAGS(kStmtCompletionTags)};
static const SWlrRequiredTags kCommonRule = {WLR_FRAME_EVENT, WLR_API_UNKNOWN, 0,
                                             WLR_TAGS(kCommonEventTags)};
static const SWlrRequiredTags kOutcomeRule = {WLR_FRAME_EVENT, WLR_API_UNKNOWN, 0,
                                              WLR_TAGS(kOutcomeOnlyTags)};
static const SWlrRequiredTags kSessionStopRule = {WLR_FRAME_SESSION_STOP, WLR_API_UNKNOWN, 0,
                                                  WLR_TAGS(kSessionStopTags)};

const SWlrRequiredTags *wlrGetRequiredTags(uint16_t frameType, uint16_t apiType, uint16_t phase) {
  int32_t i;

  for (i = 0; i < WLR_RULE_COUNT; ++i) {
    const SWlrRequiredTags *r = &kRules[i];
    if (r->frameType != frameType) continue;
    if (r->apiType != WLR_API_UNKNOWN && r->apiType != apiType) continue;
    if (r->phase != 0 && r->phase != phase) continue;
    return r;
  }

  if (frameType == WLR_FRAME_SESSION_STOP) return &kSessionStopRule;

  if (frameType == WLR_FRAME_EVENT) {
    if (wlrIsStmtApi(apiType)) {
      return (phase == WLR_PHASE_BEGIN) ? &kStmtRule : &kStmtCompletionRule;
    }
    if (wlrUsesCommonSchema(apiType)) {
      return (phase == WLR_PHASE_BEGIN) ? &kCommonRule : &kOutcomeRule;
    }
  }

  return NULL;
}

/*
 * Every tag number this build defines. Grouped as in EWlrTag so the two stay
 * comparable by eye; the conformance test checks they agree.
 */
bool wlrTagIsKnown(uint16_t tag) {
  switch (WLR_TAG_BASE(tag)) {
    /* common */
    case WLR_TAG_WALL_NS:
    case WLR_TAG_MONO_NS:
    case WLR_TAG_ORIGIN_TID:
    case WLR_TAG_EMITTER_TID:
    case WLR_TAG_API_TYPE:
    case WLR_TAG_EVENT_PHASE:
    case WLR_TAG_CALL_ID:
    case WLR_TAG_PARENT_CALL_ID:
    case WLR_TAG_CONN_ID:
    case WLR_TAG_STMT_ID:
    case WLR_TAG_RESULT_ID:
    case WLR_TAG_REQID:
    case WLR_TAG_QID:
    /* sql / connect */
    case WLR_TAG_SQL:
    case WLR_TAG_ENDPOINT:
    case WLR_TAG_USER:
    case WLR_TAG_DB:
    case WLR_TAG_PORT:
    /* connection options */
    case WLR_TAG_CONN_OPT_NAME:
    case WLR_TAG_CONN_OPT_TYPE:
    case WLR_TAG_CONN_OPT_VALUE:
    case WLR_TAG_CONN_OPT_LIST:
    /* stmt bind */
    case WLR_TAG_BIND_MODE:
    case WLR_TAG_BIND_TYPE:
    case WLR_TAG_BIND_COUNT:
    case WLR_TAG_BIND_NULL:
    case WLR_TAG_BIND_LENGTH:
    case WLR_TAG_BIND_BYTES:
    case WLR_TAG_STMT_TBNAME:
    case WLR_TAG_STMT2_TBNAMES:
    case WLR_TAG_STMT2_NULLABLE_TBNAMES:
    case WLR_TAG_STMT2_TAGS:
    case WLR_TAG_BIND_COL_IDX:
    case WLR_TAG_STMT2_ASYNC:
    case WLR_TAG_STMT_INIT_OPTIONS:
    /* schemaless */
    case WLR_TAG_SML_PROTOCOL:
    case WLR_TAG_SML_PRECISION:
    case WLR_TAG_SML_TTL:
    case WLR_TAG_SML_COUNT:
    case WLR_TAG_SML_TBNAME_KEY:
    case WLR_TAG_SML_PAYLOAD:
    /* session lifecycle */
    case WLR_TAG_ACTIVE_CALL_COUNT:
    /* results */
    case WLR_TAG_RET_CODE:
    case WLR_TAG_AFFECTED_ROWS:
    case WLR_TAG_FETCH_ROWS:
    case WLR_TAG_DURATION_NS:
    /* chunking */
    case WLR_TAG_CHUNK_INDEX:
    case WLR_TAG_CHUNK_COUNT:
    case WLR_TAG_TOTAL_LENGTH:
    case WLR_TAG_PAYLOAD_CRC:
      return true;
    default:
      return false;
  }
}

EWlrDecodeResult wlrValidateRequiredTags(uint16_t frameType, uint16_t apiType, uint16_t phase,
                                         const SWlrTlv *tlvs, int32_t tlvCount) {
  const SWlrRequiredTags *rule;
  uint16_t                inlineSeen[WLR_INLINE_SEEN_TAGS];
  int32_t                 inlineSeenCount = 0;
  uint64_t               *seenTags = NULL;
  EWlrDecodeResult        result = WLR_DECODE_OK;
  int32_t                 i;
  int32_t                 j;
  uint64_t                activeCallCount = 0;
  uint32_t                callIdCount = 0;

  /* Checked before the rule lookup so an inconsistent argument pair is reported
   * as malformed regardless of whether a rule happens to constrain this
   * combination. A negative count would otherwise skip the search loop below and
   * report every required tag as missing -- UNKNOWN_REQUIRED_TAG, which tells the
   * replay tool the file is bad when in fact the call was. */
  if (tlvCount < 0 || (tlvCount > 0 && tlvs == NULL)) return WLR_DECODE_BAD_ARG;
  if ((uint32_t)tlvCount > WLR_MAX_FRAME_PAYLOAD / WLR_TLV_HEADER_SIZE) {
    return WLR_DECODE_BAD_LENGTH;
  }

  /*
   * Forward compatibility, per the format rule in wlrFormat.h: a frame carrying a
   * REQUIRED tag this build does not know must be rejected, not replayed with the
   * tag's meaning silently dropped. An unknown OPTIONAL tag is skipped instead --
   * that asymmetry is what lets a minor version add optional tags.
   *
   * Checked before the rule lookup so it applies to every frame, including
   * combinations no rule constrains.
   */
  for (j = 0; j < tlvCount; ++j) {
    uint16_t baseTag = WLR_TAG_BASE(tlvs[j].tag);
    if (WLR_TAG_IS_REQUIRED(tlvs[j].tag) && !wlrTagIsKnown(tlvs[j].tag)) {
      result = WLR_DECODE_UNKNOWN_REQUIRED_TAG;
      goto _return;
    }
    /* SESSION_STOP carries one CALL_ID TLV per active call; every other tag must
     * occur at most once in a frame. */
    if (frameType == WLR_FRAME_SESSION_STOP && baseTag == WLR_TAG_CALL_ID) {
      ++callIdCount;
      continue;
    }
    if (frameType == WLR_FRAME_SESSION_STOP && baseTag == WLR_TAG_ACTIVE_CALL_COUNT) {
      if (tlvs[j].length != 8) {
        result = WLR_DECODE_BAD_LENGTH;
        goto _return;
      }
      activeCallCount = wlrGetU64(tlvs[j].value);
    }

    if (seenTags != NULL) {
      uint64_t mask = UINT64_C(1) << (baseTag & 63u);
      if ((seenTags[baseTag >> 6] & mask) != 0) {
        result = WLR_DECODE_BAD_LENGTH;
        goto _return;
      }
      seenTags[baseTag >> 6] |= mask;
      continue;
    }

    for (i = 0; i < inlineSeenCount; ++i) {
      if (inlineSeen[i] == baseTag) {
        result = WLR_DECODE_BAD_LENGTH;
        goto _return;
      }
    }
    if (inlineSeenCount < WLR_INLINE_SEEN_TAGS) {
      inlineSeen[inlineSeenCount++] = baseTag;
      continue;
    }

    /* WLR_TAG_BASE clears REQUIRED_BIT, so the largest possible bitmap index is
     * (WLR_TAG_REQUIRED_BIT - 1) >> 6, exactly one below this allocation size. */
    seenTags = (uint64_t *)taosMemoryCalloc(WLR_TAG_REQUIRED_BIT / 64u, sizeof(*seenTags));
    if (seenTags == NULL) {
      result = WLR_DECODE_ALLOC_FAILED;
      goto _return;
    }
    for (i = 0; i < inlineSeenCount; ++i) {
      seenTags[inlineSeen[i] >> 6] |= UINT64_C(1) << (inlineSeen[i] & 63u);
    }
    seenTags[baseTag >> 6] |= UINT64_C(1) << (baseTag & 63u);
  }

  if (frameType == WLR_FRAME_SESSION_STOP && activeCallCount != callIdCount) {
    result = WLR_DECODE_BAD_LENGTH;
    goto _return;
  }

  rule = wlrGetRequiredTags(frameType, apiType, phase);
  if (rule == NULL) {
    result = WLR_DECODE_UNKNOWN_REQUIRED_TAG;
    goto _return;
  }

  for (i = 0; i < rule->tagCount; ++i) {
    bool found = false;
    if (seenTags != NULL &&
        !(frameType == WLR_FRAME_SESSION_STOP && rule->tags[i] == WLR_TAG_CALL_ID)) {
      uint16_t baseTag = rule->tags[i];
      found = (seenTags[baseTag >> 6] & (UINT64_C(1) << (baseTag & 63u))) != 0;
    } else if (!(frameType == WLR_FRAME_SESSION_STOP && rule->tags[i] == WLR_TAG_CALL_ID)) {
      for (j = 0; j < inlineSeenCount; ++j) {
        if (inlineSeen[j] == rule->tags[i]) {
          found = true;
          break;
        }
      }
    } else {
      for (j = 0; j < tlvCount; ++j) {
        if (WLR_TAG_BASE(tlvs[j].tag) == rule->tags[i]) {
          found = true;
          break;
        }
      }
    }
    if (!found) {
      result = WLR_DECODE_UNKNOWN_REQUIRED_TAG;
      goto _return;
    }
  }

_return:
  taosMemoryFree(seenTags);
  return result;
}

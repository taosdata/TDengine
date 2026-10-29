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
 * taosreplay execution (DS 6.19-6.24).
 *
 * This phase reissues recorded calls against a target cluster through public taos.h
 * only. Recorded logical IDs are mapped to freshly created handles -- a recorded
 * address means nothing here, which is why the format stores IDs rather than
 * pointers (DS 6.18).
 *
 * Events are dispatched onto the per-thread Worker Lanes of DS 6.19 -- one lane per
 * recorded thread -- and each lane honours the recorded inter-call gaps. Lanes, not
 * a single serial pass, are what reproduce the contention the recording captured; the
 * lane machinery and the shared handle maps live in replayLane.c, so what remains
 * here is the per-event API dispatch that each lane runs.
 */

#include "replayLane.h"

#define REPLAY_TBNAME_BUFFER_SIZE (TSDB_TABLE_NAME_LEN * 2)
#define REPLAY_MAX_BIND_COLUMNS    128

typedef struct {
  tsem_t  sem;
  int32_t code;
  int32_t rows;
} SReplayAsyncFetch;

static void replayAsyncFetchCallback(void *param, TAOS_RES *res, int32_t value) {
  SReplayAsyncFetch *fetch = param;
  (void)res;
  fetch->code = value < 0 ? value : TSDB_CODE_SUCCESS;
  fetch->rows = value > 0 ? value : 0;
  (void)tsem_post(&fetch->sem);
}

static int32_t replayAsyncFetch(TAOS_RES *res, bool raw, int32_t *rows) {
  SReplayAsyncFetch fetch = {.code = TSDB_CODE_SUCCESS};
  if (tsem_init(&fetch.sem, 0, 0) != 0) return terrno;
  if (raw) {
    taos_fetch_raw_block_a(res, replayAsyncFetchCallback, &fetch);
  } else {
    taos_fetch_rows_a(res, replayAsyncFetchCallback, &fetch);
  }
  int32_t code = tsem_wait(&fetch.sem) == 0 ? fetch.code : terrno;
  if (code == TSDB_CODE_SUCCESS) *rows = fetch.rows;
  if (tsem_destroy(&fetch.sem) != 0 && code == TSDB_CODE_SUCCESS) code = terrno;
  return code;
}

static bool replayApiReissuedSynchronously(uint16_t apiType) {
  return apiType == WLR_API_QUERY_A || apiType == WLR_API_QUERY_A_WITH_REQID ||
         apiType == WLR_API_FETCH_ROWS_A || apiType == WLR_API_FETCH_RAW_BLOCK_A;
}

static bool replayCallAwaitedSynchronously(const SReplayEvent *event) {
  if (replayApiReissuedSynchronously(event->apiType)) return true;
  return event->apiType == WLR_API_STMT2_EXEC && event->stmt2Async;
}

/* --------------------------------------------------------------- credentials */

static const char *replayFindPassword(const SReplayOpts *opts, const char *user, uint32_t userLen) {
  int32_t i;
  for (i = 0; i < opts->credCount; ++i) {
    const char *eq = strchr(opts->creds[i], '=');
    if (eq == NULL) continue;
    size_t nameLen = (size_t)(eq - opts->creds[i]);
    if (user != NULL && nameLen == (size_t)userLen && strncmp(opts->creds[i], user, nameLen) == 0) {
      return eq + 1;
    }
  }
  return NULL;
}

/* ------------------------------------------------------------ endpoint mapping */

/*
 * Parse the port half of an "host:port" endpoint.
 *
 * Returns 0 on success. atoi returned 0 for a malformed or empty suffix ("host:"), and
 * port 0 was then handed to taos_connect as if it had been resolved -- surfacing as an
 * opaque connection failure with nothing pointing at the mapping that caused it. The
 * --target-host branch already guards against a zero port; these paths did not.
 */
static int32_t replayParsePort(const char *s, uint16_t *port) {
  int32_t v = 0;
  if (s == NULL || s[0] == 0) return -1;
  for (const unsigned char *p = (const unsigned char *)s; *p != 0; ++p) {
    if (*p < '0' || *p > '9') return -1;
  }
  if (taosStr2int32(s, &v) != 0) return -1;
  if (v <= 0 || v > 65535) return -1;
  *port = (uint16_t)v;
  return 0;
}

/*
 * Resolve the endpoint to connect to.
 *
 * Returns 0 on success. A recorded endpoint with no mapping is an error, not a
 * fallback: silently connecting to the recorded address could point a replay at the
 * production cluster it was captured from (DS 6.20).
 */
static int32_t replayResolveEndpoint(const SReplayOpts *opts, const char *recEp, uint32_t recEpLen,
                                     uint16_t recPort, char *host, size_t hostLen, uint16_t *port) {
  char recorded[TSDB_FQDN_LEN + 16] = {0};
  int32_t i;

  if (opts->targetHost != NULL) {
    if (strlen(opts->targetHost) >= hostLen) {
      (void)fprintf(stderr, "--target-host is too long (max %zu)\n", hostLen - 1);
      return -1;
    }
    tstrncpy(host, opts->targetHost, hostLen);
    *port = (opts->targetPort != 0) ? opts->targetPort : 6030;
    return 0;
  }

  /*
   * A recorded endpoint too long for the buffer is an ERROR, not an empty string.
   *
   * Skipping the copy used to leave `recorded` empty, and with no --target-host and no
   * --endpoint-map that fell straight into the recorded[0] == 0 branch below and
   * connected to localhost:6030 -- silently replaying against a different cluster than
   * the one named in the recording. That is precisely the misdirection DS 6.20 exists to
   * prevent, arrived at by accident instead of by an unmapped endpoint.
   */
  if (recEp != NULL && recEpLen > 0) {
    size_t portChars = recPort != 0 ? 6u : 0u;
    if (memchr(recEp, '\0', recEpLen) != NULL) {
      (void)fprintf(stderr, "recorded endpoint contains an embedded NUL; refusing to resolve it\n");
      return -1;
    }
    if ((size_t)recEpLen + portChars >= sizeof(recorded)) {
      (void)fprintf(stderr, "recorded endpoint is %u bytes, too long to resolve (max %zu)\n",
                    recEpLen, sizeof(recorded) - 1);
      return -1;
    }
    memcpy(recorded, recEp, recEpLen);
    if (recPort != 0) {
      (void)snprintf(recorded + recEpLen, sizeof(recorded) - recEpLen, ":%u", recPort);
    }
  }

  for (i = 0; i < opts->endpointMapCount; ++i) {
    const char *eq = strchr(opts->endpointMaps[i], '=');
    if (eq == NULL) continue;
    size_t oldLen = (size_t)(eq - opts->endpointMaps[i]);
    if (strlen(recorded) == oldLen && strncmp(opts->endpointMaps[i], recorded, oldLen) == 0) {
      const char *newEp = eq + 1;
      const char *colon = NULL;
      const char *hostStart = newEp;
      size_t hostChars = 0;
      if (newEp[0] == '[') {
        const char *close = strchr(newEp, ']');
        if (close == NULL || (close[1] != 0 && close[1] != ':')) {
          (void)fprintf(stderr, "invalid bracketed host in --endpoint-map entry '%s'\n",
                        opts->endpointMaps[i]);
          return -1;
        }
        hostStart = newEp + 1;
        hostChars = (size_t)(close - hostStart);
        if (close[1] == ':') colon = close + 1;
      } else {
        const char *firstColon = strchr(newEp, ':');
        const char *lastColon = strrchr(newEp, ':');
        colon = firstColon == lastColon ? lastColon : NULL;
        hostChars = colon != NULL ? (size_t)(colon - newEp) : strlen(newEp);
      }
      if (colon == NULL) {
        if (hostChars == 0) {
          (void)fprintf(stderr, "empty host in --endpoint-map entry '%s'\n", opts->endpointMaps[i]);
          return -1;
        }
        if (hostChars >= hostLen) {
          (void)fprintf(stderr, "host in --endpoint-map entry '%s' is too long (max %zu)\n",
                        opts->endpointMaps[i], hostLen - 1);
          return -1;
        }
        memcpy(host, hostStart, hostChars);
        host[hostChars] = 0;
        *port = 6030;
      } else {
        size_t hl = hostChars;
        if (hl == 0) {
          (void)fprintf(stderr, "empty host in --endpoint-map entry '%s'\n", opts->endpointMaps[i]);
          return -1;
        }
        if (hl >= hostLen) {
          (void)fprintf(stderr, "host in --endpoint-map entry '%s' is too long (max %zu)\n",
                        opts->endpointMaps[i], hostLen - 1);
          return -1;
        }
        memcpy(host, hostStart, hl);
        host[hl] = 0;
        if (replayParsePort(colon + 1, port) != 0) {
          (void)fprintf(stderr, "invalid port in --endpoint-map entry '%s'\n",
                        opts->endpointMaps[i]);
          return -1;
        }
      }
      return 0;
    }
  }

  if (opts->endpointMapCount > 0) {
    (void)fprintf(stderr, "no --endpoint-map entry for recorded endpoint '%s'\n", recorded);
    return -1;
  }

  /* Without a redirect, pass the caller's original endpoint and port to taos_connect.
   * The native client resolves configured default ports and IPv6 syntax itself. */
  if (recEp != NULL && recEpLen > 0) {
    if (recEpLen >= hostLen) {
      (void)fprintf(stderr, "recorded endpoint '%s' is too long (max %zu)\n",
                    recorded, hostLen - 1);
      return -1;
    }
    memcpy(host, recEp, recEpLen);
    host[recEpLen] = 0;
  }
  *port = recPort;
  return 0;
}

/*
 * Rebuild a TAOS_MULTI_BIND array from a recorded bind blob.
 *
 * The decoded values and null flags point into the segment buffer, which outlives the
 * call. Length tables cannot be aliased: wire u32 values are little-endian and may be
 * unaligned, while the client API requires an aligned host int32_t array. Variable-
 * length values also cannot be aliased because the wire stores them packed while the
 * API addresses rows with a fixed buffer_length stride. When fixedStride is false,
 * the caller is converting to a STMT2 bind type whose API consumes packed values, so
 * the wire value region is retained as-is. ownedLengths receives one allocation per
 * column containing the host lengths and, when needed, the repacked values for the
 * caller to release after the API call.
 *
 * Returns the column count, or -1 on a malformed blob.
 */
#define REPLAY_BIND_OOM (-2)

static int32_t replayBuildBinds(const uint8_t *blob, uint32_t blobLen, TAOS_MULTI_BIND *out,
                                int32_t capacity, bool fixedStride, int32_t **ownedLengths) {
  SWlrBindCol cols[REPLAY_MAX_BIND_COLUMNS];
  int32_t     count = 0;
  int32_t     encodedCount = 0;
  int32_t     i;
  const int32_t colCapacity = (int32_t)(sizeof(cols) / sizeof(cols[0]));
  bool        allocationFailed = false;

  if (capacity <= 0 || capacity > colCapacity || ownedLengths == NULL || out == NULL) return -1;
  memset(out, 0, (size_t)capacity * sizeof(*out));
  if (capacity > 0) {
    memset(ownedLengths, 0, (size_t)capacity * sizeof(*ownedLengths));
  }
  if (blob == NULL || blobLen == 0) return -1;
  if (wlrDecodeBind(blob, blobLen, NULL, 0, &encodedCount) != WLR_DECODE_OK) return -1;
  if (encodedCount <= 0 || encodedCount > capacity ||
      encodedCount > colCapacity) {
    (void)fprintf(stderr, "replay: bind payload has %d columns; maximum supported is %d\n",
                  encodedCount, (int32_t)(sizeof(cols) / sizeof(cols[0])));
    return -1;
  }
  if (wlrDecodeBind(blob, blobLen, cols, colCapacity, &count) != WLR_DECODE_OK) return -1;
  if (count <= 0 || count > colCapacity || count > capacity) return -1;

  for (i = 0; i < count; ++i) {
    uint32_t width = wlrTypeFixedWidth(cols[i].type);
    uint32_t widest = 0;
    size_t   lengthsBytes = (size_t)cols[i].elemCount * sizeof(int32_t);
    size_t   valuesBytes = 0;
    size_t   allocBytes;
    bool     needsEmptyValue = false;
    memset(&out[i], 0, sizeof(out[i]));
    out[i].buffer_type = cols[i].type;
    out[i].num = cols[i].elemCount;
    /* is_null is always present on the wire, so this is never a guess. */
    out[i].is_null = (char *)cols[i].isNull;
    if (width == 0) {
      int32_t e;
      for (e = 0; e < cols[i].elemCount; ++e) {
        uint32_t length = wlrBindColLength(&cols[i], e);
        if (length > INT32_MAX) goto bind_build_failed;
        if (length > widest) widest = length;
      }
      if (cols[i].elemCount > 0 && widest > SIZE_MAX / (size_t)cols[i].elemCount) {
        goto bind_build_failed;
      }
      if (fixedStride) valuesBytes = (size_t)widest * (size_t)cols[i].elemCount;
      if (cols[i].valueBytes == 0) {
        for (int32_t e = 0; e < cols[i].elemCount; ++e) {
          if (cols[i].isNull[e] == 0) {
            needsEmptyValue = true;
            break;
          }
        }
      }
    }
    if (lengthsBytes > SIZE_MAX - valuesBytes) goto bind_build_failed;
    allocBytes = lengthsBytes + valuesBytes;
    if (needsEmptyValue) {
      if (allocBytes == SIZE_MAX) goto bind_build_failed;
      allocBytes++;
    }
    if (cols[i].elemCount > 0) {
      int32_t e;
      ownedLengths[i] = (int32_t *)taosMemoryCalloc(1, allocBytes);
      if (ownedLengths[i] == NULL) {
        allocationFailed = true;
        goto bind_build_failed;
      }
      for (e = 0; e < cols[i].elemCount; ++e) {
        ownedLengths[i][e] = (int32_t)wlrBindColLength(&cols[i], e);
      }
    }
    out[i].length = width == 0 ? ownedLengths[i] : NULL;
    if (width > 0 || !fixedStride) {
      out[i].buffer = needsEmptyValue ? (uint8_t *)ownedLengths[i] + lengthsBytes + valuesBytes
                                      : (void *)cols[i].values;
      out[i].buffer_length = width;
    } else {
      uint8_t *repacked = (uint8_t *)ownedLengths[i] + lengthsBytes;
      size_t   packedOff = 0;
      int32_t  e;
      out[i].buffer = valuesBytes > 0 ? repacked
                                      : needsEmptyValue ? (uint8_t *)ownedLengths[i] + allocBytes - 1
                                                        : NULL;
      out[i].buffer_length = widest;
      for (e = 0; e < cols[i].elemCount; ++e) {
        uint32_t length = (uint32_t)ownedLengths[i][e];
        if (cols[i].isNull[e] != 0) continue;
        if ((uint64_t)packedOff + length > cols[i].valueBytes) goto bind_build_failed;
        if (length > 0) {
          memcpy(repacked + (size_t)e * widest, cols[i].values + packedOff, length);
        }
        packedOff += length;
      }
      if (packedOff != cols[i].valueBytes) goto bind_build_failed;
    }
  }
  return count;

bind_build_failed:
  for (int32_t k = 0; k < capacity; ++k) {
    taosMemoryFree(ownedLengths[k]);
    ownedLengths[k] = NULL;
    out[k].length = NULL;
    out[k].buffer = NULL;
  }
  return allocationFailed ? REPLAY_BIND_OOM : -1;
}

static void replayFreeBindLengths(int32_t **ownedLengths, int32_t count) {
  int32_t i;
  for (i = 0; i < count; ++i) taosMemoryFree(ownedLengths[i]);
}

static bool replayStmtColumnCount(SReplayShared *sh, int32_t sessionIdx, uint64_t recordedId,
                                  TAOS_STMT *stmt, int32_t *colCount) {
  if (replayLaneGetStmtShape(sh, sessionIdx, recordedId, colCount)) return true;
  int32_t count = 0;
  if (taos_stmt_num_params(stmt, &count) != TSDB_CODE_SUCCESS ||
      count <= 0 || count > REPLAY_MAX_BIND_COLUMNS) return false;
  replayLaneSetStmtShape(sh, sessionIdx, recordedId, count, true);
  *colCount = count;
  return true;
}

static bool replayStmt2FieldCounts(TAOS_STMT2 *stmt2, int32_t *colCount, int32_t *tagCount) {
  int32_t isInsert = 0;
  int32_t fieldCount = 0;
  TAOS_FIELD_ALL *fields = NULL;
  if (taos_stmt2_is_insert(stmt2, &isInsert) != TSDB_CODE_SUCCESS ||
      taos_stmt2_get_fields(stmt2, &fieldCount, &fields) != TSDB_CODE_SUCCESS) {
    taos_stmt2_free_fields(stmt2, fields);
    return false;
  }
  bool valid = fieldCount >= 0;
  *colCount = 0;
  *tagCount = 0;
  if (isInsert) {
    if (fields == NULL) valid = false;
    for (int32_t index = 0; valid && index < fieldCount; ++index) {
      if (fields[index].field_type == TAOS_FIELD_TAG) ++*tagCount;
      else if (fields[index].field_type == TAOS_FIELD_COL) ++*colCount;
      else if (fields[index].field_type != TAOS_FIELD_TBNAME) valid = false;
    }
  } else {
    *colCount = fieldCount;
  }
  taos_stmt2_free_fields(stmt2, fields);
  return valid && *colCount > 0 && *colCount <= REPLAY_MAX_BIND_COLUMNS &&
         *tagCount <= REPLAY_MAX_BIND_COLUMNS;
}

static int32_t replayBuildStmt2Tags(const uint8_t *blob, uint32_t blobLen, int32_t tableCount,
                                    int32_t tagsPerTable,
                                    TAOS_STMT2_BIND **groups, TAOS_STMT2_BIND *cells,
                                    int32_t **ownedLengths) {
  memset(groups, 0, (size_t)tableCount * sizeof(*groups));
  memset(ownedLengths, 0, REPLAY_MAX_BIND_COLUMNS * sizeof(*ownedLengths));
  if (blob == NULL) return blobLen == 0 ? 0 : -1;
  if (blobLen < 4 || wlrGetU32(blob) != (uint32_t)tableCount) return -1;
  uint32_t offset = 4;
  int32_t total = 0;
  int32_t error = -1;
  for (int32_t table = 0; table < tableCount; ++table) {
    if (blobLen - offset < 4) goto failed;
    uint32_t length = wlrGetU32(blob + offset);
    offset += 4;
    if (length > blobLen - offset) goto failed;
    if (length > 0) {
      TAOS_MULTI_BIND flat[REPLAY_MAX_BIND_COLUMNS];
      int32_t count = replayBuildBinds(blob + offset, length, flat, REPLAY_MAX_BIND_COLUMNS - total,
                                       false, ownedLengths + total);
      if (count <= 0) {
        if (count == REPLAY_BIND_OOM) error = REPLAY_BIND_OOM;
        goto failed;
      }
      if (count != tagsPerTable) {
        replayFreeBindLengths(ownedLengths + total, count);
        goto failed;
      }
      groups[table] = &cells[total];
      for (int32_t column = 0; column < count; ++column) {
        TAOS_STMT2_BIND *dst = &cells[total + column];
        TAOS_MULTI_BIND *src = &flat[column];
        dst->buffer_type = src->buffer_type;
        dst->buffer = src->buffer;
        dst->length = src->length;
        dst->is_null = src->is_null;
        dst->num = src->num;
      }
      total += count;
    }
    offset += length;
  }
  if (offset != blobLen) goto failed;
  return total;

failed:
  replayFreeBindLengths(ownedLengths, total);
  return error;
}

static int32_t replayFatalEvent(SReplayShared *sh, int32_t code, int32_t executed,
                                int32_t skipped, int32_t errors, int32_t mismatches) {
  replayLaneAddStat(sh, executed, skipped, errors + 1, mismatches);
  return code;
}

/* -------------------------------------------------------------------- replay */

/*
 * Execute one recorded event on the calling lane.
 *
 * Returns REPLAY_EXIT_OK, or a fatal code when the lane cannot continue (allocation
 * failure, unmappable endpoint). Per-call failures are counted, not fatal: a workload
 * that failed during recording should fail the same way here.
 */
static void replayCompareOutcome(SReplayShared *sh, int32_t selfIdx, uint16_t apiType,
                                 int32_t replayCode, int32_t *errors, int32_t *mismatches) {
  int32_t terminalIdx = (sh != NULL && sh->terminalByBegin != NULL && selfIdx >= 0 &&
                         selfIdx < sh->eventTotal)
                            ? sh->terminalByBegin[selfIdx]
                            : -1;
  int32_t recordedCode = terminalIdx >= 0 ? sh->session->events[terminalIdx].retCode : 0;
  if (replayCode != 0 && recordedCode == 0) ++(*errors);
  if (replayCode != recordedCode) {
    ++(*mismatches);
    if (sh->opts->verbose) {
      (void)fprintf(stderr, "code mismatch: api %u recorded %d, replay %d\n", apiType,
                    recordedCode, replayCode);
    }
  }
}

static void replayCompareCount(SReplayShared *sh, int32_t selfIdx, uint16_t apiType,
                               int64_t actual, bool affected, int32_t *mismatches) {
  int32_t terminalIdx = sh->terminalByBegin[selfIdx];
  if (terminalIdx < 0) return;
  const SReplayEvent *terminal = &sh->session->events[terminalIdx];
  if (affected ? !terminal->affectedRowsSet : !terminal->fetchRowsSet) return;
  int64_t recorded = affected ? terminal->affectedRows : terminal->fetchRows;
  if (recorded == actual) return;
  ++(*mismatches);
  if (sh->opts->verbose) {
    (void)fprintf(stderr, "%s count mismatch: api %u recorded %" PRId64 ", replay %" PRId64 "\n",
                  affected ? "affected" : "fetch", apiType, recorded, actual);
  }
}

static void replayDropStmt2(SReplayShared *sh, const SReplayEvent *e, TAOS_STMT2 *stmt2) {
  replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
  (void)taos_stmt2_close(stmt2);
}

static int32_t replayStmt2BindNoColumns(SReplayShared *sh, const SReplayEvent *e, TAOS_STMT2 *stmt2) {
  if (!e->bindTableCountSet || e->bindTableCount <= 0 || e->bindTableCount > REPLAY_MAX_BIND_COLUMNS ||
      !e->bindColIdxSet) return -1;

  int32_t expectedCols = 0;
  int32_t expectedTags = 0;
  bool shapeKnown = replayLaneGetStmt2Shape(sh, e->sessionIdx, e->stmtId, &expectedCols, &expectedTags);
  if (!shapeKnown && e->stmt2TagsLen != 0) return -1;
  if (expectedTags > REPLAY_MAX_BIND_COLUMNS / e->bindTableCount) return -1;

  SWlrSmlLine names[REPLAY_MAX_BIND_COLUMNS];
  int32_t nameCount = 0;
  bool hasNames = e->tbnameList != NULL && e->tbnameListLen > 0;
  if (hasNames && (wlrDecodeStmt2Names(e->tbnameList, e->tbnameListLen, names,
                                    REPLAY_MAX_BIND_COLUMNS, &nameCount) != WLR_DECODE_OK ||
                   nameCount != e->bindTableCount)) return -1;
  if (!hasNames && e->bindTableCount != 1) return -1;

  TAOS_STMT2_BIND *tagGroups[REPLAY_MAX_BIND_COLUMNS];
  TAOS_STMT2_BIND tagCells[REPLAY_MAX_BIND_COLUMNS];
  int32_t *tagLengths[REPLAY_MAX_BIND_COLUMNS];
  int32_t tagCount = replayBuildStmt2Tags(e->stmt2Tags, e->stmt2TagsLen, e->bindTableCount,
                                           expectedTags, tagGroups, tagCells, tagLengths);
  if (tagCount < 0) return tagCount;

  char *tableNames[REPLAY_MAX_BIND_COLUMNS] = {0};
  int32_t table;
  for (table = 0; hasNames && table < e->bindTableCount; ++table) {
    if (names[table].bytes == NULL) continue;
    tableNames[table] = (char *)taosMemoryMalloc((size_t)names[table].length + 1u);
    if (tableNames[table] == NULL) break;
    memcpy(tableNames[table], names[table].bytes, names[table].length);
    tableNames[table][names[table].length] = 0;
  }

  int32_t code = REPLAY_BIND_OOM;
  if (!hasNames || table == e->bindTableCount) {
    TAOS_STMT2_BINDV bindv = {
        .count = e->bindTableCount,
        .tbnames = hasNames ? tableNames : NULL,
        .tags = tagCount > 0 ? tagGroups : NULL,
        .bind_cols = NULL,
    };
    replayLaneDropStmtResults(sh, e->sessionIdx, e->stmtId);
    code = taos_stmt2_bind_param(stmt2, &bindv, e->bindColIdx);
    if (code == TSDB_CODE_SUCCESS && !shapeKnown) {
      int32_t colCount = 0;
      int32_t tagCount = 0;
      bool known = replayStmt2FieldCounts(stmt2, &colCount, &tagCount);
      replayLaneSetStmt2Shape(sh, e->sessionIdx, e->stmtId, colCount, tagCount, known);
    }
  }
  for (int32_t index = 0; index < e->bindTableCount; ++index) taosMemoryFree(tableNames[index]);
  replayFreeBindLengths(tagLengths, tagCount);
  return code;
}

static int32_t replayRunEvent(SReplayShared *sh, const SReplayEvent *e, int32_t selfIdx) {
  int32_t nExec = 0, nSkip = 0, nErr = 0, nMis = 0, nCountMis = 0;
  int32_t rc = REPLAY_EXIT_OK;

  if (sh == NULL || e == NULL) return REPLAY_EXIT_INTERNAL;
  if (sh->terminalByBegin == NULL || selfIdx < 0 || selfIdx >= sh->eventTotal) {
    return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
  }

  switch (e->apiType) {
      case WLR_API_CONNECT:
      case WLR_API_CONNECT_WITH: {
        char     host[TSDB_FQDN_LEN] = {0};
        uint16_t port = 0;
        char     user[TSDB_USER_LEN] = {0};
        char     db[TSDB_DB_NAME_LEN] = {0};

        if (e->connOptListOmitted) {
          (void)fprintf(stderr,
                        "recorded connect_with omitted connection options; refusing unsafe replay\n");
          return replayFatalEvent(sh, REPLAY_EXIT_INPUT, nExec, nSkip, nErr, nMis);
        }

        if (replayResolveEndpoint(sh->opts, e->endpoint, e->endpointLen, e->port,
                                  host, sizeof(host), &port) != 0) {
          return replayFatalEvent(sh, REPLAY_EXIT_CONN, nExec, nSkip, nErr, nMis);
        }
        if (e->user != NULL && e->userLen >= sizeof(user)) {
          (void)fprintf(stderr, "recorded user name too long (%u bytes); refusing connect\n", e->userLen);
          return replayFatalEvent(sh, REPLAY_EXIT_CONN, nExec, nSkip, nErr, nMis);
        }
        if (e->db != NULL && e->dbLen >= sizeof(db)) {
          (void)fprintf(stderr, "recorded db name too long (%u bytes); refusing connect\n", e->dbLen);
          return replayFatalEvent(sh, REPLAY_EXIT_CONN, nExec, nSkip, nErr, nMis);
        }
        if (e->user != NULL) memcpy(user, e->user, e->userLen);
        if (e->db != NULL) memcpy(db, e->db, e->dbLen);

        const char *pass = (e->user != NULL) ? replayFindPassword(sh->opts, e->user, e->userLen) : NULL;
        TAOS *taos = taos_connect(host[0] ? host : NULL, user[0] ? user : "root", pass != NULL ? pass : "",
                                  db[0] ? db : NULL, port);
        if (taos == NULL) {
          (void)fprintf(stderr, "connect to %s:%u as %s failed: %s\n", host, port, user, taos_errstr(NULL));
          return replayFatalEvent(sh, REPLAY_EXIT_CONN, nExec, nSkip, nErr, nMis);
        }
        if (e->apiType == WLR_API_CONNECT_WITH && e->connOptList != NULL) {
          int32_t optCount = 0;
          EWlrDecodeResult decodeResult =
              wlrDecodeConnOptList(e->connOptList, e->connOptListLen, NULL, 0, &optCount);
          SWlrConnOpt *connOpts = NULL;
          if (decodeResult == WLR_DECODE_OK && optCount > 0) {
            connOpts = (SWlrConnOpt *)taosMemoryCalloc((size_t)optCount, sizeof(*connOpts));
            if (connOpts == NULL) decodeResult = WLR_DECODE_NO_SPACE;
          }
          if (decodeResult == WLR_DECODE_OK && optCount > 0) {
            decodeResult = wlrDecodeConnOptList(e->connOptList, e->connOptListLen,
                                                connOpts, optCount, &optCount);
          }
          for (int32_t optIdx = 0; decodeResult == WLR_DECODE_OK && optIdx < optCount; ++optIdx) {
            SWlrConnOpt *opt = &connOpts[optIdx];
            char *value = NULL;
            if (opt->type < TSDB_OPTION_CONNECTION_CLEAR || opt->type >= TSDB_MAX_OPTIONS_CONNECTION) {
              decodeResult = WLR_DECODE_BAD_LENGTH;
              break;
            }
            if (opt->hasValue) {
              value = (char *)taosMemoryMalloc((size_t)opt->valueLen + 1);
              if (value == NULL) {
                decodeResult = WLR_DECODE_NO_SPACE;
                break;
              }
              if (opt->valueLen > 0) memcpy(value, opt->value, opt->valueLen);
              value[opt->valueLen] = 0;
            }
            int32_t optionCode = taos_options_connection(
                taos, (TSDB_OPTION_CONNECTION)opt->type, value);
            taosMemoryFree(value);
            if (optionCode != TSDB_CODE_SUCCESS) {
              decodeResult = WLR_DECODE_BAD_LENGTH;
              break;
            }
          }
          taosMemoryFree(connOpts);
          if (decodeResult != WLR_DECODE_OK) {
            (void)fprintf(stderr, "replay: failed to restore recorded connection options\n");
            taos_close(taos);
            return replayFatalEvent(sh, REPLAY_EXIT_CONN, nExec, nSkip, nErr, nMis);
          }
        }
        /* Keyed by the RETURN event's conn ID, which is where the recorder assigned
         * it; the BEGIN does not have one yet. */
        uint64_t recId = e->connId;
        if (recId == 0) {
          int32_t terminalIdx = sh->terminalByBegin[selfIdx];
          if (terminalIdx >= 0) recId = sh->session->events[terminalIdx].connId;
        }
        if (recId == 0) { taos_close(taos); ++nSkip; break; }
        if (replayLaneAddConn(sh, e->sessionIdx, recId, taos)) ++nExec;
        else ++nSkip;
        break;
      }

      case WLR_API_CLOSE: {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos != NULL) {
          /* Drop the public connection handle; ordinary query results remain mapped
           * until their recorded FREE_RESULT or final replay cleanup. */
          EReplayConnDropResult drop = replayLaneDropConn(sh, e->sessionIdx, e->connId);
          if (drop == REPLAY_CONN_DROP_REMOVED) {
            taos_close(taos);
            ++nExec;
          } else if (drop == REPLAY_CONN_DROP_DEFERRED) {
            /* The watchdog owns a snapshot containing this handle. Teardown closes
             * the still-mapped connection after cancellation releases ownership. */
            ++nExec;
          } else {
            ++nSkip;
          }
        } else {
          ++nSkip;
        }
        break;
      }

      case WLR_API_OPTIONS_CONNECTION: {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        /* The name is not passed to the API, but its absence is the recorder's
         * metadata-only marker. In that case an absent value may mean "omitted",
         * not "clear"; replaying it as NULL would silently change semantics. */
        if (taos == NULL || !e->connOptTypeSet || e->connOptName == NULL || e->connOptNameLen == 0 ||
            e->connOptType < TSDB_OPTION_CONNECTION_CLEAR ||
            e->connOptType >= TSDB_MAX_OPTIONS_CONNECTION) {
          ++nSkip;
          break;
        }
        char *value = NULL;
        if (e->connOptValueSet) {
          value = (char *)taosMemoryMalloc((size_t)e->connOptValueLen + 1u);
          if (value == NULL) {
            return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
          }
          if (e->connOptValueLen > 0) memcpy(value, e->connOptValue, e->connOptValueLen);
          value[e->connOptValueLen] = 0;
        }
        int32_t code = taos_options_connection(
            taos, (TSDB_OPTION_CONNECTION)e->connOptType, value);
        taosMemoryFree(value);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        ++nExec;
        break;
      }

      case WLR_API_TXN_BEGIN:
      case WLR_API_TXN_COMMIT:
      case WLR_API_TXN_ROLLBACK: {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos == NULL) {
          ++nSkip;
          break;
        }
        int32_t code = e->apiType == WLR_API_TXN_BEGIN
                           ? taos_txn_begin(taos)
                           : (e->apiType == WLR_API_TXN_COMMIT ? taos_txn_commit(taos)
                                                               : taos_txn_rollback(taos));
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        ++nExec;
        break;
      }

      case WLR_API_RESET_CURRENT_DB: {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos == NULL) {
          ++nSkip;
          break;
        }
        taos_reset_current_db(taos);
        ++nExec;
        break;
      }

      case WLR_API_KILL_QUERY: {
        /*
         * Reissued on the mapped connection. This cancels whatever that connection is
         * running on the target, which is the recorded intent -- the recording cannot
         * say which query it hit, because the API itself does not take one.
         */
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos == NULL) {
          ++nSkip;
          break;
        }
        taos_kill_query(taos);
        ++nExec;
        break;
      }

      case WLR_API_STOP_QUERY: {
        /* Bound to a result, not a connection. A stop against a result this replay
         * never created cannot be reproduced, so it is a skip rather than a no-op. */
        TAOS_RES *res = replayLaneAcquireResult(sh, e->sessionIdx, e->resultId);
        if (res == NULL) {
          ++nSkip;
          break;
        }
        taos_stop_query(res);
        replayLaneReleaseResult(sh, e->sessionIdx, e->resultId, res);
        ++nExec;
        break;
      }

      case WLR_API_SELECT_DB: {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos == NULL) {
          ++nSkip;
          break;
        }
        if (e->db == NULL || e->dbLen == 0) {
          return replayFatalEvent(sh, REPLAY_EXIT_INPUT, nExec, nSkip, nErr, nMis);
        }
        char *db = (char *)taosMemoryMalloc((size_t)e->dbLen + 1u);
        if (db == NULL) return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        memcpy(db, e->db, e->dbLen);
        db[e->dbLen] = 0;
        terrno = 0;
        int32_t code = taos_select_db(taos, db);
        taosMemoryFree(db);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        int32_t terminalIdx = sh->terminalByBegin[selfIdx];
        int32_t recordedCode = terminalIdx >= 0 ? sh->session->events[terminalIdx].retCode : 0;
        if (code != recordedCode) {
          /* The selected database determines every later statement's meaning. Abort
           * instead of closing here: another lane may still be using this shared raw
           * handle, and normal teardown closes it after all lanes have stopped. */
          replayLaneAbort(sh, REPLAY_EXIT_PARTIAL);
        }
        ++nExec;
        break;
      }

      case WLR_API_QUERY:
      case WLR_API_QUERY_WITH_REQID:
      case WLR_API_QUERY_A:            /* reissued synchronously: see the report caveat */
      case WLR_API_QUERY_A_WITH_REQID: /* likewise, but carrying its recorded reqid */
      {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos == NULL || e->sql == NULL || e->sqlLen == 0) {
          ++nSkip;
          break;
        }
        if (e->sqlLen > WLR_MAX_FRAME_PAYLOAD) {
          (void)fprintf(stderr, "replay: query eventSeq %" PRIu64 " exceeds the %u-byte frame limit; skipping\n",
                        e->eventSeq, (uint32_t)WLR_MAX_FRAME_PAYLOAD);
          ++nSkip;
          break;
        }
        char *sql = (char *)taosMemoryMalloc((size_t)e->sqlLen + 1u);
        if (sql == NULL) return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        memcpy(sql, e->sql, e->sqlLen);
        sql[e->sqlLen] = 0;

        /*
         * A recorded reqid is reissued verbatim rather than letting the driver mint a
         * fresh one. FS 4.5 records it precisely so an application's own logs still
         * correlate with server traces after a replay; regenerating it would break that
         * correspondence for exactly the diagnostic case replay exists to serve.
         */
        bool      withReqid = (e->apiType == WLR_API_QUERY_WITH_REQID ||
                          e->apiType == WLR_API_QUERY_A_WITH_REQID) &&
                         e->reqid != 0;
        terrno = 0;
        TAOS_RES *res = withReqid ? taos_query_with_reqid(taos, sql, e->reqid) : taos_query(taos, sql);
        /* The driver normally returns a result carrying an errno even on failure, but an
         * allocation failure inside the entry point can yield NULL, and taos_errno(NULL)
         * is not defined. Count it as an error rather than crashing the lane -- a lane
         * that dies without publishing hangs every lane waiting on its events. */
        if (res == NULL) {
          int32_t queryCode = terrno;
          (void)fprintf(stderr,
                        "replay: query eventSeq %" PRIu64 " (API %u) returned no result: %s\n",
                        e->eventSeq, (uint32_t)e->apiType, tstrerror(queryCode));
          replayCompareOutcome(sh, selfIdx, e->apiType, queryCode, &nErr, &nMis);
          ++nExec;
          taosMemoryFree(sql);
          break;
        }
        int32_t   code = taos_errno(res);
        replayCompareCount(sh, selfIdx, e->apiType, taos_affected_rows64(res), true, &nCountMis);

        /* Compare against the recorded outcome. Row values are not compared because
         * they were never recorded -- only codes and counts are (DS 6.23). */
        int32_t recordedCode = 0;
        int32_t terminalIdx = sh->terminalByBegin[selfIdx];
        if (terminalIdx >= 0) recordedCode = sh->session->events[terminalIdx].retCode;
        /*
         * Counted only when it DIFFERS from what was recorded. A statement that failed
         * during recording and fails the same way on replay is a faithful reproduction,
         * not a replay error -- and workloads legitimately contain failing statements
         * ("drop database if exists" against an absent database, duplicate-key inserts,
         * permission probes). Counting those as errors made every such replay exit 4
         * and report a problem that did not exist.
         */
        if (code != 0 && recordedCode == 0) ++nErr;
        if (code != recordedCode) {
          ++nMis;
          if (sh->opts->verbose) {
            (void)fprintf(stderr, "code mismatch: recorded %d, replay %d, sql: %.*s\n", recordedCode,
                          code, (int32_t)e->sqlLen, e->sql);
          }
        }

        bool resultRegistered = false;
        if (e->resultId != 0) {
          /* Ownership transfers even when registration returns false: a full map frees
           * res internally, so this branch must never free it again. */
          resultRegistered = replayLaneAddResult(sh, e->sessionIdx, e->resultId, res, e->connId);
        } else {
          /* Find the result ID the recorder assigned on RETURN. */
          uint64_t rid = 0;
          if (terminalIdx >= 0) rid = sh->session->events[terminalIdx].resultId;
          if (rid != 0) {
            /* Same ownership contract as above: false means already freed on overflow. */
            resultRegistered = replayLaneAddResult(sh, e->sessionIdx, rid, res, e->connId);
          } else {
            taos_free_result(res);
            resultRegistered = true;
          }
        }
        if (resultRegistered) ++nExec;
        else ++nSkip;
        taosMemoryFree(sql);
        break;
      }

      case WLR_API_VALIDATE_SQL: {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos == NULL || e->sql == NULL || e->sqlLen == 0) {
          ++nSkip;
          break;
        }
        if (e->sqlLen > WLR_MAX_FRAME_PAYLOAD) {
          (void)fprintf(stderr,
                        "replay: validate_sql eventSeq %" PRIu64
                        " exceeds the %u-byte frame limit; skipping\n",
                        e->eventSeq, (uint32_t)WLR_MAX_FRAME_PAYLOAD);
          ++nSkip;
          break;
        }
        char *sql = (char *)taosMemoryMalloc((size_t)e->sqlLen + 1u);
        if (sql == NULL) return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        memcpy(sql, e->sql, e->sqlLen);
        sql[e->sqlLen] = 0;

        int32_t code = taos_validate_sql(taos, sql);
        int32_t recordedCode = 0;
        int32_t terminalIdx = sh->terminalByBegin[selfIdx];
        if (terminalIdx >= 0) recordedCode = sh->session->events[terminalIdx].retCode;
        if (code != 0 && recordedCode == 0) ++nErr;
        if (code != recordedCode) {
          ++nMis;
          if (sh->opts->verbose) {
            (void)fprintf(stderr, "code mismatch: recorded %d, replay %d, validate_sql: %.*s\n",
                          recordedCode, code, (int32_t)e->sqlLen, e->sql);
          }
        }
        taosMemoryFree(sql);
        ++nExec;
        break;
      }

      case WLR_API_FETCH_ROW: {
        /* The recording holds no row data, so fetching is about exercising the same
         * call sequence and comparing counts, not reproducing values (DS 6.23). */
        /* Looked up without removing: a fetch does not consume the result, unlike
         * free_result. */
        TAOS_RES *res = replayLaneAcquireResult(sh, e->sessionIdx, e->resultId);
        if (res == NULL) {
          ++nSkip;
          break;
        }
        TAOS_ROW row = taos_fetch_row(res);
        int32_t code = taos_errno(res);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        replayCompareCount(sh, selfIdx, e->apiType, row != NULL ? 1 : 0, false, &nCountMis);
        replayLaneReleaseResult(sh, e->sessionIdx, e->resultId, res);
        ++nExec;
        break;
      }

      case WLR_API_FETCH_BLOCK_S:
      case WLR_API_FETCH_BLOCK:
      case WLR_API_FETCH_RAW_BLOCK: {
        TAOS_RES *res = replayLaneAcquireResult(sh, e->sessionIdx, e->resultId);
        if (res == NULL) {
          ++nSkip;
          break;
        }
        int32_t rows = 0;
        if (e->apiType == WLR_API_FETCH_RAW_BLOCK) {
          void *pData = NULL;
          int32_t code = taos_fetch_raw_block(res, &rows, &pData);
          replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        } else {
          TAOS_ROW row = NULL;
          int32_t code = taos_fetch_block_s(res, &rows, &row);
          replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        }
        replayCompareCount(sh, selfIdx, e->apiType, rows, false, &nCountMis);
        replayLaneReleaseResult(sh, e->sessionIdx, e->resultId, res);
        ++nExec;
        break;
      }

      case WLR_API_FETCH_ROWS_A:
      case WLR_API_FETCH_RAW_BLOCK_A: {
        TAOS_RES *res = replayLaneAcquireResult(sh, e->sessionIdx, e->resultId);
        if (res == NULL) {
          ++nSkip;
          break;
        }
        int32_t rows = 0;
        int32_t code = replayAsyncFetch(res, e->apiType == WLR_API_FETCH_RAW_BLOCK_A, &rows);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        replayCompareCount(sh, selfIdx, e->apiType, rows, false, &nCountMis);
        replayLaneReleaseResult(sh, e->sessionIdx, e->resultId, res);
        ++nExec;
        break;
      }

      case WLR_API_FREE_RESULT: {
        if (replayLaneRetireResult(sh, e->sessionIdx, e->resultId)) {
          ++nExec;
        } else {
          /* Unknown or already retired result. */
          ++nSkip;
        }
        break;
      }

      /*
       * All ten schemaless variants land here.
       *
       * The family differs only in which of ttl / reqid / tbnameKey the caller supplied,
       * and the recorder captures each of those when present. So rather than one arm per
       * entry point, this reissues through the widest variant and passes the recorded
       * values -- which is exactly what the thinner entry points do inside the client.
       * Passing 0 ttl, 0 reqid and a NULL tbname key reproduces the narrow forms.
       */
      case WLR_API_SML_INSERT:
      case WLR_API_SML_INSERT_WITH_REQID:
      case WLR_API_SML_INSERT_TTL:
      case WLR_API_SML_INSERT_TTL_WITH_REQID:
      case WLR_API_SML_INSERT_TTL_WITH_REQID_TBNAME_KEY:
      case WLR_API_SML_INSERT_RAW:
      case WLR_API_SML_INSERT_RAW_WITH_REQID:
      case WLR_API_SML_INSERT_RAW_TTL:
      case WLR_API_SML_INSERT_RAW_TTL_WITH_REQID:
      case WLR_API_SML_INSERT_RAW_TTL_WITH_REQID_TBNAME_KEY: {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos == NULL || e->smlPayload == NULL || e->smlPayloadLen == 0) {
          ++nSkip;
          break;
        }

        /* NUL-terminated copy of the recorded key, since taos.h takes a C string. */
        char  tbnameKeyBuf[TSDB_COL_NAME_LEN] = {0};
        char *tbnameKey = NULL;
        if (e->smlTbnameKeySet && e->smlTbnameKeyLen >= sizeof(tbnameKeyBuf)) {
          (void)fprintf(stderr,
                        "replay: schemaless table-name key is %u bytes (max %zu); skipping\n",
                        e->smlTbnameKeyLen, sizeof(tbnameKeyBuf) - 1);
          ++nSkip;
          break;
        }
        if (e->smlTbnameKeySet) {
          if (e->smlTbnameKeyLen > 0) memcpy(tbnameKeyBuf, e->smlTbnameKey, e->smlTbnameKeyLen);
          tbnameKey = tbnameKeyBuf;
        }

        bool isRaw = (e->apiType == WLR_API_SML_INSERT_RAW ||
                      e->apiType == WLR_API_SML_INSERT_RAW_WITH_REQID ||
                      e->apiType == WLR_API_SML_INSERT_RAW_TTL ||
                      e->apiType == WLR_API_SML_INSERT_RAW_TTL_WITH_REQID ||
                      e->apiType == WLR_API_SML_INSERT_RAW_TTL_WITH_REQID_TBNAME_KEY);
        int32_t replayCode = 0;
        bool resultRegistered = true;

        /*
         * Reissued through the same API shape it was recorded from, with the same
         * protocol and precision. Rewriting schemaless as INSERT SQL would change what
         * the server parses and defeat the point of replaying it (DS 6.22).
         */
        if (isRaw) {
          if (e->smlPayloadLen > WLR_MAX_FRAME_PAYLOAD) {
            (void)fprintf(stderr, "replay: SML eventSeq %" PRIu64 " exceeds the %u-byte frame limit; skipping\n",
                          e->eventSeq, (uint32_t)WLR_MAX_FRAME_PAYLOAD);
            ++nSkip;
            break;
          }
          char *raw = (char *)taosMemoryMalloc((size_t)e->smlPayloadLen + 1u);
          if (raw == NULL)
            return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
          memcpy(raw, e->smlPayload, e->smlPayloadLen);
          raw[e->smlPayloadLen] = 0;
          int32_t   totalRows = 0;
          terrno = 0;
          TAOS_RES *r = taos_schemaless_insert_raw_ttl_with_reqid_tbname_key(
              taos, raw, (int32_t)e->smlPayloadLen, &totalRows, e->smlProtocol, e->smlPrecision,
              e->smlTtl, e->reqid, tbnameKey);
          replayCode = r == NULL ? terrno : taos_errno(r);
          if (r != NULL) {
            replayCompareCount(sh, selfIdx, e->apiType, taos_affected_rows64(r), true, &nCountMis);
            int32_t terminalIdx = sh->terminalByBegin[selfIdx];
            uint64_t resultId = terminalIdx >= 0 ? sh->session->events[terminalIdx].resultId : 0;
            if (resultId != 0) {
              resultRegistered = replayLaneAddResult(sh, e->sessionIdx, resultId, r, e->connId);
            } else {
              taos_free_result(r);
            }
          }
          taosMemoryFree(raw);
        } else {
          /* Line form: the payload holds count + per-entry lengths + bytes, so the
           * original char*[] is rebuilt rather than guessed at (DS 6.10). */
          int32_t     lineCount = 0;
          EWlrDecodeResult lineDecode =
              wlrDecodeSmlLines(e->smlPayload, e->smlPayloadLen, NULL, 0, &lineCount);
          if (lineDecode != WLR_DECODE_OK || lineCount <= 0) {
            (void)fprintf(stderr, "replay: malformed SML line payload; skipping\n");
            ++nSkip;
            break;
          }
          SWlrSmlLine *lines = (SWlrSmlLine *)taosMemoryMalloc((size_t)lineCount * sizeof(*lines));
          if (lines == NULL) return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
          lineDecode = wlrDecodeSmlLines(e->smlPayload, e->smlPayloadLen, lines, lineCount, &lineCount);
          if (lineDecode != WLR_DECODE_OK) {
            taosMemoryFree(lines);
            ++nSkip;
            break;
          }
          char **argv = (char **)taosMemoryCalloc((size_t)lineCount, sizeof(char *));
          if (argv == NULL) {
            taosMemoryFree(lines);
            return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
          }
          int32_t li;
          bool    ok = true;
          for (li = 0; li < lineCount; ++li) {
            argv[li] = (char *)taosMemoryMalloc((size_t)lines[li].length + 1u);
            if (argv[li] == NULL) {
              ok = false;
              break;
            }
            if (lines[li].length > 0) memcpy(argv[li], lines[li].bytes, lines[li].length);
            argv[li][lines[li].length] = 0;
          }
          if (ok) {
            terrno = 0;
            TAOS_RES *r = taos_schemaless_insert_ttl_with_reqid_tbname_key(
                taos, argv, lineCount, e->smlProtocol, e->smlPrecision, e->smlTtl, e->reqid,
                tbnameKey);
            replayCode = r == NULL ? terrno : taos_errno(r);
            if (r != NULL) {
              replayCompareCount(sh, selfIdx, e->apiType, taos_affected_rows64(r), true, &nCountMis);
              int32_t terminalIdx = sh->terminalByBegin[selfIdx];
              uint64_t resultId = terminalIdx >= 0 ? sh->session->events[terminalIdx].resultId : 0;
              if (resultId != 0) {
                resultRegistered = replayLaneAddResult(sh, e->sessionIdx, resultId, r, e->connId);
              } else {
                taos_free_result(r);
              }
            }
          }
          for (li = 0; li < lineCount; ++li) {
            if (argv[li] != NULL) taosMemoryFree(argv[li]);
          }
          taosMemoryFree(argv);
          taosMemoryFree(lines);
          if (!ok) return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        }
        replayCompareOutcome(sh, selfIdx, e->apiType, replayCode, &nErr, &nMis);
        if (resultRegistered) ++nExec;
        else ++nSkip;
        break;
      }

      case WLR_API_STMT_INIT:
      case WLR_API_STMT_INIT_WITH_REQID:
      case WLR_API_STMT_INIT_WITH_OPTIONS: {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos == NULL || e->stmtId == 0) {
          ++nSkip;
          break;
        }
        TAOS_STMT *stmt = NULL;
        terrno = 0;
        if (e->apiType == WLR_API_STMT_INIT_WITH_OPTIONS) {
          TAOS_STMT_OPTIONS opt = {0};
          opt.reqId = e->reqid;
          opt.singleStbInsert = (e->stmtInitOptions & WLR_STMT_OPT_SINGLE_STB_INSERT) != 0;
          opt.singleTableBindOnce = (e->stmtInitOptions & WLR_STMT_OPT_SINGLE_TABLE_BIND_ONCE) != 0;
          if (!e->stmtInitOptionsSet) {
            (void)fprintf(stderr, "replay: STMT_INIT_WITH_OPTIONS flags absent; using defaults\n");
          }
          stmt = taos_stmt_init_with_options(taos, &opt);
        } else if (e->apiType == WLR_API_STMT_INIT_WITH_REQID && e->reqid != 0) {
          stmt = taos_stmt_init_with_reqid(taos, e->reqid);
        } else {
          stmt = taos_stmt_init(taos);
        }
        if (stmt == NULL) {
          replayCompareOutcome(sh, selfIdx, e->apiType, terrno, &nErr, &nMis);
          ++nSkip;
          break;
        }
        replayCompareOutcome(sh, selfIdx, e->apiType, 0, &nErr, &nMis);
        if (replayLaneAddStmt(sh, e->sessionIdx, e->stmtId, e->connId, stmt, NULL)) ++nExec;
        else ++nSkip;
        break;
      }

      case WLR_API_STMT_PREPARE: {
        TAOS_STMT *stmt = replayLaneFindStmt(sh, e->sessionIdx, e->stmtId);
        if (e->sql == NULL || e->sqlLen == 0) {
          if (stmt != NULL) {
            replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
            (void)taos_stmt_close(stmt);
          }
          ++nSkip;
          break;
        }
        if (stmt == NULL) {
          ++nSkip;
          break;
        }
        char *sql = (char *)taosMemoryMalloc((size_t)e->sqlLen + 1u);
        if (sql == NULL) return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        memcpy(sql, e->sql, e->sqlLen);
        sql[e->sqlLen] = 0;
        replayLaneDropStmtResults(sh, e->sessionIdx, e->stmtId);
        replayLaneSetStmtShape(sh, e->sessionIdx, e->stmtId, 0, false);
        int32_t code = taos_stmt_prepare(stmt, sql, e->sqlLen);
        taosMemoryFree(sql);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        ++nExec;
        break;
      }

      case WLR_API_STMT_SET_TBNAME:
      case WLR_API_STMT_SET_SUB_TBNAME: {
        TAOS_STMT *stmt = replayLaneFindStmt(sh, e->sessionIdx, e->stmtId);
        if (stmt == NULL || e->tbname == NULL || e->tbnameLen == 0) {
          ++nSkip;
          break;
        }
        char name[REPLAY_TBNAME_BUFFER_SIZE] = {0};
        if (e->tbnameLen >= sizeof(name)) {
          (void)fprintf(stderr, "replay: table name of %u bytes exceeds %zu-byte buffer; skipping\n",
                        e->tbnameLen, sizeof(name) - 1);
          ++nSkip;
          break;
        }
        memcpy(name, e->tbname, e->tbnameLen);
        int32_t code = e->apiType == WLR_API_STMT_SET_SUB_TBNAME
                           ? taos_stmt_set_sub_tbname(stmt, name)
                           : taos_stmt_set_tbname(stmt, name);
        if (code == TSDB_CODE_SUCCESS) replayLaneSetStmtShape(sh, e->sessionIdx, e->stmtId, 0, false);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        ++nExec;
        break;
      }

      case WLR_API_STMT_SET_TAGS:
      case WLR_API_STMT_SET_TBNAME_TAGS: {
        (void)fprintf(stderr, "replay: legacy STMT tag bind shape cannot be verified; skipping\n");
        TAOS_STMT *stmt = replayLaneFindStmt(sh, e->sessionIdx, e->stmtId);
        if (stmt != NULL) {
          replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
          (void)taos_stmt_close(stmt);
        }
        ++nSkip;
        break;
      }

      case WLR_API_STMT_BIND_PARAM:
      case WLR_API_STMT_BIND_PARAM_BATCH: {
        TAOS_STMT      *stmt = replayLaneFindStmt(sh, e->sessionIdx, e->stmtId);
        TAOS_MULTI_BIND binds[REPLAY_MAX_BIND_COLUMNS];
        int32_t        *ownedLengths[REPLAY_MAX_BIND_COLUMNS];
        int32_t         n;
        if (stmt == NULL) {
          ++nSkip;
          break;
        }
        n = replayBuildBinds(e->bindBytes, e->bindLen, binds, REPLAY_MAX_BIND_COLUMNS, true, ownedLengths);
        if (n == REPLAY_BIND_OOM) {
          return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        }
        if (n <= 0) {
          replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
          (void)taos_stmt_close(stmt);
          ++nSkip;
          break;
        }
        int32_t expectedCols = 0;
        if (!replayStmtColumnCount(sh, e->sessionIdx, e->stmtId, stmt, &expectedCols) || n != expectedCols) {
          (void)fprintf(stderr, "replay: STMT bind does not match prepared column count; skipping\n");
          replayFreeBindLengths(ownedLengths, n);
          replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
          (void)taos_stmt_close(stmt);
          ++nSkip;
          break;
        }
        int32_t bindRc = (e->apiType == WLR_API_STMT_BIND_PARAM) ? taos_stmt_bind_param(stmt, binds)
                                                                : taos_stmt_bind_param_batch(stmt, binds);
        replayFreeBindLengths(ownedLengths, n);
        replayCompareOutcome(sh, selfIdx, e->apiType, bindRc, &nErr, &nMis);
        ++nExec;
        break;
      }

      case WLR_API_STMT_BIND_SINGLE_PARAM_BATCH: {
        TAOS_STMT      *stmt = replayLaneFindStmt(sh, e->sessionIdx, e->stmtId);
        TAOS_MULTI_BIND binds[REPLAY_MAX_BIND_COLUMNS];
        int32_t        *ownedLengths[REPLAY_MAX_BIND_COLUMNS];
        int32_t         n;
        if (stmt == NULL || !e->bindColIdxSet || e->bindColIdx < 0) {
          ++nSkip;
          break;
        }
        n = replayBuildBinds(e->bindBytes, e->bindLen, binds, REPLAY_MAX_BIND_COLUMNS, true, ownedLengths);
        if (n == REPLAY_BIND_OOM) {
          return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        }
        if (n != 1) {
          if (n > 0) replayFreeBindLengths(ownedLengths, n);
          replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
          (void)taos_stmt_close(stmt);
          ++nSkip;
          break;
        }
        int32_t expectedCols = 0;
        if (!replayStmtColumnCount(sh, e->sessionIdx, e->stmtId, stmt, &expectedCols) ||
            e->bindColIdx >= expectedCols) {
          (void)fprintf(stderr, "replay: STMT bind column index exceeds prepared column count; skipping\n");
          replayFreeBindLengths(ownedLengths, n);
          replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
          (void)taos_stmt_close(stmt);
          ++nSkip;
          break;
        }
        replayLaneDropStmtResults(sh, e->sessionIdx, e->stmtId);
        int32_t code = taos_stmt_bind_single_param_batch(stmt, binds, e->bindColIdx);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        replayFreeBindLengths(ownedLengths, n);
        ++nExec;
        break;
      }

      case WLR_API_STMT_ADD_BATCH: {
        TAOS_STMT *stmt = replayLaneFindStmt(sh, e->sessionIdx, e->stmtId);
        if (stmt == NULL) {
          ++nSkip;
          break;
        }
        int32_t code = taos_stmt_add_batch(stmt);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        ++nExec;
        break;
      }

      case WLR_API_STMT_EXECUTE: {
        TAOS_STMT *stmt = replayLaneFindStmt(sh, e->sessionIdx, e->stmtId);
        if (stmt == NULL) {
          ++nSkip;
          break;
        }
        int32_t code = taos_stmt_execute(stmt);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        if (code == 0) {
          replayCompareCount(sh, selfIdx, e->apiType, taos_stmt_affected_rows_once(stmt), true,
                             &nCountMis);
        }
        ++nExec;
        break;
      }

      case WLR_API_STMT_CLOSE: {
        TAOS_STMT *stmt = replayLaneFindStmt(sh, e->sessionIdx, e->stmtId);
        if (stmt == NULL) {
          ++nSkip;
          break;
        }
        replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
        int32_t code = taos_stmt_close(stmt);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        ++nExec;
        break;
      }

      case WLR_API_STMT_USE_RESULT:
      case WLR_API_STMT2_RESULT: {
        TAOS_RES *res = NULL;
        uint64_t connId = 0;
        if (e->apiType == WLR_API_STMT_USE_RESULT) {
          TAOS_STMT *stmt = replayLaneFindStmtWithConn(sh, e->sessionIdx, e->stmtId, &connId);
          if (stmt == NULL) {
            ++nSkip;
            break;
          }
          terrno = 0;
          res = taos_stmt_use_result(stmt);
        } else {
          TAOS_STMT2 *stmt2 = replayLaneFindStmt2WithConn(sh, e->sessionIdx, e->stmtId, &connId);
          if (stmt2 == NULL) {
            ++nSkip;
            break;
          }
          terrno = 0;
          res = taos_stmt2_result(stmt2);
        }
        if (res == NULL) {
          replayCompareOutcome(sh, selfIdx, e->apiType, terrno, &nErr, &nMis);
          ++nSkip;
          break;
        }
        replayCompareOutcome(sh, selfIdx, e->apiType, taos_errno(res), &nErr, &nMis);
        int32_t terminalIdx = sh->terminalByBegin[selfIdx];
        uint64_t resultId = terminalIdx >= 0 ? sh->session->events[terminalIdx].resultId : 0;
        if (resultId != 0 && connId != 0 &&
            !replayLaneAddBorrowedResult(sh, e->sessionIdx, resultId, res, connId, e->stmtId)) {
          ++nSkip;
          break;
        }
        ++nExec;
        break;
      }

      case WLR_API_STMT2_INIT: {
        TAOS *taos = replayLaneFindConn(sh, e->sessionIdx, e->connId);
        if (taos == NULL || e->stmtId == 0) {
          ++nSkip;
          break;
        }
        TAOS_STMT2_OPTION opt;
        memset(&opt, 0, sizeof(opt));
        opt.reqid = e->reqid;
        opt.singleStbInsert = (e->stmtInitOptions & WLR_STMT_OPT_SINGLE_STB_INSERT) != 0;
        opt.singleTableBindOnce = (e->stmtInitOptions & WLR_STMT_OPT_SINGLE_TABLE_BIND_ONCE) != 0;
        if (!e->stmtInitOptionsSet) {
          (void)fprintf(stderr, "replay: STMT2_INIT options absent; using defaults\n");
        } else if ((e->stmtInitOptions & (WLR_STMT_OPT_ASYNC_CALLBACK | WLR_STMT_OPT_USERDATA)) != 0) {
          (void)fprintf(stderr,
                        "replay: STMT2_INIT callback/userdata cannot be restored; executing synchronously\n");
        }
        terrno = 0;
        TAOS_STMT2 *stmt2 = taos_stmt2_init(taos, &opt);
        if (stmt2 == NULL) {
          replayCompareOutcome(sh, selfIdx, e->apiType, terrno, &nErr, &nMis);
          ++nSkip;
          break;
        }
        replayCompareOutcome(sh, selfIdx, e->apiType, 0, &nErr, &nMis);
        if (replayLaneAddStmt(sh, e->sessionIdx, e->stmtId, e->connId, NULL, stmt2)) ++nExec;
        else ++nSkip;
        break;
      }

      case WLR_API_STMT2_PREPARE: {
        TAOS_STMT2 *stmt2 = replayLaneFindStmt2(sh, e->sessionIdx, e->stmtId);
        if (e->sql == NULL || e->sqlLen == 0) {
          if (stmt2 != NULL) {
            replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
            (void)taos_stmt2_close(stmt2);
          }
          ++nSkip;
          break;
        }
        if (stmt2 == NULL) {
          ++nSkip;
          break;
        }
        char *sql = (char *)taosMemoryMalloc((size_t)e->sqlLen + 1u);
        if (sql == NULL) return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        memcpy(sql, e->sql, e->sqlLen);
        sql[e->sqlLen] = 0;
        replayLaneDropStmtResults(sh, e->sessionIdx, e->stmtId);
        replayLaneSetStmt2Shape(sh, e->sessionIdx, e->stmtId, 0, 0, false);
        int32_t code = taos_stmt2_prepare(stmt2, sql, e->sqlLen);
        taosMemoryFree(sql);
        if (code == TSDB_CODE_SUCCESS) {
          int32_t colCount = 0;
          int32_t tagCount = 0;
          bool known = replayStmt2FieldCounts(stmt2, &colCount, &tagCount);
          replayLaneSetStmt2Shape(sh, e->sessionIdx, e->stmtId, colCount, tagCount, known);
        }
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        ++nExec;
        break;
      }

      case WLR_API_STMT2_BIND_PARAM:
      case WLR_API_STMT2_BIND_PARAM_A: {
        TAOS_STMT2     *stmt2 = replayLaneFindStmt2(sh, e->sessionIdx, e->stmtId);
        TAOS_MULTI_BIND flat[REPLAY_MAX_BIND_COLUMNS];
        int32_t        *ownedLengths[REPLAY_MAX_BIND_COLUMNS];
        int32_t         n;
        if (stmt2 == NULL) {
          ++nSkip;
          break;
        }
        if (e->apiType == WLR_API_STMT2_BIND_PARAM_A) {
          (void)fprintf(stderr,
                        "replay: asynchronous STMT2 row bind is replayed synchronously; callback timing is not reproduced\n");
        }
        n = replayBuildBinds(e->bindBytes, e->bindLen, flat, REPLAY_MAX_BIND_COLUMNS, false, ownedLengths);
        if (n == REPLAY_BIND_OOM) {
          return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        }
        if (n < 0 && e->bindBytes != NULL && e->bindLen == 4 && wlrGetU32(e->bindBytes) == 0) {
          int32_t code = replayStmt2BindNoColumns(sh, e, stmt2);
          if (code == REPLAY_BIND_OOM) {
            return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
          }
          if (code != -1) {
            replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
            ++nExec;
            break;
          }
        }
        if (n <= 0) {
          replayDropStmt2(sh, e, stmt2);
          ++nSkip;
          break;
        }

        /*
         * Rebuild BINDV's per-table nesting. The column list is flat on the wire, so
         * the table names are what restore the grouping: N tables each own
         * (n / tableCount) columns. Without the names replay could not tell which
         * columns belonged to which table, and a wrong-table insert would corrupt the
         * target while reporting success. That is why the recorder captures the table
         * names, and why a blob without them is skipped below rather than guessed at.
         */
        SWlrSmlLine names[REPLAY_MAX_BIND_COLUMNS];
        int32_t     tableCount = 0;
        EWlrDecodeResult nameDecode =
            (e->tbnameList != NULL && e->tbnameListLen > 0)
                ? wlrDecodeStmt2Names(e->tbnameList, e->tbnameListLen, names,
                                    REPLAY_MAX_BIND_COLUMNS, &tableCount)
                : WLR_DECODE_BAD_LENGTH;
        bool hasTableNames = nameDecode == WLR_DECODE_OK;
        if (!hasTableNames && e->tbnameList == NULL && e->bindTableCountSet && e->bindTableCount == 1) {
          tableCount = 1;
          nameDecode = WLR_DECODE_OK;
        }
        if (nameDecode != WLR_DECODE_OK || tableCount <= 0 || (n % tableCount) != 0 ||
            (e->bindTableCountSet && tableCount != e->bindTableCount)) {
          if (nameDecode == WLR_DECODE_NO_SPACE) {
            (void)fprintf(stderr, "replay: STMT2 bind has more than %d tables; skipping\n",
                          REPLAY_MAX_BIND_COLUMNS);
          } else {
            (void)fprintf(stderr, "replay: STMT2 bind has missing or inconsistent table names; skipping\n");
          }
          /* Recorded before table-name capture existed, or an inconsistent shape. */
          replayFreeBindLengths(ownedLengths, n);
          replayDropStmt2(sh, e, stmt2);
          ++nSkip;
          break;
        }

        int32_t expectedCols = 0;
        int32_t expectedTags = 0;
        if (!replayLaneGetStmt2Shape(sh, e->sessionIdx, e->stmtId, &expectedCols, &expectedTags) &&
            hasTableNames && e->stmt2TagsLen == 0) {
          int32_t primeCode = replayStmt2BindNoColumns(sh, e, stmt2);
          if (primeCode == REPLAY_BIND_OOM) {
            replayFreeBindLengths(ownedLengths, n);
            return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
          }
        }
        if (!replayLaneGetStmt2Shape(sh, e->sessionIdx, e->stmtId, &expectedCols, &expectedTags) ||
            n / tableCount != (e->bindColIdxSet && e->bindColIdx >= 0 ? 1 : expectedCols)) {
          (void)fprintf(stderr, "replay: STMT2 bind does not match prepared column count; skipping\n");
          replayFreeBindLengths(ownedLengths, n);
          replayDropStmt2(sh, e, stmt2);
          ++nSkip;
          break;
        }
        if (e->bindColIdxSet && e->bindColIdx >= expectedCols) {
          (void)fprintf(stderr, "replay: STMT2 bind column index exceeds prepared column count; skipping\n");
          replayFreeBindLengths(ownedLengths, n);
          replayDropStmt2(sh, e, stmt2);
          ++nSkip;
          break;
        }

        TAOS_STMT2_BIND *tagGroups[REPLAY_MAX_BIND_COLUMNS];
        TAOS_STMT2_BIND tagCells[REPLAY_MAX_BIND_COLUMNS];
        int32_t *tagLengths[REPLAY_MAX_BIND_COLUMNS];
        int32_t tagCount = replayBuildStmt2Tags(e->stmt2Tags, e->stmt2TagsLen, tableCount, expectedTags,
                                                 tagGroups, tagCells, tagLengths);
        if (tagCount < 0) {
          replayFreeBindLengths(ownedLengths, n);
          if (tagCount == REPLAY_BIND_OOM) {
            return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
          }
          (void)fprintf(stderr, "replay: STMT2 tag bind does not match prepared tag count; skipping\n");
          replayDropStmt2(sh, e, stmt2);
          ++nSkip;
          break;
        }

        int32_t colsPer = n / tableCount;
        char  **tbnames = (char **)taosMemoryCalloc((size_t)tableCount, sizeof(char *));
        TAOS_STMT2_BIND **colGroups =
            (TAOS_STMT2_BIND **)taosMemoryCalloc((size_t)tableCount, sizeof(TAOS_STMT2_BIND *));
        TAOS_STMT2_BIND *cells =
            (TAOS_STMT2_BIND *)taosMemoryCalloc((size_t)n, sizeof(TAOS_STMT2_BIND));
        if (tbnames == NULL || colGroups == NULL || cells == NULL) {
          taosMemoryFree(tbnames);
          taosMemoryFree(colGroups);
          taosMemoryFree(cells);
          replayFreeBindLengths(ownedLengths, n);
          replayFreeBindLengths(tagLengths, tagCount);
          return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        }

        bool built = true;
        bool allocationFailed = false;
        int32_t ti;
        for (ti = 0; ti < tableCount && built; ++ti) {
          if (hasTableNames && names[ti].bytes != NULL) {
            tbnames[ti] = (char *)taosMemoryMalloc((size_t)names[ti].length + 1u);
            if (tbnames[ti] == NULL) {
              allocationFailed = true;
              built = false;
              break;
            }
            if (names[ti].length > 0) memcpy(tbnames[ti], names[ti].bytes, names[ti].length);
            tbnames[ti][names[ti].length] = 0;
          }

          int32_t ci;
          if (flat[ti * colsPer].buffer_type == 0 && flat[ti * colsPer].num == 0) {
            for (ci = 1; ci < colsPer; ++ci) {
              if (flat[ti * colsPer + ci].buffer_type != 0 ||
                  flat[ti * colsPer + ci].num != 0) built = false;
            }
            continue;
          }
          for (ci = 0; ci < colsPer; ++ci) {
            TAOS_MULTI_BIND *src = &flat[ti * colsPer + ci];
            if (src->buffer_type == 0 && src->num == 0) {
              built = false;
              break;
            }
            TAOS_STMT2_BIND *dst = &cells[ti * colsPer + ci];
            /* TAOS_STMT2_BIND has no buffer_length: the type carries the width, which
             * is why replayBuildBinds derived it and it is dropped here. */
            dst->buffer_type = src->buffer_type;
            dst->buffer = src->buffer;
            dst->length = src->length;
            dst->is_null = src->is_null;
            dst->num = src->num;
          }
          colGroups[ti] = &cells[ti * colsPer];
        }

        if (built) {
          TAOS_STMT2_BINDV bindv;
          bindv.count = tableCount;
          bindv.tbnames = tbnames;
          bindv.tags = tagGroups;
          bindv.bind_cols = colGroups;
          int32_t colIdx = e->bindColIdxSet ? e->bindColIdx : -1;
          replayLaneDropStmtResults(sh, e->sessionIdx, e->stmtId);
          int32_t code = taos_stmt2_bind_param(stmt2, &bindv, colIdx);
          replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
          ++nExec;
        }

        /* tbnames is calloc-initialized, so this full-range cleanup is safe even
         * when construction stopped part-way through the table list. */
        for (ti = 0; ti < tableCount; ++ti) {
          if (tbnames[ti] != NULL) taosMemoryFree(tbnames[ti]);
        }
        taosMemoryFree(tbnames);
        taosMemoryFree(colGroups);
        taosMemoryFree(cells);
        replayFreeBindLengths(ownedLengths, n);
        replayFreeBindLengths(tagLengths, tagCount);
        if (allocationFailed) {
          return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        }
        break;
      }

      case WLR_API_STMT2_BIND_PARAM_COLUMN:
      case WLR_API_STMT2_BIND_PARAM_COLUMN_A: {
        TAOS_STMT2     *stmt2 = replayLaneFindStmt2(sh, e->sessionIdx, e->stmtId);
        TAOS_MULTI_BIND flat[REPLAY_MAX_BIND_COLUMNS];
        int32_t        *ownedLengths[REPLAY_MAX_BIND_COLUMNS];
        int32_t         n;
        if (stmt2 == NULL) {
          ++nSkip;
          break;
        }
        if (e->apiType == WLR_API_STMT2_BIND_PARAM_COLUMN_A) {
          (void)fprintf(stderr,
                        "replay: asynchronous STMT2 column bind is replayed synchronously; callback timing is not reproduced\n");
        }
        n = replayBuildBinds(e->bindBytes, e->bindLen, flat, REPLAY_MAX_BIND_COLUMNS, false, ownedLengths);
        if (n == REPLAY_BIND_OOM) {
          return replayFatalEvent(sh, REPLAY_EXIT_INTERNAL, nExec, nSkip, nErr, nMis);
        }
        if (n <= 0) {
          replayDropStmt2(sh, e, stmt2);
          ++nSkip;
          break;
        }
        TAOS_STMT2_COLUMN_BIND columns[REPLAY_MAX_BIND_COLUMNS];
        int32_t numRows = flat[0].num;
        bool shapeOk = numRows > 0 && numRows <= INT16_MAX;
        for (int32_t col = 0; col < n; ++col) {
          if (flat[col].num != numRows) {
            shapeOk = false;
            break;
          }
          columns[col].buffer_type = flat[col].buffer_type;
          columns[col].buffer = flat[col].buffer;
          columns[col].length = flat[col].length;
          columns[col].is_null = flat[col].is_null;
        }
        if (!shapeOk) {
          (void)fprintf(stderr, "replay: STMT2 column bind has an invalid row count or inconsistent shape; skipping\n");
          replayFreeBindLengths(ownedLengths, n);
          replayDropStmt2(sh, e, stmt2);
          ++nSkip;
          break;
        }
        TAOS_STMT2_COLUMN_BINDV bindv = {
            .num_columns = n,
            .num_rows = numRows,
            .num_tables = e->bindTableCountSet ? e->bindTableCount : 0,
            .columns = columns,
        };
        replayLaneDropStmtResults(sh, e->sessionIdx, e->stmtId);
        int32_t code = taos_stmt2_bind_param_column(stmt2, &bindv);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        replayFreeBindLengths(ownedLengths, n);
        ++nExec;
        break;
      }

      case WLR_API_STMT2_EXEC: {
        uint64_t connId = 0;
        TAOS_STMT2 *stmt2 =
            replayLaneFindStmt2WithConn(sh, e->sessionIdx, e->stmtId, &connId);
        if (stmt2 == NULL) {
          ++nSkip;
          break;
        }
        int32_t affected = 0;
        int32_t code = taos_stmt2_exec(stmt2, &affected);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        replayCompareCount(sh, selfIdx, e->apiType, affected, true, &nCountMis);
        int32_t terminalIdx = sh->terminalByBegin[selfIdx];
        if (code == 0 && terminalIdx >= 0) {
          const SReplayEvent *terminal = &sh->session->events[terminalIdx];
          if (terminal->resultId != 0 && sh->resultUsedByEvent[terminalIdx]) {
            TAOS_RES *res = taos_stmt2_result(stmt2);
            if (res == NULL || connId == 0 ||
                !replayLaneAddBorrowedResult(sh, e->sessionIdx, terminal->resultId, res, connId,
                                             e->stmtId)) {
              ++nSkip;
            }
          }
        }
        ++nExec;
        break;
      }

      case WLR_API_STMT2_CLOSE: {
        TAOS_STMT2 *stmt2 = replayLaneFindStmt2(sh, e->sessionIdx, e->stmtId);
        if (stmt2 == NULL) {
          ++nSkip;
          break;
        }
        replayLaneDropStmt(sh, e->sessionIdx, e->stmtId);
        int32_t code = taos_stmt2_close(stmt2);
        replayCompareOutcome(sh, selfIdx, e->apiType, code, &nErr, &nMis);
        ++nExec;
        break;
      }

      default:
        /*
         * Unknown or diagnostic-only API. Count it as skipped so the report never
         * overstates what replay actually ran.
         */
        ++nSkip;
        break;
  }

  replayLaneAddStat(sh, nExec, nSkip, nErr, nMis);
  if (nCountMis > 0) {
    (void)taosThreadMutexLock(&sh->statLock);
    sh->countMismatches += nCountMis;
    (void)taosThreadMutexUnlock(&sh->statLock);
  }
  return rc;
}

/* Lane thread: this thread's events, in recorded order, each at its target time. */
static void *replayLaneMain(void *param) {
  SReplayLane   *lane = (SReplayLane *)param;
  SReplayShared *sh = lane->shared;
  int32_t        i;

  for (i = 0; i < lane->eventCount; ++i) {
    const SReplayEvent *e = &sh->session->events[lane->eventIdx[i]];

    /* Another lane hit a fatal condition: publish the rest and stop, so the run
     * terminates with a report instead of every lane blocking. */
    if (replayLaneAborted(sh)) {
      int32_t k;
      for (k = i; k < lane->eventCount; ++k) {
        replayLaneAddStat(sh, 0, 1, 0, 0);
        replayLaneMarkDone(sh, lane->eventIdx[k]);
      }
      break;
    }

    /* Pace against the shared origin, so lanes stay aligned with one another. */
    replayLaneWaitUntil(sh, e->monoNs);
    /* Then honour recorded ordering: time alone lets a lane overtake a call that had
     * already returned before this one started. */
    replayLaneWaitDeps(sh, lane->eventIdx[i]);
    if (replayLaneAborted(sh)) {
      int32_t k;
      for (k = i; k < lane->eventCount; ++k) {
        replayLaneAddStat(sh, 0, 1, 0, 0);
        replayLaneMarkDone(sh, lane->eventIdx[k]);
      }
      break;
    }
    bool unfinished = sh->opts->unfinishedTimeoutSec > 0 && sh->terminalByBegin[lane->eventIdx[i]] < 0;
    if (unfinished) {
      (void)taosThreadMutexLock(&sh->depLock);
      sh->unfinishedDeadlineMs[lane->watchdogSlot] =
          taosGetMonoTimestampMs() + (int64_t)sh->opts->unfinishedTimeoutSec * 1000;
      (void)taosThreadCondBroadcast(&sh->depCond);
      (void)taosThreadMutexUnlock(&sh->depLock);
    }
    int32_t rc = replayRunEvent(sh, e, lane->eventIdx[i]);
    if (unfinished) {
      (void)taosThreadMutexLock(&sh->depLock);
      sh->unfinishedDeadlineMs[lane->watchdogSlot] = 0;
      (void)taosThreadCondBroadcast(&sh->depCond);
      (void)taosThreadMutexUnlock(&sh->depLock);
    }
    /* Published even on failure, or lanes waiting behind this index would stall. */
    replayLaneMarkDone(sh, lane->eventIdx[i]);
    if (rc != REPLAY_EXIT_OK) {
      /*
       * Fatal for the whole replay, not just this lane. A failed connect or an
       * unmappable endpoint means other lanes are either about to fail identically or
       * are already blocked waiting on an index this lane will never publish.
       *
       * Two things must happen, in this order: signal the abort so waiters wake, then
       * publish every index this lane still owns. Doing only the first leaves the
       * dependency counter short; doing only the second leaves waiters asleep until
       * their own index happens to clear.
       */
      replayLaneAbort(sh, rc);
      int32_t k;
      for (k = i + 1; k < lane->eventCount; ++k) {
        replayLaneAddStat(sh, 0, 1, 0, 0); /* skipped, not silently dropped */
        replayLaneMarkDone(sh, lane->eventIdx[k]);
      }
      break;
    }
  }
  return NULL;
}

/* Observe fatal aborts for every replay. Only incomplete recordings use the
 * unfinished-call timeout; either condition cancels active queries. */
static void *replayUnfinishedWatchdog(void *param) {
  SReplayShared  *sh = (SReplayShared *)param;
  struct timespec deadline;
  bool            timedOut = false;
  bool            waitFailed = false;
  bool            aborted = false;

  (void)taosThreadMutexLock(&sh->depLock);
  while (!sh->replayDone && sh->aborted == 0) {
    int64_t earliestMs = 0;
    for (int32_t slot = 0; slot < REPLAY_MAX_LANES; ++slot) {
      int64_t candidate = sh->unfinishedDeadlineMs[slot];
      if (candidate != 0 && (earliestMs == 0 || candidate < earliestMs)) earliestMs = candidate;
    }
    if (earliestMs == 0) {
      if (taosThreadCondWait(&sh->depCond, &sh->depLock) != 0) {
        waitFailed = true;
        break;
      }
      continue;
    }
    int64_t remainingMs = earliestMs - taosGetMonoTimestampMs();
    if (remainingMs <= 0) {
      timedOut = true;
      break;
    }
#ifdef WINDOWS
    deadline.tv_sec = remainingMs / 1000;
    deadline.tv_nsec = (remainingMs % 1000) * 1000000;
#else
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
      waitFailed = true;
      break;
    }
    deadline.tv_sec += remainingMs / 1000;
    deadline.tv_nsec += (remainingMs % 1000) * 1000000;
    if (deadline.tv_nsec >= 1000000000) {
      deadline.tv_sec++;
      deadline.tv_nsec -= 1000000000;
    }
#endif
    int32_t code = taosThreadCondTimedWait(&sh->depCond, &sh->depLock, &deadline);
    if (code == TSDB_CODE_TIMEOUT_ERROR) continue;
    if (code != 0) {
      waitFailed = true;
      break;
    }
  }
  aborted = sh->aborted != 0;
  (void)taosThreadMutexUnlock(&sh->depLock);

  if (waitFailed) {
    replayLaneAbort(sh, REPLAY_EXIT_INTERNAL);
    return NULL;
  }
  if (!timedOut && !aborted) return NULL;

  if (timedOut) {
    (void)fprintf(stderr, "unfinished-call replay timed out after %d seconds; cancelling active queries\n",
                  sh->opts->unfinishedTimeoutSec);
    replayLaneAbort(sh, REPLAY_EXIT_PARTIAL);
  }
  TAOS **connections = (TAOS **)taosMemoryMalloc(REPLAY_MAX_HANDLES * sizeof(*connections));
  SReplayResEntry *results = (SReplayResEntry *)taosMemoryMalloc(REPLAY_MAX_HANDLES * sizeof(*results));
  if (connections == NULL || results == NULL) {
    taosMemoryFree(connections);
    taosMemoryFree(results);
    (void)fprintf(stderr,
                  "unfinished-call watchdog could not allocate a handle snapshot; "
                  "active queries were not cancelled\n");
    return NULL;
  }
  for (;;) {
    (void)taosThreadMutexLock(&sh->depLock);
    bool replayDone = sh->replayDone;
    (void)taosThreadMutexUnlock(&sh->depLock);
    if (replayDone) break;

    int32_t connectionCount = 0;
    int32_t resultCount = 0;
    (void)taosThreadMutexLock(&sh->mapLock);
    sh->cancellingQueries = true;
    while (sh->connDropsInFlight > 0) {
      (void)taosThreadCondWait(&sh->mapCond, &sh->mapLock);
    }
    for (int32_t i = 0; i < sh->connCount; ++i) {
      if (sh->conns[i].taos != NULL && connectionCount < REPLAY_MAX_HANDLES) {
        connections[connectionCount++] = sh->conns[i].taos;
      }
    }
    for (int32_t i = 0; i < sh->resultCount; ++i) {
      if (sh->results[i].res != NULL &&
          (!sh->results[i].retired || sh->results[i].borrowers > 0) &&
          resultCount < REPLAY_MAX_HANDLES) {
        results[resultCount++] = sh->results[i];
        sh->results[i].borrowers++;
      }
    }
    (void)taosThreadMutexUnlock(&sh->mapLock);
    for (int32_t i = 0; i < connectionCount; ++i) taos_kill_query(connections[i]);
    for (int32_t i = 0; i < resultCount; ++i) {
      taos_stop_query(results[i].res);
      replayLaneReleaseResult(sh, results[i].sessionIdx, results[i].recordedId, results[i].res);
    }
    (void)taosThreadMutexLock(&sh->mapLock);
    sh->cancellingQueries = false;
    (void)taosThreadCondBroadcast(&sh->mapCond);
    (void)taosThreadMutexUnlock(&sh->mapLock);
    taosMsleep(100);
  }
  taosMemoryFree(connections);
  taosMemoryFree(results);
  return NULL;
}

int32_t replayExecute(const SReplayOpts *opts, SReplaySession *session) {
  SReplayShared *sh = NULL;
  SReplayLane   *lanes = NULL;
  int32_t        laneCount = 0;
  int32_t        i;
  int32_t        exitCode = REPLAY_EXIT_OK;
  TdThread       timeoutThread;
  bool           timeoutStarted = false;

  for (int32_t index = 0; index < session->eventCount; ++index) {
    const SReplayEvent *event = &session->events[index];
    if (event->phase != WLR_PHASE_BEGIN) continue;
    if (event->apiType == WLR_API_SELECT_DB && (event->db == NULL || event->dbLen == 0)) {
      (void)fprintf(stderr,
                    "replay: select_db eventSeq %" PRIu64 " has unavailable database name; refusing replay before connecting\n",
                    event->eventSeq);
      return REPLAY_EXIT_INPUT;
    }
    if (event->apiType != WLR_API_QUERY && event->apiType != WLR_API_QUERY_WITH_REQID &&
        event->apiType != WLR_API_QUERY_A && event->apiType != WLR_API_QUERY_A_WITH_REQID) continue;
    if (event->sql != NULL && event->sqlLen > 0 && event->sqlLen <= WLR_MAX_FRAME_PAYLOAD) continue;
    (void)fprintf(stderr,
                  "replay: query eventSeq %" PRIu64 " has unavailable SQL; refusing replay before connecting\n",
                  event->eventSeq);
    return REPLAY_EXIT_INPUT;
  }

  sh = (SReplayShared *)taosMemoryCalloc(1, sizeof(SReplayShared));
  lanes = (SReplayLane *)taosMemoryCalloc(REPLAY_MAX_LANES, sizeof(SReplayLane));
  if (sh == NULL || lanes == NULL) {
    taosMemoryFree(sh);
    taosMemoryFree(lanes);
    return REPLAY_EXIT_INTERNAL;
  }
  if (taosThreadMutexInit(&sh->mapLock, NULL) != 0) {
    (void)fprintf(stderr, "failed to initialize replay map mutex\n");
    taosMemoryFree(sh);
    taosMemoryFree(lanes);
    return REPLAY_EXIT_INTERNAL;
  }
  if (taosThreadCondInit(&sh->mapCond, NULL) != 0) {
    (void)fprintf(stderr, "failed to initialize replay map condition\n");
    (void)taosThreadMutexDestroy(&sh->mapLock);
    taosMemoryFree(sh);
    taosMemoryFree(lanes);
    return REPLAY_EXIT_INTERNAL;
  }
  if (taosThreadMutexInit(&sh->statLock, NULL) != 0) {
    (void)fprintf(stderr, "failed to initialize replay statistics mutex\n");
    (void)taosThreadCondDestroy(&sh->mapCond);
    (void)taosThreadMutexDestroy(&sh->mapLock);
    taosMemoryFree(sh);
    taosMemoryFree(lanes);
    return REPLAY_EXIT_INTERNAL;
  }
  if (taosThreadMutexInit(&sh->depLock, NULL) != 0) {
    (void)fprintf(stderr, "failed to initialize replay dependency mutex\n");
    (void)taosThreadMutexDestroy(&sh->statLock);
    (void)taosThreadCondDestroy(&sh->mapCond);
    (void)taosThreadMutexDestroy(&sh->mapLock);
    taosMemoryFree(sh);
    taosMemoryFree(lanes);
    return REPLAY_EXIT_INTERNAL;
  }
  if (taosThreadCondInit(&sh->depCond, NULL) != 0) {
    (void)fprintf(stderr, "failed to initialize replay dependency condition\n");
    (void)taosThreadMutexDestroy(&sh->depLock);
    (void)taosThreadMutexDestroy(&sh->statLock);
    (void)taosThreadCondDestroy(&sh->mapCond);
    (void)taosThreadMutexDestroy(&sh->mapLock);
    taosMemoryFree(sh);
    taosMemoryFree(lanes);
    return REPLAY_EXIT_INTERNAL;
  }
  sh->opts = opts;
  sh->session = session;

  /*
   * Credentials are pre-flighted before any lane starts. Failing here costs nothing;
   * failing once lanes are running leaves the target partially mutated (DS 6.20).
   */
  for (i = 0; i < session->eventCount; ++i) {
    const SReplayEvent *e = &session->events[i];
    if (e->phase != WLR_PHASE_BEGIN) continue;
    if (e->apiType == WLR_API_CONNECT_AUTH || e->apiType == WLR_API_CONNECT_TOTP ||
        e->apiType == WLR_API_CONNECT_TOKEN) {
      (void)fprintf(stderr,
                    "replay: recorded authentication API %u requires a secret that is "
                    "intentionally absent from the recording; refusing replay\n",
                    (uint32_t)e->apiType);
      exitCode = REPLAY_EXIT_CONN;
      goto _done;
    }
    if (e->apiType != WLR_API_CONNECT && e->apiType != WLR_API_CONNECT_WITH) continue;
    if (e->apiType == WLR_API_CONNECT_WITH && e->connOptListOmitted) {
      (void)fprintf(stderr,
                    "replay: recorded connect_with omitted connection options; refusing replay before connecting\n");
      exitCode = REPLAY_EXIT_INPUT;
      goto _done;
    }
    char     host[TSDB_FQDN_LEN] = {0};
    uint16_t port = 0;
    if (replayResolveEndpoint(opts, e->endpoint, e->endpointLen, e->port,
                              host, sizeof(host), &port) != 0) {
      exitCode = REPLAY_EXIT_CONN;
      goto _done;
    }
    if (e->user == NULL || e->userLen == 0) continue;
    if (replayFindPassword(opts, e->user, e->userLen) == NULL) {
      (void)fprintf(stderr, "no --user-password given for recorded user '%.*s'\n", (int32_t)e->userLen,
                    e->user);
      exitCode = REPLAY_EXIT_CONN;
      goto _done;
    }
  }

  /* Build dependencies from recorded call intervals. A BEGIN waits for calls whose
   * RETURN/DONE preceded it, but not for calls that were still in flight and therefore
   * genuinely overlapped it. */
  int32_t depCode = replayLaneInitDeps(sh);
  if (depCode != REPLAY_EXIT_OK) {
    (void)fprintf(stderr, "failed to initialize replay dependency state\n");
    exitCode = depCode;
    goto _done;
  }

  /*
   * Partition BEGIN events by recorded thread. Only BEGIN drives execution: RETURN and
   * DONE are the recorded outcome to compare against, not calls to reissue.
   */
  for (i = 0; i < session->eventCount; ++i) {
    const SReplayEvent *e = &session->events[i];
    int32_t             li;
    if (e->phase != WLR_PHASE_BEGIN) continue;

    for (li = 0; li < laneCount; ++li) {
      if (lanes[li].sessionIdx == e->sessionIdx && lanes[li].originTid == e->originTid) break;
    }
    if (li == laneCount) {
      if (laneCount >= REPLAY_MAX_LANES) {
        (void)fprintf(stderr, "recording uses more than %d threads; refusing replay before connecting\n",
                      REPLAY_MAX_LANES);
        exitCode = REPLAY_EXIT_INPUT;
        goto _done;
      }
      lanes[laneCount].sessionIdx = e->sessionIdx;
      lanes[laneCount].originTid = e->originTid;
      lanes[laneCount].shared = sh;
      laneCount++;
    }
    if (lanes[li].eventCount >= lanes[li].eventCap) {
      if (lanes[li].eventCap > INT32_MAX / 2) {
        (void)fprintf(stderr, "replay: too many events for thread %" PRIu64 "\n",
                      lanes[li].originTid);
        exitCode = REPLAY_EXIT_INTERNAL;
        goto _done;
      }
      int32_t newCap = (lanes[li].eventCap == 0) ? 64 : lanes[li].eventCap * 2;
      if ((size_t)newCap > SIZE_MAX / sizeof(int32_t)) {
        (void)fprintf(stderr, "replay: event index allocation overflow for thread %" PRIu64 "\n",
                      lanes[li].originTid);
        exitCode = REPLAY_EXIT_INTERNAL;
        goto _done;
      }
      int32_t *n = (int32_t *)taosMemoryRealloc(lanes[li].eventIdx, (size_t)newCap * sizeof(int32_t));
      if (n == NULL) {
        exitCode = REPLAY_EXIT_INTERNAL;
        goto _done;
      }
      lanes[li].eventIdx = n;
      lanes[li].eventCap = newCap;
    }
    /* Appended in ascending event order, which is what keeps a lane's calls causally
     * ordered (a fetch after its query). */
    lanes[li].eventIdx[lanes[li].eventCount++] = i;
  }

  /* Async calls are awaited by their replay lane. That is safe only when the original
   * lane issued no later call before DONE: otherwise serialising here can put a
   * recorded kill/stop behind the operation it was meant to cancel. Reject that
   * dependency shape before any target connection is opened or mutated. */
  for (i = 0; i < laneCount; ++i) {
    int32_t eventPos;
    for (eventPos = 0; eventPos + 1 < lanes[i].eventCount; ++eventPos) {
      int32_t beginIdx = lanes[i].eventIdx[eventPos];
      const SReplayEvent *event = &session->events[beginIdx];
      int32_t terminalIdx = sh->terminalByBegin[beginIdx];
      if (!replayCallAwaitedSynchronously(event)) continue;
      if (terminalIdx < 0 || lanes[i].eventIdx[eventPos + 1] < terminalIdx) {
        (void)fprintf(stderr,
                      "replay: asynchronous call api=%u callId=%" PRIu64
                      " overlaps a later call on recorded thread %" PRIu64
                      "; this dependency shape is not yet replayable safely\n",
                      event->apiType, event->callId, lanes[i].originTid);
        exitCode = REPLAY_EXIT_INPUT;
        goto _done;
      }
    }
  }

  if (laneCount == 0) {
    (void)fprintf(stderr, "no replayable events found\n");
    exitCode = REPLAY_EXIT_INPUT;
    goto _done;
  }

  sh->originMonoNs = session->events[0].monoNs;
  sh->startMonoNs = taosGetMonoTimestampNs();
  if (!session->unfinishedCountKnown && opts->unfinishedTimeoutSec > 0) {
    (void)fprintf(stderr,
                  "replay: unfinished-call count is unknown; enabling the timeout watchdog conservatively\n");
  }
  (void)taosThreadMutexLock(&sh->depLock);
  sh->replayDone = false;
  (void)taosThreadMutexUnlock(&sh->depLock);
  if (taosThreadCreate(&timeoutThread, NULL, replayUnfinishedWatchdog, sh) != 0) {
    (void)fprintf(stderr, "failed to start replay cancellation watchdog\n");
    exitCode = REPLAY_EXIT_INTERNAL;
    goto _done;
  }
  timeoutStarted = true;

  for (i = 0; i < laneCount; ++i) {
    lanes[i].watchdogSlot = i;
    if (taosThreadCreate(&lanes[i].thread, NULL, replayLaneMain, &lanes[i]) == 0) {
      lanes[i].started = true;
    } else {
      (void)fprintf(stderr, "failed to start lane for thread %" PRIu64 "\n", lanes[i].originTid);
      exitCode = REPLAY_EXIT_INTERNAL;
      /*
       * Abort, or the replay HANGS instead of failing.
       *
       * This lane's events are never dispatched and so never published. Lanes that
       * did start and whose events sort after them wait on replayDepsSatisfied, which
       * can never become true -- and with aborted == 0 nothing releases the wait, so
       * taosThreadJoin below blocks forever. Publishing the abort turns an
       * unrecoverable hang into the reported failure exitCode already records.
       */
      replayLaneAbort(sh, REPLAY_EXIT_INTERNAL);
    }
  }
  for (i = 0; i < laneCount; ++i) {
    if (lanes[i].started) (void)taosThreadJoin(lanes[i].thread, NULL);
  }

_done:
  if (timeoutStarted) {
    (void)taosThreadMutexLock(&sh->depLock);
    sh->replayDone = true;
    (void)taosThreadCondBroadcast(&sh->depCond);
    (void)taosThreadMutexUnlock(&sh->depLock);
    (void)taosThreadJoin(timeoutThread, NULL);
    timeoutStarted = false;
  }
  /*
   * Cleanup order matters: statements hold requests owned by their connection, and
   * results are owned by it too, so both must go before the connections do.
   */
  for (i = 0; i < sh->stmtCount; ++i) {
    if (sh->stmts[i].stmt != NULL) (void)taos_stmt_close(sh->stmts[i].stmt);
    if (sh->stmts[i].stmt2 != NULL) (void)taos_stmt2_close(sh->stmts[i].stmt2);
  }
  for (i = 0; i < sh->resultCount; ++i) {
    if (sh->results[i].owned && sh->results[i].res != NULL) {
      taos_free_result(sh->results[i].res);
    }
  }
  for (i = 0; i < sh->connCount; ++i) {
    if (sh->conns[i].taos != NULL) taos_close(sh->conns[i].taos);
  }

  (void)printf("\n=== replay report ===\n");
  (void)printf("session segments     : %d\n", session->bufferCount);
  (void)printf("session stop marker  : %s\n", session->sawSessionStop ? "present" : "absent");
  (void)printf("session completeness : %s%s\n",
               session->structurallyComplete ? "structurally complete" : "INCOMPLETE",
               session->sawFatal ? ", recorder failed open" : "");
  (void)printf("worker lanes         : %d  (one per recorded thread)\n", laneCount);
  (void)printf("connections replayed : %d\n", sh->connCount);
  (void)printf("events in input      : %d\n", session->eventCount);
  (void)printf("calls executed       : %d\n", sh->executed);
  (void)printf("calls skipped        : %d\n", sh->skipped);
  (void)printf("api errors           : %d\n", sh->errors);
  (void)printf("code mismatches      : %d\n", sh->codeMismatches);
  (void)printf("count mismatches     : %d\n", sh->countMismatches);
  (void)printf("max schedule lateness: %.3f ms\n", (double)sh->maxLatenessNs / 1e6);
  if (session->unfinishedCountKnown) {
    (void)printf("unfinished in input  : %d\n", session->unfinishedCalls);
  } else {
    (void)printf("unfinished in input  : unavailable (treated as potentially unfinished)\n");
  }
  if (laneCount > 1) {
    (void)printf("\nConcurrency reproduced across %d lanes. Lateness above reflects how far\n",
                 laneCount);
    (void)printf("behind the recorded schedule the slowest call ran.\n");
  } else {
    (void)printf("\nNOTE: the recording used a single thread, so this replay is inherently\n");
    (void)printf("      serial. That is faithful, not a limitation of the tool.\n");
  }

  if (opts->reportDir != NULL) {
    char      path[TSDB_FILENAME_LEN * 2] = {0};
    TdFilePtr rf = NULL;
    int32_t pathN = snprintf(path, sizeof(path), "%s%sreplay-report.json", opts->reportDir, TD_DIRSEP);
    if (pathN < 0 || (size_t)pathN >= sizeof(path)) {
      (void)fprintf(stderr, "--report-dir is too long (maximum report path is %zu bytes)\n",
                    sizeof(path) - 1);
      exitCode = REPLAY_EXIT_PARTIAL;
    } else {
      rf = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_PRIVATE);
    }
    if (rf == NULL) {
      if (pathN >= 0 && (size_t)pathN < sizeof(path)) {
        (void)fprintf(stderr, "cannot write report to %s\n", path);
      }
      exitCode = REPLAY_EXIT_PARTIAL;
    } else {
      char    buf[2048];
      char    unfinishedCalls[32] = "null";
      bool    reportWritten = false;
      if (session->unfinishedCountKnown) {
        (void)snprintf(unfinishedCalls, sizeof(unfinishedCalls), "%d", session->unfinishedCalls);
      }
      int32_t n = snprintf(buf, sizeof(buf),
                           "{\n"
                           "  \"segments\": %d,\n"
                           "  \"sessionStopMarker\": %s,\n"
                           "  \"structurallyComplete\": %s,\n"
                           "  \"recorderFailedOpen\": %s,\n"
                           "  \"workerLanes\": %d,\n"
                           "  \"eventsInInput\": %d,\n"
                           "  \"connectionsReplayed\": %d,\n"
                           "  \"callsExecuted\": %d,\n"
                           "  \"callsSkipped\": %d,\n"
                           "  \"apiErrors\": %d,\n"
                           "  \"codeMismatches\": %d,\n"
                           "  \"countMismatches\": %d,\n"
                           "  \"maxScheduleLatenessMs\": %.3f,\n"
                           "  \"unfinishedCallsInInput\": %s,\n"
                           "  \"unfinishedCallCountKnown\": %s,\n"
                           "  \"concurrencyReproduced\": %s\n"
                           "}\n",
                           session->bufferCount, session->sawSessionStop ? "true" : "false",
                           session->structurallyComplete ? "true" : "false",
                           session->sawFatal ? "true" : "false", laneCount, session->eventCount,
                           sh->connCount, sh->executed, sh->skipped, sh->errors, sh->codeMismatches,
                           sh->countMismatches,
                           (double)sh->maxLatenessNs / 1e6, unfinishedCalls,
                           session->unfinishedCountKnown ? "true" : "false",
                           laneCount > 1 ? "true" : "false");
      if (n < 0) {
        (void)fprintf(stderr, "failed to format replay report JSON\n");
      } else if (n > 0 && (size_t)n < sizeof(buf)) {
        reportWritten = taosWriteFile(rf, buf, (int64_t)n) == n;
      } else if ((size_t)n >= sizeof(buf)) {
        (void)fprintf(stderr, "replay report truncated; JSON not written\n");
      }
      if (reportWritten) (void)taosFsyncFile(rf);
      (void)taosCloseFile(&rf);
      if (!reportWritten) (void)taosRemoveFile(path);
      if (reportWritten) {
        (void)printf("report written        : %s\n", path);
      } else {
        (void)fprintf(stderr, "replay report was not written to %s\n", path);
        if (exitCode == REPLAY_EXIT_OK) exitCode = REPLAY_EXIT_PARTIAL;
      }
    }
  }

  /* A fatal abort outranks the generic partial code: an operator needs to know the
   * replay could not connect, not merely that it finished imperfectly. */
  if (replayLaneAborted(sh) && sh->abortCode != REPLAY_EXIT_OK) {
    exitCode = sh->abortCode;
  /* A skipped call is a coverage gap even when caused by an intentional safety
   * limit: returning success would claim to have replayed calls that never ran. */
  } else if (exitCode == REPLAY_EXIT_OK &&
             (sh->errors > 0 || sh->codeMismatches > 0 || sh->countMismatches > 0 || sh->skipped > 0 ||
              !session->structurallyComplete || session->sawFatal)) {
    exitCode = REPLAY_EXIT_PARTIAL;
  }

  for (i = 0; i < laneCount; ++i) {
    taosMemoryFree(lanes[i].eventIdx);
  }
  replayLaneFreeDeps(sh);
  (void)taosThreadCondDestroy(&sh->mapCond);
  (void)taosThreadMutexDestroy(&sh->mapLock);
  (void)taosThreadMutexDestroy(&sh->statLock);
  (void)taosThreadMutexDestroy(&sh->depLock);
  (void)taosThreadCondDestroy(&sh->depCond);
  taosMemoryFree(lanes);
  taosMemoryFree(sh);
  return exitCode;
}

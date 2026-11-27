/*
 * Copyright (c) 2019 TAOS Data, Inc. (jhtao@taosdata.com)
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * This program is free software: you can use, redistribute, and/or modify it under the terms of the
 * GNU Affero General Public License, version 3 or later ("AGPL") as published by the Free Software
 * Foundation.
 */

// Compile the implementation in this test-only translation unit to use its private state without
// duplicating layouts. Regression entry point for the Devon telemetry report_url fix.
#include "../src/thttp.c"

// Devon scan item 290: a report_url value longer than the destination capacity must be rejected
// without touching the destination; a value that fits must be copied out.
// urlLen < cap  -> return 1 when the value is copied correctly
// urlLen >= cap -> return 1 when the call fails and the destination is untouched
// 0 on unexpected behavior, negative on setup failure.
int32_t testTelemetryReportUrlBound(int32_t urlLen) {
  char dst[257];
  memset(dst, 'Z', sizeof(dst) - 1);
  dst[sizeof(dst) - 1] = 0;

  if (urlLen <= 0 || urlLen > 1000) return -1;
  int32_t cap = (int32_t)sizeof(dst);

  int32_t respLen = (int32_t)strlen("{\"report_url\":\"") + urlLen + 8;
  char*   resp = taosMemoryCalloc(1, respLen);
  if (resp == NULL) return -2;

  int32_t n = snprintf(resp, respLen, "{\"report_url\":\"");
  for (int32_t i = 0; i < urlLen; i++) resp[n + i] = 'A';
  resp[n + urlLen] = '"';
  resp[n + urlLen + 1] = '}';
  resp[n + urlLen + 2] = 0;

  int32_t code = taosTelemetryGetValueFromHttpResp(resp, "report_url", dst, cap);
  taosMemoryFree(resp);

  if (urlLen < cap - 1) {
    if (code != 0) return 0;
    if (dst[0] != 'A' || dst[urlLen] != 0) return 0;
    return 1;
  }
  if (code == 0) return 0;
  if (dst[0] != 'Z') return 0;
  return 1;
}

/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "os.h"
#include "tdTestSeam.h"
#include "tlog.h"

static const char* gateDir;
static const char* gateStream;

static void waitForDeployRelease(const char* phase, const char* streamName) {
  const char* name = strrchr(streamName, '.');
  if (strcmp(name ? name + 1 : streamName, gateStream) != 0) return;
  char path[PATH_MAX];
  int  n = snprintf(path, sizeof(path), "%s/%s.hold", gateDir, phase);
  if (n < 0 || n >= sizeof(path) || !taosCheckExistFile(path)) return;
  uInfo("stream deploy test entered %s, phaseStartTs:%" PRId64, phase, taosGetTimestampMs());
  for (int i = 0; i < 1200; ++i) {
    if (!taosCheckExistFile(path)) {
      uInfo("stream deploy test released %s", phase);
      return;
    }
    taosMsleep(100);
  }
  uError("stream deploy test expired %s", phase);
}

int dmStartDaemon(int argc, const char* argv[]);

int main(int argc, const char* argv[]) {
  if (argc < 5) return 2;
  gateDir = argv[1];
  gateStream = argv[2];
  gTdSeam_msmDeployPhase = waitForDeployRelease;
  argv[2] = argv[0];
  return dmStartDaemon(argc - 2, argv + 2);
}

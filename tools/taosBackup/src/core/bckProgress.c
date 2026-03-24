/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include "bckProgress.h"
#include "bck.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <inttypes.h>

BckProgress  g_progress     = {0};
volatile int g_tty_progress = 0;

static pthread_t    s_progThread;
static volatile int s_progStop = 0;

static void progFmtTimestamp(char *buf, size_t sz) {
    int64_t ms = taosGetTimestampMs();
    time_t t = (time_t)(ms / 1000);
    struct tm tm_s;
    taosLocalTime(&t, &tm_s, NULL, 0, NULL);
    snprintf(buf, sz, "%02d:%02d:%02d", tm_s.tm_hour, tm_s.tm_min, tm_s.tm_sec);
}

// Format seconds as "~Xs", "~Xm Ys", "~Xh Ym"
static void progFmtEta(char *buf, size_t sz, double secs) {
    if (secs <= 0 || secs > 86400.0 * 7) {
        snprintf(buf, sz, "?");
        return;
    }
    int s = (int)secs;
    if (s < 60)
        snprintf(buf, sz, "~%ds", s);
    else if (s < 3600)
        snprintf(buf, sz, "~%2dm%02ds", s / 60, s % 60);
    else
        snprintf(buf, sz, "~%dh%02dm", s / 3600, (s % 3600) / 60);
}

// Print one progress line.  newline=true adds \n (used for non-tty or final clear).
static void progPrintLine(bool newline) {
    char ts[10];
    progFmtTimestamp(ts, sizeof(ts));

    int64_t phase  = g_progress.phase;

    // snapshot all atomic/volatile fields
    int64_t dbIdx  = g_progress.dbIndex;
    int64_t dbTot  = g_progress.dbTotal;
    char    dname[PROGRESS_DB_NAME_LEN];
    memcpy(dname, (char *)g_progress.dbName, PROGRESS_DB_NAME_LEN - 1);
    dname[PROGRESS_DB_NAME_LEN - 1] = '\0';
    int64_t stbIdx = g_progress.stbIndex;
    int64_t stbTot = g_progress.stbTotal;
    char    sname[PROGRESS_STB_NAME_LEN];
    memcpy(sname, (char *)g_progress.stbName, PROGRESS_STB_NAME_LEN - 1);
    sname[PROGRESS_STB_NAME_LEN - 1] = '\0';

    int64_t ctbDone  = g_progress.ctbDoneCur;
    int64_t ctbTot   = g_progress.ctbTotalCur;
    int64_t doneAll  = g_progress.ctbDoneAll + ctbDone;
    int64_t totAll   = g_progress.ctbTotalAll;
    int64_t startMs  = g_progress.startMs;

    // elapsed seconds
    double elapsed = 0.0;
    if (startMs > 0) {
        int64_t nowMs = taosGetTimestampMs();
        elapsed = (double)(nowMs - startMs) / 1000.0;
    }

    double speed = (elapsed > 1.0) ? (double)doneAll / elapsed : 0.0;

    // ETA
    char etaBuf[32];
    if (speed > 0.0 && totAll > doneAll)
        progFmtEta(etaBuf, sizeof(etaBuf), (double)(totAll - doneAll) / speed);
    else if (totAll > 0 && doneAll >= totAll)
        snprintf(etaBuf, sizeof(etaBuf), "done");
    else
        snprintf(etaBuf, sizeof(etaBuf), "?");

    // per-STB percent
    char pctBuf[16];
    if (ctbTot > 0)
        snprintf(pctBuf, sizeof(pctBuf), "%.1f%%", 100.0 * (double)ctbDone / (double)ctbTot);
    else
        snprintf(pctBuf, sizeof(pctBuf), "-");

    // row count abbreviation (data phase only; meta phase shows ctb count)
    int64_t rows = g_stats.totalRows;
    char rowsBuf[32];
    if      (rows >= 1000000000LL) snprintf(rowsBuf, sizeof(rowsBuf), "%.1fB", (double)rows / 1e9);
    else if (rows >= 1000000LL)    snprintf(rowsBuf, sizeof(rowsBuf), "%.1fM", (double)rows / 1e6);
    else if (rows >= 1000LL)       snprintf(rowsBuf, sizeof(rowsBuf), "%.1fK", (double)rows / 1e3);
    else                           snprintf(rowsBuf, sizeof(rowsBuf), "%" PRId64, rows);

    char line[512];

    const char *dbDisp  = dname[0] ? dname : "-";
    const char *stbDisp = sname[0] ? sname : "-";

    if (phase == PROGRESS_PHASE_META) {
        // META phase: [X/Y] db: name  [X/Y] stb: name  meta  ctb: X/Y (%)  speed: N/s  ETA: xxx
        snprintf(line, sizeof(line),
                 "[%s]  [%" PRId64 "/%" PRId64 "] db: %s"
                 "  [%" PRId64 "/%" PRId64 "] stb: %s  meta"
                 "  ctb: %" PRId64 "/%" PRId64 " (%s)"
                 "  speed: %.0f/s  ETA: %s",
                 ts, dbIdx, dbTot, dbDisp,
                 stbIdx, stbTot, stbDisp,
                 ctbDone, ctbTot, pctBuf,
                 speed, etaBuf);
    } else {
        // DATA phase: [X/Y] db: name  [X/Y] stb: name  (file|ctb): X/Y (%)  rows: N  speed: N/s  ETA: xxx
        const char *unitLabel = g_progress.isRestore ? "file" : "ctb";
        snprintf(line, sizeof(line),
                 "[%s]  [%" PRId64 "/%" PRId64 "] db: %s"
                 "  [%" PRId64 "/%" PRId64 "] stb: %s"
                 "  %s: %" PRId64 "/%" PRId64 " (%s)"
                 "  rows: %s  speed: %.0f/s  ETA: %s",
                 ts, dbIdx, dbTot, dbDisp,
                 stbIdx, stbTot, stbDisp,
                 unitLabel, ctbDone, ctbTot, pctBuf,
                 rowsBuf, speed, etaBuf);
    }

    flockfile(stdout);
    if (g_tty_progress) {
        printf("\r%s\033[K", line);
        if (newline) printf("\n");
    } else {
        printf("%s\n", line);
    }
    fflush(stdout);
    funlockfile(stdout);
}

static void *progressThread(void *arg) {
    (void)arg;
    bool tty = isatty(STDOUT_FILENO);
    g_tty_progress = tty ? 1 : 0;

    int nonTtyTick = 0;  // counts 1-second ticks; print every 30s in non-tty

    while (!s_progStop) {
        // sleep 1 second via 100 ms slices for responsive stop
        for (int i = 0; i < 10 && !s_progStop; i++) {
            taosMsleep(100);
        }
        if (s_progStop) break;

        // wait until at least one STB is in progress (meta or data phase)
        if (g_progress.phase == PROGRESS_PHASE_IDLE) continue;

        if (tty) {
            progPrintLine(false);
        } else {
            nonTtyTick++;
            if (nonTtyTick >= 30) {
                progPrintLine(true);
                nonTtyTick = 0;
            }
        }
    }

    // in tty mode: clear the rolling line so the next log message prints cleanly
    if (tty && g_progress.phase != PROGRESS_PHASE_IDLE) {
        flockfile(stdout);
        printf("\r\033[K");
        fflush(stdout);
        funlockfile(stdout);
    }
    g_tty_progress = 0;

    return NULL;
}

void progressStart(void) {
    s_progStop = 0;
    pthread_create(&s_progThread, NULL, progressThread, NULL);
}

void progressStop(void) {
    s_progStop = 1;
    pthread_join(s_progThread, NULL);
}

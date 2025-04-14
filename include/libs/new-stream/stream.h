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

#ifndef TDENGINE_STREAM_H
#define TDENGINE_STREAM_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

#define STREAM_MAX_GROUP_NUM 5

enum {
  STREAM_STATUS_INIT = 1,
  STREAM_STATUS_RUNNING,
  STREAM_STATUS_STOPPED,
  STREAM_STATUS_FAILED,
};

typedef enum EStreamTaskType {
  STREAM_READER_TASK = 0,
  STREAM_TRIGGER_TASK,
  STREAM_RUNNER_TASK,
} EStreamTaskType;

static const char *gTaskTypeStr[] = {"Reader", "Trigger", "Runner"};

typedef struct SStreamTask {
  EStreamTaskType type;
  int64_t         streamId;  // ID of the stream
  int64_t         taskId;    // ID of the current task
  int32_t         vgId;      // ID of the vgroup
  int32_t         cmdId;     // ID of the current command (real-time, historical, or recalculation)
} SStreamTask;

typedef enum EStreamTriggerType {
  STREAM_PERIODIC_TRIGGER,
  STERAM_COMMIT_TRIGGER,
  STREAM_WINDOW_TRIGGER,
} EStreamTriggerType;

typedef struct SStreamTriggerTask {
  SStreamTask        task;
  EStreamTriggerType type;
} SStreamTriggerTask;

#define STREAM_GID(_streamId) ((_streamId) % STREAM_MAX_GROUP_NUM)

// clang-format off
#define stFatal(...) do { if (stDebugFlag & DEBUG_FATAL) { taosPrintLog("STM FATAL ", DEBUG_FATAL, 255,         __VA_ARGS__); }} while(0)
#define stError(...) do { if (stDebugFlag & DEBUG_ERROR) { taosPrintLog("STM ERROR ", DEBUG_ERROR, 255,         __VA_ARGS__); }} while(0)
#define stWarn(...)  do { if (stDebugFlag & DEBUG_WARN)  { taosPrintLog("STM WARN  ", DEBUG_WARN,  255,         __VA_ARGS__); }} while(0)
#define stInfo(...)  do { if (stDebugFlag & DEBUG_INFO)  { taosPrintLog("STM INFO  ", DEBUG_INFO,  255,         __VA_ARGS__); }} while(0)
#define stDebug(...) do { if (stDebugFlag & DEBUG_DEBUG) { taosPrintLog("STM DEBUG ", DEBUG_DEBUG, stDebugFlag, __VA_ARGS__); }} while(0)
#define stTrace(...) do { if (stDebugFlag & DEBUG_TRACE) { taosPrintLog("STM TRACE ", DEBUG_TRACE, stDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

#define ST_TASK_FLOG(param, ...)                                                                                      \
  stFatal("TYPE: %s, VGID:%d, STREAM:%" PRIx64 ", TASK:%" PRId64 ", COMMAND:%d " param,             \
          gTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->vgId, ((SStreamTask *)pTask)->streamId, \
          ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->cmdId, __VA_ARGS__)
#define ST_TASK_ELOG(param, ...)                                                                                      \
  stError("TYPE: %s, VGID:%d, STREAM:%" PRIx64 ", TASK:%" PRId64 ", COMMAND:%d " param,             \
          gTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->vgId, ((SStreamTask *)pTask)->streamId, \
          ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->cmdId, __VA_ARGS__)
#define ST_TASK_WLOG(param, ...)                                                                                     \
  stWarn("TYPE: %s, VGID:%d, STREAM:%" PRIx64 ", TASK:%" PRId64 ", COMMAND:%d " param,             \
         gTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->vgId, ((SStreamTask *)pTask)->streamId, \
         ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->cmdId, __VA_ARGS__)
#define ST_TASK_ILOG(param, ...)                                                                                     \
  stInfo("TYPE: %s, VGID:%d, STREAM:%" PRIx64 ", TASK:%" PRId64 ", COMMAND:%d " param,             \
         gTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->vgId, ((SStreamTask *)pTask)->streamId, \
         ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->cmdId, __VA_ARGS__)
#define ST_TASK_DLOG(param, ...)                                                                                      \
  stDebug("TYPE: %s, VGID:%d, STREAM:%" PRIx64 ", TASK:%" PRId64 ", COMMAND:%d " param,             \
          gTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->vgId, ((SStreamTask *)pTask)->streamId, \
          ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->cmdId, __VA_ARGS__)
#define ST_TASK_TLOG(param, ...)                                                                                      \
  stTrace("TYPE: %s, VGID:%d, STREAM:%" PRIx64 ", TASK:%" PRId64 ", COMMAND:%d " param,             \
          gTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->vgId, ((SStreamTask *)pTask)->streamId, \
          ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->cmdId, __VA_ARGS__)

#define mstFatal(param, ...) stFatal("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstError(param, ...) stError("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstWarn(param, ...) stWarn("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstInfo(param, ...) stInfo("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstDebug(param, ...) stDebug("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstTrace(param, ...) stTrace("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_H

//
// Created by slzhou on 22-4-20.
//

#ifndef TDENGINE_FNLOG_H
#define TDENGINE_FNLOG_H
#include "tlog.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define fnFatal(...) { if (udfDebugFlag & DEBUG_FATAL) { taosPrintLog("UDF FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}
#define fnError(...) { if (udfDebugFlag & DEBUG_ERROR) { taosPrintLog("UDF ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}
#define fnWarn(...)  { if (udfDebugFlag & DEBUG_WARN)  { taosPrintLog("UDF WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}
#define fnInfo(...)  { if (udfDebugFlag & DEBUG_INFO)  { taosPrintLog("UDF ", DEBUG_INFO, 255, __VA_ARGS__); }}
#define fnDebug(...) { if (udfDebugFlag & DEBUG_DEBUG) { taosPrintLog("UDF ", DEBUG_DEBUG, udfDebugFlag, __VA_ARGS__); }}
#define fnTrace(...) { if (udfDebugFlag & DEBUG_TRACE) { taosPrintLog("UDF ", DEBUG_TRACE, udfDebugFlag, __VA_ARGS__); }}
// clang-format on

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FNLOG_H

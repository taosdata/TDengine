//
// Created by slzhou on 22-4-20.
//

#ifndef TDENGINE_FNLOG_H
#define TDENGINE_FNLOG_H
#include "tlog.h"

#ifdef __cplusplus
extern "C" {
#endif

#define fnFatal(...) { if (fnDebugFlag & DEBUG_FATAL) { taosPrintLog("FN FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}
#define fnError(...) { if (fnDebugFlag & DEBUG_ERROR) { taosPrintLog("FN ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}
#define fnWarn(...)  { if (fnDebugFlag & DEBUG_WARN)  { taosPrintLog("FN WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}
#define fnInfo(...)  { if (fnDebugFlag & DEBUG_INFO)  { taosPrintLog("FN ", DEBUG_INFO, 255, __VA_ARGS__); }}
#define fnDebug(...) { if (fnDebugFlag & DEBUG_DEBUG) { taosPrintLog("FN ", DEBUG_DEBUG, dDebugFlag, __VA_ARGS__); }}
#define fnTrace(...) { if (fnDebugFlag & DEBUG_TRACE) { taosPrintLog("FN ", DEBUG_TRACE, dDebugFlag, __VA_ARGS__); }}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FNLOG_H

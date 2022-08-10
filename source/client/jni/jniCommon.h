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

#include <jni.h>
#include "taoserror.h"
#include "tdef.h"
#include "tlog.h"
#include "ttypes.h"

#ifndef TDENGINE_JNICOMMON_H
#define TDENGINE_JNICOMMON_H

#define jniFatal(...)                                                     \
  {                                                                       \
    if (jniDebugFlag & DEBUG_FATAL) {                                     \
      taosPrintLog("JNI FATAL ", DEBUG_FATAL, jniDebugFlag, __VA_ARGS__); \
    }                                                                     \
  }
#define jniError(...)                                                     \
  {                                                                       \
    if (jniDebugFlag & DEBUG_ERROR) {                                     \
      taosPrintLog("JNI ERROR ", DEBUG_ERROR, jniDebugFlag, __VA_ARGS__); \
    }                                                                     \
  }
#define jniWarn(...)                                                    \
  {                                                                     \
    if (jniDebugFlag & DEBUG_WARN) {                                    \
      taosPrintLog("JNI WARN ", DEBUG_WARN, jniDebugFlag, __VA_ARGS__); \
    }                                                                   \
  }
#define jniInfo(...)                                               \
  {                                                                \
    if (jniDebugFlag & DEBUG_INFO) {                               \
      taosPrintLog("JNI ", DEBUG_INFO, jniDebugFlag, __VA_ARGS__); \
    }                                                              \
  }
#define jniDebug(...)                                               \
  {                                                                 \
    if (jniDebugFlag & DEBUG_DEBUG) {                               \
      taosPrintLog("JNI ", DEBUG_DEBUG, jniDebugFlag, __VA_ARGS__); \
    }                                                               \
  }
#define jniTrace(...)                                               \
  {                                                                 \
    if (jniDebugFlag & DEBUG_TRACE) {                               \
      taosPrintLog("JNI ", DEBUG_TRACE, jniDebugFlag, __VA_ARGS__); \
    }                                                               \
  }

extern jclass    g_arrayListClass;
extern jmethodID g_arrayListConstructFp;
extern jmethodID g_arrayListAddFp;

extern jclass    g_metadataClass;
extern jmethodID g_metadataConstructFp;
extern jfieldID  g_metadataColtypeField;
extern jfieldID  g_metadataColnameField;
extern jfieldID  g_metadataColsizeField;
extern jfieldID  g_metadataColindexField;

extern jclass    g_rowdataClass;
extern jmethodID g_rowdataConstructor;
extern jmethodID g_rowdataClearFp;
extern jmethodID g_rowdataSetBooleanFp;
extern jmethodID g_rowdataSetByteFp;
extern jmethodID g_rowdataSetShortFp;
extern jmethodID g_rowdataSetIntFp;
extern jmethodID g_rowdataSetLongFp;
extern jmethodID g_rowdataSetFloatFp;
extern jmethodID g_rowdataSetDoubleFp;
extern jmethodID g_rowdataSetStringFp;
extern jmethodID g_rowdataSetTimestampFp;
extern jmethodID g_rowdataSetByteArrayFp;

extern jmethodID g_blockdataSetByteArrayFp;
extern jmethodID g_blockdataSetNumOfRowsFp;
extern jmethodID g_blockdataSetNumOfColsFp;

extern jclass    g_tmqClass;
extern jmethodID g_createConsumerErrorCallback;
extern jmethodID g_topicListCallback;

extern jclass    g_consumerClass;
extern jmethodID g_commitCallback;

jstring jniFromNCharToByteArray(JNIEnv *, char *, int32_t);

#define JNI_SUCCESS         0
#define JNI_TDENGINE_ERROR  -1
#define JNI_CONNECTION_NULL -2
#define JNI_RESULT_SET_NULL -3
#define JNI_NUM_OF_FIELDS_0 -4
#define JNI_SQL_NULL        -5
#define JNI_FETCH_END       -6
#define JNI_OUT_OF_MEMORY   -7

#define TMQ_CONF_NULL             -100;
#define TMQ_CONF_KEY_NULL         -101;
#define TMQ_CONF_VALUE_NULL       -102;
#define TMQ_TOPIC_NULL            -110;
#define TMQ_TOPIC_NAME_NULL       -111;
#define TMQ_CONSUMER_NULL         -120;
#define TMQ_CONSUMER_CREATE_ERROR -121;

extern JavaVM *g_vm;

void jniGetGlobalMethod(JNIEnv *env);

int32_t check_for_params(jobject jobj, jlong conn, jlong res);

#endif  // TDENGINE_JNICOMMON_H

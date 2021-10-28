#include <jni.h>

#ifndef TDENGINE_JNICOMMON_H
#define TDENGINE_JNICOMMON_H

#define jniFatal(...)                                                            \
  {                                                                              \
    if (jniDebugFlag & DEBUG_FATAL) {                                            \
      taosPrintLog("JNI FATAL ", tscEmbedded ? 255 : jniDebugFlag, __VA_ARGS__); \
    }                                                                            \
  }
#define jniError(...)                                                            \
  {                                                                              \
    if (jniDebugFlag & DEBUG_ERROR) {                                            \
      taosPrintLog("JNI ERROR ", tscEmbedded ? 255 : jniDebugFlag, __VA_ARGS__); \
    }                                                                            \
  }
#define jniWarn(...)                                                            \
  {                                                                             \
    if (jniDebugFlag & DEBUG_WARN) {                                            \
      taosPrintLog("JNI WARN ", tscEmbedded ? 255 : jniDebugFlag, __VA_ARGS__); \
    }                                                                           \
  }
#define jniInfo(...)                                                       \
  {                                                                        \
    if (jniDebugFlag & DEBUG_INFO) {                                       \
      taosPrintLog("JNI ", tscEmbedded ? 255 : jniDebugFlag, __VA_ARGS__); \
    }                                                                      \
  }
#define jniDebug(...)                                  \
  {                                                    \
    if (jniDebugFlag & DEBUG_DEBUG) {                  \
      taosPrintLog("JNI ", jniDebugFlag, __VA_ARGS__); \
    }                                                  \
  }
#define jniTrace(...)                                  \
  {                                                    \
    if (jniDebugFlag & DEBUG_TRACE) {                  \
      taosPrintLog("JNI ", jniDebugFlag, __VA_ARGS__); \
    }                                                  \
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

#define JNI_SUCCESS 0
#define JNI_TDENGINE_ERROR -1
#define JNI_CONNECTION_NULL -2
#define JNI_RESULT_SET_NULL -3
#define JNI_NUM_OF_FIELDS_0 -4
#define JNI_SQL_NULL -5
#define JNI_FETCH_END -6
#define JNI_OUT_OF_MEMORY -7

extern JavaVM *g_vm;

void jniGetGlobalMethod(JNIEnv *env);

int32_t check_for_params(jobject jobj, jlong conn, jlong res);

#endif  // TDENGINE_JNICOMMON_H

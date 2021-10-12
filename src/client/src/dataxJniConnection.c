#include "os.h"
#include "taos.h"
#include "tlog.h"
#include "tscUtil.h"

#include "com_alibaba_datax_plugin_writer_JniConnection.h"

#define jniError(...)                                                            \
  {                                                                              \
    if (jniDebugFlag & DEBUG_ERROR) {                                            \
      taosPrintLog("JNI ERROR ", tscEmbedded ? 255 : jniDebugFlag, __VA_ARGS__); \
    }                                                                            \
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

int __init_datax = 0;

JavaVM *g_vm_datax = NULL;

jclass    g_arrayListClass;
jmethodID g_arrayListConstructFp;
jmethodID g_arrayListAddFp;

jclass    g_metadataClass;
jmethodID g_metadataConstructFp;
jfieldID  g_metadataColtypeField;
jfieldID  g_metadataColnameField;
jfieldID  g_metadataColsizeField;
jfieldID  g_metadataColindexField;

jclass    g_rowdataClass;
jmethodID g_rowdataConstructor;
jmethodID g_rowdataClearFp;
jmethodID g_rowdataSetBooleanFp;
jmethodID g_rowdataSetByteFp;
jmethodID g_rowdataSetShortFp;
jmethodID g_rowdataSetIntFp;
jmethodID g_rowdataSetLongFp;
jmethodID g_rowdataSetFloatFp;
jmethodID g_rowdataSetDoubleFp;
jmethodID g_rowdataSetStringFp;
jmethodID g_rowdataSetTimestampFp;
jmethodID g_rowdataSetByteArrayFp;

jmethodID g_blockdataSetByteArrayFp;
jmethodID g_blockdataSetNumOfRowsFp;
jmethodID g_blockdataSetNumOfColsFp;

#define JNI_SUCCESS 0
#define JNI_TDENGINE_ERROR -1
#define JNI_CONNECTION_NULL -2
#define JNI_RESULT_SET_NULL -3
#define JNI_NUM_OF_FIELDS_0 -4
#define JNI_SQL_NULL -5
#define JNI_FETCH_END -6
#define JNI_OUT_OF_MEMORY -7

static void    jniGetGlobalMethod(JNIEnv *env);
static int32_t check_for_params(jobject jobj, jlong conn, jlong res);

JNIEXPORT void JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_initImp(JNIEnv *env, jobject jobj,
                                                                                  jstring jconfigDir) {
  if (jconfigDir != NULL) {
    const char *confDir = (*env)->GetStringUTFChars(env, jconfigDir, NULL);
    if (confDir && strlen(confDir) != 0) {
      tstrncpy(configDir, confDir, TSDB_FILENAME_LEN);
    }
    (*env)->ReleaseStringUTFChars(env, jconfigDir, confDir);
  }

  jniGetGlobalMethod(env);
  jniDebug("jni initialized successfully, config directory: %s", configDir);
}

JNIEXPORT jint JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_setOptions(JNIEnv *env, jobject jobj,
                                                                                     jint    optionIndex,
                                                                                     jstring optionValue) {
  if (optionValue == NULL) {
    jniDebug("option index:%d value is null", (int32_t)optionIndex);
    return 0;
  }

  int res = 0;

  if (optionIndex == TSDB_OPTION_LOCALE) {
    const char *locale = (*env)->GetStringUTFChars(env, optionValue, NULL);
    if (locale && strlen(locale) != 0) {
      res = taos_options(TSDB_OPTION_LOCALE, locale);
      jniDebug("set locale to %s, result:%d", locale, res);
    } else {
      jniDebug("input locale is empty");
    }
    (*env)->ReleaseStringUTFChars(env, optionValue, locale);
  } else if (optionIndex == TSDB_OPTION_CHARSET) {
    const char *charset = (*env)->GetStringUTFChars(env, optionValue, NULL);
    if (charset && strlen(charset) != 0) {
      res = taos_options(TSDB_OPTION_CHARSET, charset);
      jniDebug("set character encoding to %s, result:%d", charset, res);
    } else {
      jniDebug("input character encoding is empty");
    }
    (*env)->ReleaseStringUTFChars(env, optionValue, charset);
  } else if (optionIndex == TSDB_OPTION_TIMEZONE) {
    const char *tz1 = (*env)->GetStringUTFChars(env, optionValue, NULL);
    if (tz1 && strlen(tz1) != 0) {
      res = taos_options(TSDB_OPTION_TIMEZONE, tz1);
      jniDebug("set timezone to %s, result:%d", tz1, res);
    } else {
      jniDebug("input timezone is empty");
    }
    (*env)->ReleaseStringUTFChars(env, optionValue, tz1);
  } else {
    jniError("option index:%d is not found", (int32_t)optionIndex);
  }

  return res;
}

JNIEXPORT jstring JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_getTsCharset(JNIEnv *env, jobject jobj) {
  return (*env)->NewStringUTF(env, (const char *)tsCharset);
}

JNIEXPORT jlong JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_connectImp(JNIEnv *env, jobject jobj,
                                                                                      jstring jhost, jint jport,
                                                                                      jstring jdbName, jstring juser,
                                                                                      jstring jpass) {
  jlong       ret = 0;
  const char *host = NULL;
  const char *user = NULL;
  const char *pass = NULL;
  const char *dbname = NULL;

  if (jhost != NULL) {
    host = (*env)->GetStringUTFChars(env, jhost, NULL);
  }

  if (jdbName != NULL) {
    dbname = (*env)->GetStringUTFChars(env, jdbName, NULL);
  }

  if (juser != NULL) {
    user = (*env)->GetStringUTFChars(env, juser, NULL);
  }

  if (jpass != NULL) {
    pass = (*env)->GetStringUTFChars(env, jpass, NULL);
  }

  if (user == NULL) {
    jniDebug("jobj:%p, user not specified, use default user %s", jobj, TSDB_DEFAULT_USER);
  }

  if (pass == NULL) {
    jniDebug("jobj:%p, pass not specified, use default password", jobj);
  }

  ret = (jlong)taos_connect((char *)host, (char *)user, (char *)pass, (char *)dbname, (uint16_t)jport);
  if (ret == 0) {
    jniError("jobj:%p, conn:%p, connect to database failed, host=%s, user=%s, dbname=%s, port=%d", jobj, (void *)ret,
             (char *)host, (char *)user, (char *)dbname, (int32_t)jport);
  } else {
    jniDebug("jobj:%p, conn:%p, connect to database succeed, host=%s, user=%s, dbname=%s, port=%d", jobj, (void *)ret,
             (char *)host, (char *)user, (char *)dbname, (int32_t)jport);
  }

  if (host != NULL) {
    (*env)->ReleaseStringUTFChars(env, jhost, host);
  }

  if (dbname != NULL) {
    (*env)->ReleaseStringUTFChars(env, jdbName, dbname);
  }

  if (user != NULL) {
    (*env)->ReleaseStringUTFChars(env, juser, user);
  }

  if (pass != NULL) {
    (*env)->ReleaseStringUTFChars(env, jpass, pass);
  }

  return ret;
}

JNIEXPORT jlong JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_executeQueryImp(JNIEnv *env, jobject jobj,
                                                                                           jbyteArray jsql, jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if (jsql == NULL) {
    jniError("jobj:%p, conn:%p, empty sql string", jobj, tscon);
    return JNI_SQL_NULL;
  }

  jsize len = (*env)->GetArrayLength(env, jsql);

  char *str = (char *)calloc(1, sizeof(char) * (len + 1));
  if (str == NULL) {
    jniError("jobj:%p, conn:%p, alloc memory failed", jobj, tscon);
    return JNI_OUT_OF_MEMORY;
  }

  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)str);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  SSqlObj *pSql = taos_query(tscon, str);
  int32_t  code = taos_errno(pSql);

  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code:%s, msg:%s", jobj, tscon, tstrerror(code), taos_errstr(pSql));
  } else {
    if (pSql->cmd.command == TSDB_SQL_INSERT) {
      int32_t affectRows = taos_affected_rows(pSql);
      jniDebug("jobj:%p, conn:%p, code:%s, affect rows:%d", jobj, tscon, tstrerror(code), affectRows);
    } else {
      jniDebug("jobj:%p, conn:%p, code:%s", jobj, tscon, tstrerror(code));
    }
  }

  free(str);
  return (jlong)pSql;
}

JNIEXPORT jint JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_getErrCodeImp(JNIEnv *env, jobject jobj,
                                                                                        jlong con, jlong tres) {
  int32_t code = check_for_params(jobj, con, tres);
  if (code != JNI_SUCCESS) {
    return code;
  }

  return (jint)taos_errno((TAOS_RES *)tres);
}

JNIEXPORT jstring JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_getErrMsgImp(JNIEnv *env, jobject jobj,
                                                                                          jlong tres) {
  TAOS_RES *pSql = (TAOS_RES *)tres;
  return (*env)->NewStringUTF(env, (const char *)taos_errstr(pSql));
}

JNIEXPORT jint JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_getAffectedRowsImp(JNIEnv *env, jobject jobj,
                                                                                             jlong con, jlong res) {
  TAOS *  tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  jint ret = taos_affected_rows((SSqlObj *)res);
  jniDebug("jobj:%p, conn:%p, sql:%p, res: %p, affect rows:%d", jobj, tscon, (TAOS *)con, (TAOS_RES *)res,
           (int32_t)ret);

  return ret;
}

JNIEXPORT jint JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_closeConnectionImp(JNIEnv *env, jobject jobj,
                                                                                             jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is already closed", jobj);
    return JNI_CONNECTION_NULL;
  } else {
    jniDebug("jobj:%p, conn:%p, close connection success", jobj, tscon);
    taos_close(tscon);
    return JNI_SUCCESS;
  }
}

static void jniGetGlobalMethod(JNIEnv *env) {
  // make sure init function executed once
  switch (atomic_val_compare_exchange_32(&__init_datax, 0, 1)) {
    case 0:
      break;
    case 1:
      do {
        taosMsleep(0);
      } while (atomic_load_32(&__init_datax) == 1);
    case 2:
      return;
  }

  if (g_vm_datax == NULL) {
    (*env)->GetJavaVM(env, &g_vm_datax);
  }

  jclass arrayListClass = (*env)->FindClass(env, "java/util/ArrayList");
  g_arrayListClass = (*env)->NewGlobalRef(env, arrayListClass);
  g_arrayListConstructFp = (*env)->GetMethodID(env, g_arrayListClass, "<init>", "()V");
  g_arrayListAddFp = (*env)->GetMethodID(env, g_arrayListClass, "add", "(Ljava/lang/Object;)Z");
  (*env)->DeleteLocalRef(env, arrayListClass);

  jclass metadataClass = (*env)->FindClass(env, "com/taosdata/jdbc/ColumnMetaData");
  g_metadataClass = (*env)->NewGlobalRef(env, metadataClass);
  g_metadataConstructFp = (*env)->GetMethodID(env, g_metadataClass, "<init>", "()V");
  g_metadataColtypeField = (*env)->GetFieldID(env, g_metadataClass, "colType", "I");
  g_metadataColnameField = (*env)->GetFieldID(env, g_metadataClass, "colName", "Ljava/lang/String;");
  g_metadataColsizeField = (*env)->GetFieldID(env, g_metadataClass, "colSize", "I");
  g_metadataColindexField = (*env)->GetFieldID(env, g_metadataClass, "colIndex", "I");
  (*env)->DeleteLocalRef(env, metadataClass);

  jclass rowdataClass = (*env)->FindClass(env, "com/taosdata/jdbc/TSDBResultSetRowData");
  g_rowdataClass = (*env)->NewGlobalRef(env, rowdataClass);
  g_rowdataConstructor = (*env)->GetMethodID(env, g_rowdataClass, "<init>", "(I)V");
  g_rowdataClearFp = (*env)->GetMethodID(env, g_rowdataClass, "clear", "()V");
  g_rowdataSetBooleanFp = (*env)->GetMethodID(env, g_rowdataClass, "setBoolean", "(IZ)V");
  g_rowdataSetByteFp = (*env)->GetMethodID(env, g_rowdataClass, "setByte", "(IB)V");
  g_rowdataSetShortFp = (*env)->GetMethodID(env, g_rowdataClass, "setShort", "(IS)V");
  g_rowdataSetIntFp = (*env)->GetMethodID(env, g_rowdataClass, "setInt", "(II)V");
  g_rowdataSetLongFp = (*env)->GetMethodID(env, g_rowdataClass, "setLong", "(IJ)V");
  g_rowdataSetFloatFp = (*env)->GetMethodID(env, g_rowdataClass, "setFloat", "(IF)V");
  g_rowdataSetDoubleFp = (*env)->GetMethodID(env, g_rowdataClass, "setDouble", "(ID)V");
  g_rowdataSetStringFp = (*env)->GetMethodID(env, g_rowdataClass, "setString", "(ILjava/lang/String;)V");
  g_rowdataSetTimestampFp = (*env)->GetMethodID(env, g_rowdataClass, "setTimestamp", "(IJI)V");
  g_rowdataSetByteArrayFp = (*env)->GetMethodID(env, g_rowdataClass, "setByteArray", "(I[B)V");
  (*env)->DeleteLocalRef(env, rowdataClass);

  jclass blockdataClass = (*env)->FindClass(env, "com/taosdata/jdbc/TSDBResultSetBlockData");
  jclass g_blockdataClass = (*env)->NewGlobalRef(env, blockdataClass);
  g_blockdataSetByteArrayFp = (*env)->GetMethodID(env, g_blockdataClass, "setByteArray", "(II[B)V");
  g_blockdataSetNumOfRowsFp = (*env)->GetMethodID(env, g_blockdataClass, "setNumOfRows", "(I)V");
  g_blockdataSetNumOfColsFp = (*env)->GetMethodID(env, g_blockdataClass, "setNumOfCols", "(I)V");
  (*env)->DeleteLocalRef(env, blockdataClass);

  atomic_store_32(&__init_datax, 2);
  jniDebug("native method register finished");
}

static int32_t check_for_params(jobject jobj, jlong conn, jlong res) {
  if ((TAOS *)conn == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if ((TAOS_RES *)res == NULL) {
    jniError("jobj:%p, conn:%p, res is null", jobj, (TAOS *)conn);
    return JNI_RESULT_SET_NULL;
  }

  return JNI_SUCCESS;
}

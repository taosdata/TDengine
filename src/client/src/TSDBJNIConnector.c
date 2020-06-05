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

#include "os.h"
#include "taos.h"
#include "tlog.h"
#include "tscUtil.h"
#include "tsclient.h"
#include "ttime.h"

#include "com_taosdata_jdbc_TSDBJNIConnector.h"

#define jniError(...)                                        \
  {                                                          \
    if (jniDebugFlag & DEBUG_ERROR) {                        \
      taosPrintLog("ERROR JNI ", jniDebugFlag, __VA_ARGS__); \
    }                                                        \
  }
#define jniWarn(...)                                        \
  {                                                         \
    if (jniDebugFlag & DEBUG_WARN) {                        \
      taosPrintLog("WARN JNI ", jniDebugFlag, __VA_ARGS__); \
    }                                                       \
  }
#define jniTrace(...)                                  \
  {                                                    \
    if (jniDebugFlag & DEBUG_TRACE) {                  \
      taosPrintLog("JNI ", jniDebugFlag, __VA_ARGS__); \
    }                                                  \
  }
#define jniPrint(...) \
  { taosPrintLog("JNI ", tscEmbedded ? 255 : uDebugFlag, __VA_ARGS__); }

int __init = 0;

JavaVM *g_vm = NULL;

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

#define JNI_SUCCESS          0
#define JNI_TDENGINE_ERROR  -1
#define JNI_CONNECTION_NULL -2
#define JNI_RESULT_SET_NULL -3
#define JNI_NUM_OF_FIELDS_0 -4
#define JNI_SQL_NULL        -5
#define JNI_FETCH_END       -6
#define JNI_OUT_OF_MEMORY   -7

void jniGetGlobalMethod(JNIEnv *env) {
  // make sure init function executed once
  switch (atomic_val_compare_exchange_32(&__init, 0, 1)) {
    case 0:
      break;
    case 1:
      do {
        taosMsleep(0);
      } while (atomic_load_32(&__init) == 1);
    case 2:
      return;
  }

  if (g_vm == NULL) {
    (*env)->GetJavaVM(env, &g_vm);
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
  g_rowdataSetTimestampFp = (*env)->GetMethodID(env, g_rowdataClass, "setTimestamp", "(IJ)V");
  g_rowdataSetByteArrayFp = (*env)->GetMethodID(env, g_rowdataClass, "setByteArray", "(I[B)V");
  (*env)->DeleteLocalRef(env, rowdataClass);

  atomic_store_32(&__init, 2);
  jniTrace("native method register finished");
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setAllocModeImp(JNIEnv *env, jobject jobj, jint jMode,
                                                                               jstring jPath, jboolean jAutoDump) {
  if (jPath != NULL) {
    const char *path = (*env)->GetStringUTFChars(env, jPath, NULL);
    taosSetAllocMode(jMode, path, !!jAutoDump);
    (*env)->ReleaseStringUTFChars(env, jPath, path);
  } else {
    taosSetAllocMode(jMode, NULL, !!jAutoDump);
  }
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_dumpMemoryLeakImp(JNIEnv *env, jobject jobj) {
  taosDumpMemoryLeak();
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_initImp(JNIEnv *env, jobject jobj, jstring jconfigDir) {
  if (jconfigDir != NULL) {
    const char *confDir = (*env)->GetStringUTFChars(env, jconfigDir, NULL);
    if (confDir && strlen(configDir) != 0) {
      strcpy(configDir, confDir);
    }
    (*env)->ReleaseStringUTFChars(env, jconfigDir, confDir);
  }

  jniGetGlobalMethod(env);
  jniTrace("jni initialized successfully, config directory: %s", configDir);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setOptions(JNIEnv *env, jobject jobj, jint optionIndex,
                                                                          jstring optionValue) {
  if (optionValue == NULL) {
    jniTrace("option index:%d value is null", optionIndex);
    return 0;
  }

  int res = 0;

  if (optionIndex == TSDB_OPTION_LOCALE) {
    const char *locale = (*env)->GetStringUTFChars(env, optionValue, NULL);
    if (locale && strlen(locale) != 0) {
      res = taos_options(TSDB_OPTION_LOCALE, locale);
      jniTrace("set locale to %s, result:%d", locale, res);
    } else {
      jniTrace("input locale is empty");
    }
    (*env)->ReleaseStringUTFChars(env, optionValue, locale);
  } else if (optionIndex == TSDB_OPTION_CHARSET) {
    const char *charset = (*env)->GetStringUTFChars(env, optionValue, NULL);
    if (charset && strlen(charset) != 0) {
      res = taos_options(TSDB_OPTION_CHARSET, charset);
      jniTrace("set character encoding to %s, result:%d", charset, res);
    } else {
      jniTrace("input character encoding is empty");
    }
    (*env)->ReleaseStringUTFChars(env, optionValue, charset);
  } else if (optionIndex == TSDB_OPTION_TIMEZONE) {
    const char *tz1 = (*env)->GetStringUTFChars(env, optionValue, NULL);
    if (tz1 && strlen(tz1) != 0) {
      res = taos_options(TSDB_OPTION_TIMEZONE, tz1);
      jniTrace("set timezone to %s, result:%d", timezone, res);
    } else {
      jniTrace("input timezone is empty");
    }
    (*env)->ReleaseStringUTFChars(env, optionValue, tz1);
  } else {
    jniError("option index:%d is not found", optionIndex);
  }

  return res;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_connectImp(JNIEnv *env, jobject jobj, jstring jhost,
                                                                           jint jport, jstring jdbName, jstring juser,
                                                                           jstring jpass) {
  jlong       ret = 0;
  const char *host = NULL;
  const char *dbname = NULL;
  const char *user = NULL;
  const char *pass = NULL;

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
    jniTrace("jobj:%p, user is null, use tsDefaultUser", jobj);
    user = tsDefaultUser;
  }
  if (pass == NULL) {
    jniTrace("jobj:%p, pass is null, use tsDefaultPass", jobj);
    pass = tsDefaultPass;
  }

  /*
   * set numOfThreadsPerCore = 0
   * means only one thread for client side scheduler
   */
  tsNumOfThreadsPerCore = 0.0;

  ret = (jlong)taos_connect((char *)host, (char *)user, (char *)pass, (char *)dbname, jport);
  if (ret == 0) {
    jniError("jobj:%p, conn:%p, connect to database failed, host=%s, user=%s, dbname=%s, port=%d", jobj, (void *)ret,
             (char *)host, (char *)user, (char *)dbname, jport);
  } else {
    jniTrace("jobj:%p, conn:%p, connect to database succeed, host=%s, user=%s, dbname=%s, port=%d", jobj, (void *)ret,
             (char *)host, (char *)user, (char *)dbname, jport);
  }

  if (host != NULL) (*env)->ReleaseStringUTFChars(env, jhost, host);
  if (dbname != NULL) (*env)->ReleaseStringUTFChars(env, jdbName, dbname);
  if (user != NULL && user != tsDefaultUser) (*env)->ReleaseStringUTFChars(env, juser, user);
  if (pass != NULL && pass != tsDefaultPass) (*env)->ReleaseStringUTFChars(env, jpass, pass);

  return ret;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_executeQueryImp(JNIEnv *env, jobject jobj,
                                                                                jbyteArray jsql, jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if (jsql == NULL) {
    jniError("jobj:%p, conn:%p, sql is null", jobj, tscon);
    return JNI_SQL_NULL;
  }

  jsize len = (*env)->GetArrayLength(env, jsql);

  char *dst = (char *)calloc(1, sizeof(char) * (len + 1));
  if (dst == NULL) {
    jniError("jobj:%p, conn:%p, can not alloc memory", jobj, tscon);
    return JNI_OUT_OF_MEMORY;
  }

  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)dst);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  jniTrace("jobj:%p, conn:%p, sql:%s", jobj, tscon, dst);

  SSqlObj *pSql = taos_query(tscon, dst);
  int32_t  code = taos_errno(pSql);

  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code:%s, msg:%s", jobj, tscon, tstrerror(code), taos_errstr(pSql));
  } else {
    int32_t affectRows = 0;
    if (pSql->cmd.command == TSDB_SQL_INSERT) {
      affectRows = taos_affected_rows(pSql);
      jniTrace("jobj:%p, conn:%p, code:%s, affect rows:%d", jobj, tscon, tstrerror(code), affectRows);
    } else {
      jniTrace("jobj:%p, conn:%p, code:%s", jobj, tscon, tstrerror(code));
    }
  }

  free(dst);
  return (jlong)pSql;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrCodeImp(JNIEnv *env, jobject jobj, jlong con, jlong tres) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return (jint)TSDB_CODE_INVALID_CONNECTION;
  }

  if ((void *)tres == NULL) {
    jniError("jobj:%p, conn:%p, resultset is null", jobj, tscon);
    return JNI_RESULT_SET_NULL;
  }

  TAOS_RES *pSql = (TAOS_RES *)tres;

  return (jint)taos_errno(pSql);
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrMsgImp(JNIEnv *env, jobject jobj, jlong tres) {
  TAOS_RES *pSql = (TAOS_RES *)tres;
  return (*env)->NewStringUTF(env, (const char *)taos_errstr(pSql));
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getResultSetImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong tres) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if ((void *)tres == NULL) {
    jniError("jobj:%p, conn:%p, resultset is null", jobj, tscon);
    return JNI_RESULT_SET_NULL;
  }

  SSqlObj *pSql = (TAOS_RES *)tres;
  STscObj *pObj = pSql->pTscObj;

  if (tscIsUpdateQuery(pSql)) {
    taos_free_result(pSql);  // free result here
    jniTrace("jobj:%p, conn:%p, no resultset, %p", jobj, pObj, (void *)tres);
    return 0;
  } else {
    jniTrace("jobj:%p, conn:%p, get resultset, %p", jobj, pObj, (void *)tres);
    return tres;
  }
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_freeResultSetImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong res) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if ((void *)res == NULL) {
    jniError("jobj:%p, conn:%p, resultset is null", jobj, tscon);
    return JNI_RESULT_SET_NULL;
  }

  taos_free_result((void *)res);
  jniTrace("jobj:%p, conn:%p, free resultset:%p", jobj, tscon, (void *)res);
  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getAffectedRowsImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                  jlong res) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if ((void *)res == NULL) {
    jniError("jobj:%p, conn:%p, resultset is null", jobj, tscon);
    return JNI_RESULT_SET_NULL;
  }

  jint ret = taos_affected_rows((SSqlObj *)res);
  jniTrace("jobj:%p, conn:%p, sql:%p, affect rows:%d", jobj, tscon, (void *)con, res, ret);

  return ret;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getSchemaMetaDataImp(JNIEnv *env, jobject jobj,
                                                                                    jlong con, jlong res,
                                                                                    jobject arrayListObj) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_RES *result = (TAOS_RES *)res;
  if (result == NULL) {
    jniError("jobj:%p, conn:%p, resultset is null", jobj, tscon);
    return JNI_RESULT_SET_NULL;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);
  int         num_fields = taos_num_fields(result);

  // jobject arrayListObj = (*env)->NewObject(env, g_arrayListClass, g_arrayListConstructFp, "");

  if (num_fields == 0) {
    jniError("jobj:%p, conn:%p, resultset:%p, fields size is %d", jobj, tscon, res, num_fields);
    return JNI_NUM_OF_FIELDS_0;
  } else {
    jniTrace("jobj:%p, conn:%p, resultset:%p, fields size is %d", jobj, tscon, res, num_fields);
    for (int i = 0; i < num_fields; ++i) {
      jobject metadataObj = (*env)->NewObject(env, g_metadataClass, g_metadataConstructFp);
      (*env)->SetIntField(env, metadataObj, g_metadataColtypeField, fields[i].type);
      (*env)->SetIntField(env, metadataObj, g_metadataColsizeField, fields[i].bytes);
      (*env)->SetIntField(env, metadataObj, g_metadataColindexField, i);
      jstring metadataObjColname = (*env)->NewStringUTF(env, fields[i].name);
      (*env)->SetObjectField(env, metadataObj, g_metadataColnameField, metadataObjColname);
      (*env)->CallBooleanMethod(env, arrayListObj, g_arrayListAddFp, metadataObj);
    }
  }

  return JNI_SUCCESS;
}

/**
 *
 * @param env      vm
 * @param nchar    true multibytes data
 * @param maxBytes the maximum allowable field length
 * @return
 */
jstring jniFromNCharToByteArray(JNIEnv *env, char *nchar, int32_t maxBytes) {
  int len = (int)strlen(nchar);
  if (len > maxBytes) {  // no terminated symbol exists '\0'
    len = maxBytes;
  }

  jbyteArray bytes = (*env)->NewByteArray(env, len);
  (*env)->SetByteArrayRegion(env, bytes, 0, len, (jbyte *)nchar);
  return bytes;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_fetchRowImp(JNIEnv *env, jobject jobj, jlong con,
                                                                           jlong res, jobject rowobj) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_RES *result = (TAOS_RES *)res;
  if (result == NULL) {
    jniError("jobj:%p, conn:%p, resultset is null", jobj, tscon);
    return JNI_RESULT_SET_NULL;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);
  int         num_fields = taos_num_fields(result);

  if (num_fields == 0) {
    jniError("jobj:%p, conn:%p, resultset:%p, fields size is %d", jobj, tscon, res, num_fields);
    return JNI_NUM_OF_FIELDS_0;
  }

  TAOS_ROW row = taos_fetch_row(result);
  if (row == NULL) {
    int tserrno = taos_errno(result);
    if (tserrno == 0) {
      jniTrace("jobj:%p, conn:%p, resultset:%p, fields size is %d, fetch row to the end", jobj, tscon, res, num_fields);
      return JNI_FETCH_END;
    } else {
      jniTrace("jobj:%p, conn:%p, interruptted query", jobj, tscon);
      return JNI_RESULT_SET_NULL;
    }
  }

  char tmp[TSDB_MAX_BYTES_PER_ROW] = {0};

  for (int i = 0; i < num_fields; i++) {
    if (row[i] == NULL) {
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_BOOL:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetBooleanFp, i, (jboolean)(*((char *)row[i]) == 1));
        break;
      case TSDB_DATA_TYPE_TINYINT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetByteFp, i, (jbyte) * ((char *)row[i]));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetShortFp, i, (jshort) * ((short *)row[i]));
        break;
      case TSDB_DATA_TYPE_INT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetIntFp, i, (jint) * (int *)row[i]);
        break;
      case TSDB_DATA_TYPE_BIGINT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetLongFp, i, (jlong) * ((int64_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_FLOAT: {
        float fv = 0;
        fv = GET_FLOAT_VAL(row[i]);
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetFloatFp, i, (jfloat)fv);
      } break;
      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = 0;
        dv = GET_DOUBLE_VAL(row[i]);
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetDoubleFp, i, (jdouble)dv);
      } break;
      case TSDB_DATA_TYPE_BINARY: {
        strncpy(tmp, row[i], (size_t)fields[i].bytes);  // handle the case that terminated does not exist
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetStringFp, i, (*env)->NewStringUTF(env, tmp));

        memset(tmp, 0, (size_t)fields[i].bytes);
        break;
      }
      case TSDB_DATA_TYPE_NCHAR: {
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetByteArrayFp, i,
                               jniFromNCharToByteArray(env, (char *)row[i], fields[i].bytes));
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetTimestampFp, i, (jlong) * ((int64_t *)row[i]));
        break;
      default:
        break;
    }
  }

  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_closeConnectionImp(JNIEnv *env, jobject jobj,
                                                                                  jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is already closed", jobj);
    return JNI_CONNECTION_NULL;
  } else {
    jniTrace("jobj:%p, conn:%p, close connection success", jobj, tscon);
    taos_close(tscon);
    return JNI_SUCCESS;
  }
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_subscribeImp(JNIEnv *env, jobject jobj, jlong con,
                                                                             jboolean restart, jstring jtopic, jstring jsql, jint jinterval) {
  jlong sub = 0;
  TAOS *taos = (TAOS *)con;
  char *topic = NULL;
  char *sql = NULL;

  jniGetGlobalMethod(env);
  jniTrace("jobj:%p, in TSDBJNIConnector_subscribeImp", jobj);

  if (jtopic != NULL) {
    topic = (char *)(*env)->GetStringUTFChars(env, jtopic, NULL);
  }
  if (jsql != NULL) {
    sql = (char *)(*env)->GetStringUTFChars(env, jsql, NULL);
  }

  TAOS_SUB *tsub = taos_subscribe(taos, (int)restart, topic, sql, NULL, NULL, jinterval);
  sub = (jlong)tsub;

  if (sub == 0) {
    jniTrace("jobj:%p, failed to subscribe: topic:%s", jobj, jtopic);
  } else {
    jniTrace("jobj:%p, successfully subscribe: topic: %s", jobj, jtopic);
  }

  if (topic != NULL) (*env)->ReleaseStringUTFChars(env, jtopic, topic);
  if (sql != NULL) (*env)->ReleaseStringUTFChars(env, jsql, sql);

  return sub;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_consumeImp(JNIEnv *env, jobject jobj, jlong sub) {
  jniTrace("jobj:%p, in TSDBJNIConnector_consumeImp, sub:%ld", jobj, sub);
  jniGetGlobalMethod(env);

  TAOS_SUB *tsub = (TAOS_SUB *)sub;

  TAOS_RES *res = taos_consume(tsub);

  if (res == NULL) {
    jniError("jobj:%p, tsub:%p, taos_consume returns NULL", jobj, tsub);
    return 0l;
  }

  return (long)res;
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_unsubscribeImp(JNIEnv *env, jobject jobj, jlong sub,
                                                                              jboolean keepProgress) {
  TAOS_SUB *tsub = (TAOS_SUB *)sub;
  taos_unsubscribe(tsub, keepProgress);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_validateCreateTableSqlImp(JNIEnv *env, jobject jobj,
                                                                                         jlong con, jbyteArray jsql) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if (jsql == NULL) {
    jniError("jobj:%p, conn:%p, sql is null", jobj, tscon);
    return JNI_SQL_NULL;
  }

  jsize len = (*env)->GetArrayLength(env, jsql);

  char *dst = (char *)calloc(1, sizeof(char) * (len + 1));
  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)dst);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  int code = taos_validate_sql(tscon, dst);
  jniTrace("jobj:%p, conn:%p, code is %d", jobj, tscon, code);

  free(dst);
  return code;
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getTsCharset(JNIEnv *env, jobject jobj) {
  return (*env)->NewStringUTF(env, (const char *)tsCharset);
}

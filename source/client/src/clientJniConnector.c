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

#include "taos.h"

#include "com_taosdata_jdbc_TSDBJNIConnector.h"
#include "jniCommon.h"

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

jmethodID g_blockdataSetByteArrayFp;
jmethodID g_blockdataSetNumOfRowsFp;
jmethodID g_blockdataSetNumOfColsFp;

jclass    g_tmqClass;
jmethodID g_createConsumerErrorCallback;
jmethodID g_topicListCallback;

jclass g_consumerClass;
// deprecated
jmethodID g_commitCallback;

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
  g_rowdataSetTimestampFp = (*env)->GetMethodID(env, g_rowdataClass, "setTimestamp", "(IJI)V");
  g_rowdataSetByteArrayFp = (*env)->GetMethodID(env, g_rowdataClass, "setByteArray", "(I[B)V");
  (*env)->DeleteLocalRef(env, rowdataClass);

  jclass blockdataClass = (*env)->FindClass(env, "com/taosdata/jdbc/TSDBResultSetBlockData");
  jclass g_blockdataClass = (*env)->NewGlobalRef(env, blockdataClass);
  g_blockdataSetByteArrayFp = (*env)->GetMethodID(env, g_blockdataClass, "setByteArray", "([B)V");
  g_blockdataSetNumOfRowsFp = (*env)->GetMethodID(env, g_blockdataClass, "setNumOfRows", "(I)V");
  g_blockdataSetNumOfColsFp = (*env)->GetMethodID(env, g_blockdataClass, "setNumOfCols", "(I)V");
  (*env)->DeleteLocalRef(env, blockdataClass);

  jclass tmqClass = (*env)->FindClass(env, "com/taosdata/jdbc/tmq/TMQConnector");
  jclass g_tmqClass = (*env)->NewGlobalRef(env, tmqClass);
  g_createConsumerErrorCallback =
      (*env)->GetMethodID(env, g_tmqClass, "setCreateConsumerErrorMsg", "(Ljava/lang/String;)V");
  g_topicListCallback = (*env)->GetMethodID(env, g_tmqClass, "setTopicList", "([Ljava/lang/String;)V");
  (*env)->DeleteLocalRef(env, tmqClass);

  jclass consumerClass = (*env)->FindClass(env, "com/taosdata/jdbc/tmq/TaosConsumer");
  jclass g_consumerClass = (*env)->NewGlobalRef(env, consumerClass);
  g_commitCallback = (*env)->GetMethodID(env, g_consumerClass, "commitCallbackHandler", "(I)V");
  (*env)->DeleteLocalRef(env, consumerClass);

  atomic_store_32(&__init, 2);
  jniDebug("native method register finished");
}

int32_t check_for_params(jobject jobj, jlong conn, jlong res) {
  if ((TAOS *)conn == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if ((TAOS_RES *)res == NULL) {
    jniError("jobj:%p, conn:%p, param res is null", jobj, (TAOS *)conn);
    return JNI_RESULT_SET_NULL;
  }

  return JNI_SUCCESS;
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_initImp(JNIEnv *env, jobject jobj, jstring jconfigDir) {
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

JNIEXPORT jobject createTSDBException(JNIEnv *env, int code, char *msg) {
  // find class
  jclass exception_clazz = (*env)->FindClass(env, "com/taosdata/jdbc/TSDBException");
  // find methods
  jmethodID init_method = (*env)->GetMethodID(env, exception_clazz, "<init>", "()V");
  jmethodID setCode_method = (*env)->GetMethodID(env, exception_clazz, "setCode", "(I)V");
  jmethodID setMessage_method = (*env)->GetMethodID(env, exception_clazz, "setMessage", "(Ljava/lang/String;)V");
  // new exception
  jobject exception_obj = (*env)->NewObject(env, exception_clazz, init_method);
  // set code
  (*env)->CallVoidMethod(env, exception_obj, setCode_method, code);
  // set message
  jstring message = (*env)->NewStringUTF(env, msg);
  (*env)->CallVoidMethod(env, exception_obj, setMessage_method, message);

  return exception_obj;
}

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setConfigImp(JNIEnv *env, jclass jobj,
                                                                               jstring config) {
  if (config == NULL) {
    char *msg = "config value is null";
    jniDebug("config value is null");
    return createTSDBException(env, -1, msg);
  }

  const char *cfg = (*env)->GetStringUTFChars(env, config, NULL);
  if (!cfg) {
    char *msg = "config value is null";
    jniDebug("config value is null");
    return createTSDBException(env, -1, msg);
  }

  setConfRet result = taos_set_config(cfg);
  int        code = result.retCode;
  char      *msg = result.retMsg;

  return createTSDBException(env, code, msg);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setOptions(JNIEnv *env, jobject jobj, jint optionIndex,
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

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_connectImp(JNIEnv *env, jobject jobj, jstring jhost,
                                                                           jint jport, jstring jdbName, jstring juser,
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

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_executeQueryImp(JNIEnv *env, jobject jobj,
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

  char *str = (char *)taosMemoryCalloc(1, sizeof(char) * (len + 1));
  if (str == NULL) {
    jniError("jobj:%p, conn:%p, alloc memory failed", jobj, tscon);
    return JNI_OUT_OF_MEMORY;
  }

  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)str);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  TAOS_RES *tres = taos_query(tscon, str);
  int32_t   code = taos_errno(tres);

  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code:0x%x, msg:%s", jobj, tscon, code, taos_errstr(tres));
  } else {
    if (taos_is_update_query(tres)) {
      int32_t affectRows = taos_affected_rows(tres);
      jniDebug("jobj:%p, conn:%p, code:0x%x, affect rows:%d", jobj, tscon, code, affectRows);
    } else {
      jniDebug("jobj:%p, conn:%p, code:0x%x", jobj, tscon, code);
    }
  }

  taosMemoryFreeClear(str);
  return (jlong)tres;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_executeQueryWithReqId(JNIEnv *env, jobject jobj,
                                                                                      jbyteArray jsql, jlong con,
                                                                                      jlong reqId) {
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

  char *str = (char *)taosMemoryCalloc(1, sizeof(char) * (len + 1));
  if (str == NULL) {
    jniError("jobj:%p, conn:%p, alloc memory failed", jobj, tscon);
    return JNI_OUT_OF_MEMORY;
  }

  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)str);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  TAOS_RES *tres = taos_query_with_reqid(tscon, str, reqId);
  int32_t   code = taos_errno(tres);

  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code:0x%x, msg:%s", jobj, tscon, code, taos_errstr(tres));
  } else {
    if (taos_is_update_query(tres)) {
      int32_t affectRows = taos_affected_rows(tres);
      jniDebug("jobj:%p, conn:%p, code:0x%x, affect rows:%d", jobj, tscon, code, affectRows);
    } else {
      jniDebug("jobj:%p, conn:%p, code:0x%x", jobj, tscon, code);
    }
  }

  taosMemoryFreeClear(str);
  return (jlong)tres;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrCodeImp(JNIEnv *env, jobject jobj, jlong con,
                                                                             jlong tres) {
  int32_t code = check_for_params(jobj, con, tres);
  if (code != JNI_SUCCESS) {
    return code;
  }

  return (jint)taos_errno((TAOS_RES *)tres);
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrMsgImp(JNIEnv *env, jobject jobj, jlong tres) {
  TAOS_RES *pSql = (TAOS_RES *)tres;
  return (*env)->NewStringUTF(env, (const char *)taos_errstr(pSql));
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getResultSetImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong tres) {
  TAOS   *tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, tres);
  if (code != JNI_SUCCESS) {
    return code;
  }

  if (taos_is_update_query((TAOS_RES *)tres)) {
    jniDebug("jobj:%p, conn:%p, update query, no resultset, %p", jobj, tscon, (void *)tres);
  } else {
    jniDebug("jobj:%p, conn:%p, get resultset, %p", jobj, tscon, (void *)tres);
  }

  return tres;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_isUpdateQueryImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong tres) {
  int32_t code = check_for_params(jobj, con, tres);
  if (code != JNI_SUCCESS) {
    return code;
  }

  return (taos_is_update_query((TAOS_RES *)tres) ? 1 : 0);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_freeResultSetImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong res) {
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  jniDebug("jobj:%p, conn:%p, free resultset:%p", jobj, (TAOS *)con, (void *)res);
  taos_free_result((void *)res);
  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getAffectedRowsImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                  jlong res) {
  TAOS   *tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  jint ret = taos_affected_rows((TAOS_RES *)res);
  jniDebug("jobj:%p, conn:%p, sql:%p, res: %p, affect rows:%d", jobj, tscon, (TAOS *)con, (TAOS_RES *)res,
           (int32_t)ret);

  return ret;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getSchemaMetaDataImp(JNIEnv *env, jobject jobj,
                                                                                    jlong con, jlong res,
                                                                                    jobject arrayListObj) {
  TAOS   *tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  TAOS_RES   *tres = (TAOS_RES *)res;
  TAOS_FIELD *fields = taos_fetch_fields(tres);

  int32_t num_fields = taos_num_fields(tres);
  if (num_fields == 0) {
    jniError("jobj:%p, conn:%p, resultset:%p, fields size is %d", jobj, tscon, tres, num_fields);
    return JNI_NUM_OF_FIELDS_0;
  } else {
    jniDebug("jobj:%p, conn:%p, resultset:%p, fields size is %d", jobj, tscon, tres, num_fields);
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
  jbyteArray bytes = (*env)->NewByteArray(env, maxBytes);
  (*env)->SetByteArrayRegion(env, bytes, 0, maxBytes, (jbyte *)nchar);
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

  int32_t numOfFields = taos_num_fields(result);
  if (numOfFields == 0) {
    jniError("jobj:%p, conn:%p, resultset:%p, fields size %d", jobj, tscon, (void *)res, numOfFields);
    return JNI_NUM_OF_FIELDS_0;
  }

  TAOS_ROW row = taos_fetch_row(result);
  if (row == NULL) {
    int code = taos_errno(result);
    if (code == TSDB_CODE_SUCCESS) {
      jniDebug("jobj:%p, conn:%p, resultset:%p, fields size is %d, fetch row to the end", jobj, tscon, (void *)res,
               numOfFields);
      return JNI_FETCH_END;
    } else {
      jniDebug("jobj:%p, conn:%p, interrupted query. fetch row error code: 0x%x, msg:%s", jobj, tscon, code,
               taos_errstr(result));
      return JNI_RESULT_SET_NULL;
    }
  }

  int32_t *length = taos_fetch_lengths(result);

  char tmp[TSDB_MAX_BYTES_PER_ROW] = {0};

  for (int i = 0; i < numOfFields; i++) {
    if (row[i] == NULL) {
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_BOOL:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetBooleanFp, i, (jboolean)(*((char *)row[i]) == 1));
        break;
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_TINYINT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetByteFp, i, (jbyte) * ((int8_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_SMALLINT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetShortFp, i, (jshort) * ((int16_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_INT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetIntFp, i, (jint) * (int32_t *)row[i]);
        break;
      case TSDB_DATA_TYPE_UBIGINT:
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
      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_VARBINARY:
      case TSDB_DATA_TYPE_GEOMETRY: {
        memcpy(tmp, row[i], length[i]);  // handle the case that terminated does not exist
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetStringFp, i, (*env)->NewStringUTF(env, tmp));

        memset(tmp, 0, length[i]);
        break;
      }
      case TSDB_DATA_TYPE_NCHAR: {
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetByteArrayFp, i,
                               jniFromNCharToByteArray(env, (char *)row[i], length[i]));
        break;
      }
      case TSDB_DATA_TYPE_JSON: {
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetByteArrayFp, i,
                               jniFromNCharToByteArray(env, (char *)row[i], length[i]));
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP: {
        int precision = taos_result_precision(result);
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetTimestampFp, i, (jlong) * ((int64_t *)row[i]), precision);
        break;
      }
      default:
        break;
    }
  }

  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_fetchBlockImp(JNIEnv *env, jobject jobj, jlong con,
                                                                             jlong res, jobject rowobj) {
  TAOS   *tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  TAOS_RES *tres = (TAOS_RES *)res;

  int32_t numOfFields = taos_num_fields(tres);
  if (numOfFields <= 0) {
    jniError("jobj:%p, conn:%p, query interrupted. taos_num_fields error code: 0x%x, msg:%s", jobj, tscon,
             taos_errno(tres), taos_errstr(tres));
    return JNI_RESULT_SET_NULL;
  }

  void   *data = NULL;
  int32_t numOfRows = 0;
  int     error_code = taos_fetch_raw_block(tres, &numOfRows, &data);
  if (numOfRows == 0) {
    if (error_code == JNI_SUCCESS) {
      jniDebug("jobj:%p, conn:%p, resultset:%p, no data to retrieve", jobj, tscon, (void *)res);
      return JNI_FETCH_END;
    } else {
      jniError("jobj:%p, conn:%p, query interrupted. fetch block error code: 0x%x, msg:%s", jobj, tscon, error_code,
               taos_errstr(tres));
      return JNI_RESULT_SET_NULL;
    }
  }

  (*env)->CallVoidMethod(env, rowobj, g_blockdataSetNumOfRowsFp, (jint)numOfRows);
  (*env)->CallVoidMethod(env, rowobj, g_blockdataSetNumOfColsFp, (jint)numOfFields);

  int32_t len = *(int32_t *)(((char *)data) + 4);
  (*env)->CallVoidMethod(env, rowobj, g_blockdataSetByteArrayFp, jniFromNCharToByteArray(env, (char *)data, len));

  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_closeConnectionImp(JNIEnv *env, jobject jobj,
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

  char *str = (char *)taosMemoryCalloc(1, sizeof(char) * (len + 1));
  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)str);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  int code = taos_validate_sql(tscon, str);
  jniDebug("jobj:%p, conn:%p, code: 0x%x", jobj, tscon, code);

  taosMemoryFreeClear(str);
  return code;
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getTsCharset(JNIEnv *env, jobject jobj) {
  return (*env)->NewStringUTF(env, (const char *)tsCharset);
}

/**
 * Get Result Time Precision
 * @param env           vm
 * @param jobj          the TSDBJNIConnector java object
 * @param con           the c connection pointer
 * @param res           the TAOS_RES object, i.e. the SSqlObject
 * @return precision    0:ms 1:us 2:ns
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getResultTimePrecisionImp(JNIEnv *env, jobject jobj,
                                                                                         jlong con, jlong res) {
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

  return taos_result_precision(result);
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_prepareStmtImp(JNIEnv *env, jobject jobj,
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

  char *str = (char *)taosMemoryCalloc(1, sizeof(char) * (len + 1));
  if (str == NULL) {
    jniError("jobj:%p, conn:%p, alloc memory failed", jobj, tscon);
    return JNI_OUT_OF_MEMORY;
  }

  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)str);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  TAOS_STMT *pStmt = taos_stmt_init(tscon);
  int32_t    code = taos_stmt_prepare(pStmt, str, len);
  taosMemoryFreeClear(str);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("prepareStmt jobj:%p, conn:%p, code: 0x%x", jobj, tscon, code);
    return JNI_TDENGINE_ERROR;
  }

  return (jlong)pStmt;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_prepareStmtWithReqId(JNIEnv *env, jobject jobj,
                                                                                     jbyteArray jsql, jlong con,
                                                                                     jlong reqId) {
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

  char *str = (char *)taosMemoryCalloc(1, sizeof(char) * (len + 1));
  if (str == NULL) {
    jniError("jobj:%p, conn:%p, alloc memory failed", jobj, tscon);
    return JNI_OUT_OF_MEMORY;
  }

  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)str);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  TAOS_STMT *pStmt = taos_stmt_init_with_reqid(tscon, reqId);
  int32_t    code = taos_stmt_prepare(pStmt, str, len);
  taosMemoryFreeClear(str);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("prepareStmtWithReqId jobj:%p, conn:%p, code: 0x%x", jobj, tscon, code);
    return JNI_TDENGINE_ERROR;
  }

  return (jlong)pStmt;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setBindTableNameImp(JNIEnv *env, jobject jobj,
                                                                                   jlong stmt, jstring jname,
                                                                                   jlong conn) {
  TAOS *tsconn = (TAOS *)conn;
  if (tsconn == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_STMT *pStmt = (TAOS_STMT *)stmt;
  if (pStmt == NULL) {
    jniError("jobj:%p, conn:%p, invalid stmt handle", jobj, tsconn);
    return JNI_SQL_NULL;
  }

  const char *name = (*env)->GetStringUTFChars(env, jname, NULL);

  int32_t code = taos_stmt_set_tbname((void *)stmt, name);
  if (code != TSDB_CODE_SUCCESS) {
    (*env)->ReleaseStringUTFChars(env, jname, name);

    jniError("bindTableName jobj:%p, conn:%p, code: 0x%x", jobj, tsconn, code);
    return code;
  }

  jniDebug("jobj:%p, conn:%p, set stmt bind table name:%s", jobj, tsconn, name);
  (*env)->ReleaseStringUTFChars(env, jname, name);
  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setTableNameTagsImp(
    JNIEnv *env, jobject jobj, jlong stmt, jstring tableName, jint numOfTags, jbyteArray tags, jbyteArray typeList,
    jbyteArray lengthList, jbyteArray nullList, jlong conn) {
  TAOS *tsconn = (TAOS *)conn;
  if (tsconn == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_STMT *pStmt = (TAOS_STMT *)stmt;
  if (pStmt == NULL) {
    jniError("jobj:%p, conn:%p, invalid stmt handle", jobj, tsconn);
    return JNI_SQL_NULL;
  }

  jsize len = (*env)->GetArrayLength(env, tags);
  char *tagsData = (char *)taosMemoryCalloc(1, len);
  (*env)->GetByteArrayRegion(env, tags, 0, len, (jbyte *)tagsData);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  len = (*env)->GetArrayLength(env, lengthList);
  int32_t *lengthArray = (int32_t *)taosMemoryCalloc(1, len);
  (*env)->GetByteArrayRegion(env, lengthList, 0, len, (jbyte *)lengthArray);
  if ((*env)->ExceptionCheck(env)) {
  }

  len = (*env)->GetArrayLength(env, typeList);
  char *typeArray = (char *)taosMemoryCalloc(1, len);
  (*env)->GetByteArrayRegion(env, typeList, 0, len, (jbyte *)typeArray);
  if ((*env)->ExceptionCheck(env)) {
  }

  len = (*env)->GetArrayLength(env, nullList);
  char *nullArray = (char *)taosMemoryCalloc(1, len);
  (*env)->GetByteArrayRegion(env, nullList, 0, len, (jbyte *)nullArray);
  if ((*env)->ExceptionCheck(env)) {
  }

  const char *name = (*env)->GetStringUTFChars(env, tableName, NULL);
  char       *curTags = tagsData;

  TAOS_MULTI_BIND *tagsBind = taosMemoryCalloc(numOfTags, sizeof(TAOS_MULTI_BIND));
  for (int32_t i = 0; i < numOfTags; ++i) {
    tagsBind[i].buffer_type = typeArray[i];
    tagsBind[i].buffer = curTags;
    tagsBind[i].is_null = &nullArray[i];
    tagsBind[i].length = &lengthArray[i];

    curTags += lengthArray[i];
  }

  int32_t code = taos_stmt_set_tbname_tags((void *)stmt, name, tagsBind);

  int32_t nTags = (int32_t)numOfTags;
  jniDebug("jobj:%p, conn:%p, set table name:%s, numOfTags:%d", jobj, tsconn, name, nTags);

  taosMemoryFreeClear(tagsData);
  taosMemoryFreeClear(lengthArray);
  taosMemoryFreeClear(typeArray);
  taosMemoryFreeClear(nullArray);
  taosMemoryFreeClear(tagsBind);
  (*env)->ReleaseStringUTFChars(env, tableName, name);

  if (code != TSDB_CODE_SUCCESS) {
    jniError("tableNameTags jobj:%p, conn:%p, code: 0x%x", jobj, tsconn, code);
    return code;
  }
  return JNI_SUCCESS;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_bindColDataImp(
    JNIEnv *env, jobject jobj, jlong stmt, jbyteArray colDataList, jbyteArray lengthList, jbyteArray nullList,
    jint dataType, jint dataBytes, jint numOfRows, jint colIndex, jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_STMT *pStmt = (TAOS_STMT *)stmt;
  if (pStmt == NULL) {
    jniError("jobj:%p, conn:%p, invalid stmt", jobj, tscon);
    return JNI_SQL_NULL;
  }

  // todo refactor
  jsize len = (*env)->GetArrayLength(env, colDataList);
  char *colBuf = (char *)taosMemoryCalloc(1, len);
  (*env)->GetByteArrayRegion(env, colDataList, 0, len, (jbyte *)colBuf);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  len = (*env)->GetArrayLength(env, lengthList);
  char *lengthArray = (char *)taosMemoryCalloc(1, len);
  (*env)->GetByteArrayRegion(env, lengthList, 0, len, (jbyte *)lengthArray);
  if ((*env)->ExceptionCheck(env)) {
  }

  len = (*env)->GetArrayLength(env, nullList);
  char *nullArray = (char *)taosMemoryCalloc(1, len);
  (*env)->GetByteArrayRegion(env, nullList, 0, len, (jbyte *)nullArray);
  if ((*env)->ExceptionCheck(env)) {
  }

  // bind multi-rows with only one invoke.
  TAOS_MULTI_BIND *b = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND));

  b->num = numOfRows;
  b->buffer_type = dataType;  // todo check data type
  b->buffer_length = IS_VAR_DATA_TYPE(dataType) ? dataBytes : tDataTypes[dataType].bytes;
  b->is_null = nullArray;
  b->buffer = colBuf;
  b->length = (int32_t *)lengthArray;

  // set the length and is_null array
  if (!IS_VAR_DATA_TYPE(dataType)) {
    int32_t bytes = tDataTypes[dataType].bytes;
    for (int32_t i = 0; i < numOfRows; ++i) {
      b->length[i] = bytes;
    }
  }

  int32_t code = taos_stmt_bind_single_param_batch(pStmt, b, colIndex);
  taosMemoryFreeClear(b->length);
  taosMemoryFreeClear(b->buffer);
  taosMemoryFreeClear(b->is_null);
  taosMemoryFreeClear(b);

  if (code != TSDB_CODE_SUCCESS) {
    jniError("bindColData jobj:%p, conn:%p, code: 0x%x", jobj, tscon, code);
    return code;
  }

  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_addBatchImp(JNIEnv *env, jobject jobj, jlong stmt,
                                                                           jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_STMT *pStmt = (TAOS_STMT *)stmt;
  if (pStmt == NULL) {
    jniError("jobj:%p, conn:%p, invalid stmt", jobj, tscon);
    return JNI_SQL_NULL;
  }

  int32_t code = taos_stmt_add_batch(pStmt);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("add batch jobj:%p, conn:%p, code: 0x%x", jobj, tscon, code);
    return code;
  }

  jniDebug("jobj:%p, conn:%p, stmt closed", jobj, tscon);
  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_executeBatchImp(JNIEnv *env, jobject jobj, jlong stmt,
                                                                               jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_STMT *pStmt = (TAOS_STMT *)stmt;
  if (pStmt == NULL) {
    jniError("jobj:%p, conn:%p, invalid stmt", jobj, tscon);
    return JNI_SQL_NULL;
  }

  int32_t code = taos_stmt_execute(pStmt);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("excute batch jobj:%p, conn:%p, code: 0x%x", jobj, tscon, code);
    return code;
  }

  jniDebug("jobj:%p, conn:%p, batch execute", jobj, tscon);
  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_closeStmt(JNIEnv *env, jobject jobj, jlong stmt,
                                                                         jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_STMT *pStmt = (TAOS_STMT *)stmt;
  if (pStmt == NULL) {
    jniError("jobj:%p, conn:%p, invalid stmt", jobj, tscon);
    return JNI_SQL_NULL;
  }

  int32_t code = taos_stmt_close(pStmt);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("close stmt jobj:%p, conn:%p, code: 0x%x", jobj, tscon, code);
    return code;
  }

  jniDebug("jobj:%p, conn:%p, stmt closed", jobj, tscon);
  return JNI_SUCCESS;
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_stmtErrorMsgImp(JNIEnv *env, jobject jobj, jlong stmt,
                                                                                  jlong con) {
  char  errMsg[128];
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    sprintf(errMsg, "jobj:%p, connection already closed", jobj);
    return (*env)->NewStringUTF(env, errMsg);
  }

  TAOS_STMT *pStmt = (TAOS_STMT *)stmt;
  if (pStmt == NULL) {
    jniError("jobj:%p, conn:%p, invalid stmt", jobj, tscon);
    sprintf(errMsg, "jobj:%p, conn:%p, invalid stmt", jobj, tscon);
    return (*env)->NewStringUTF(env, errMsg);
  }

  return (*env)->NewStringUTF(env, taos_stmt_errstr((TAOS_STMT *)stmt));
}

TAOS_RES *schemalessInsert(JNIEnv *env, jobject jobj, jobjectArray lines, TAOS *taos, jint protocol, jint precision) {
  int    numLines = (*env)->GetArrayLength(env, lines);
  char **c_lines = taosMemoryCalloc(numLines, sizeof(char *));
  if (c_lines == NULL) {
    jniError("c_lines:%p, alloc memory failed", c_lines);
    return NULL;
  }
  for (int i = 0; i < numLines; ++i) {
    jstring line = (jstring)((*env)->GetObjectArrayElement(env, lines, i));
    c_lines[i] = (char *)(*env)->GetStringUTFChars(env, line, 0);
  }

  TAOS_RES *tres = taos_schemaless_insert(taos, c_lines, numLines, protocol, precision);

  for (int i = 0; i < numLines; ++i) {
    jstring line = (jstring)((*env)->GetObjectArrayElement(env, lines, i));
    (*env)->ReleaseStringUTFChars(env, line, c_lines[i]);
  }

  taosMemoryFreeClear(c_lines);
  return tres;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_insertLinesImp(JNIEnv *env, jobject jobj,
                                                                              jobjectArray lines, jlong conn,
                                                                              jint protocol, jint precision) {
  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_RES *tres = schemalessInsert(env, jobj, lines, taos, protocol, precision);

  int code = taos_errno(tres);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code: 0x%x, msg:%s", jobj, taos, code, taos_errstr(tres));
    taos_free_result(tres);
    return JNI_TDENGINE_ERROR;
  }
  taos_free_result(tres);

  return JNI_SUCCESS;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertImp(JNIEnv *env, jobject jobj,
                                                                                    jobjectArray lines, jlong conn,
                                                                                    jint protocol, jint precision) {
  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  TAOS_RES *tres = schemalessInsert(env, jobj, lines, taos, protocol, precision);

  return (jlong)tres;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertWithReqId(
    JNIEnv *env, jobject jobj, jlong conn, jobjectArray lines, jint protocol, jint precision, jlong reqId) {
  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  int    numLines = (*env)->GetArrayLength(env, lines);
  char **c_lines = taosMemoryCalloc(numLines, sizeof(char *));
  if (c_lines == NULL) {
    jniError("c_lines:%p, alloc memory failed", c_lines);
    return JNI_OUT_OF_MEMORY;
  }

  for (int i = 0; i < numLines; ++i) {
    jstring line = (jstring)((*env)->GetObjectArrayElement(env, lines, i));
    c_lines[i] = (char *)(*env)->GetStringUTFChars(env, line, 0);
  }

  TAOS_RES *tres = taos_schemaless_insert_with_reqid(taos, c_lines, numLines, protocol, precision, reqId);

  for (int i = 0; i < numLines; ++i) {
    jstring line = (jstring)((*env)->GetObjectArrayElement(env, lines, i));
    (*env)->ReleaseStringUTFChars(env, line, c_lines[i]);
  }

  taosMemoryFreeClear(c_lines);

  return (jlong)tres;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertWithTtl(JNIEnv *env, jobject jobj,
                                                                                        jlong conn, jobjectArray lines,
                                                                                        jint protocol, jint precision,
                                                                                        jint ttl) {
  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  int    numLines = (*env)->GetArrayLength(env, lines);
  char **c_lines = taosMemoryCalloc(numLines, sizeof(char *));
  if (c_lines == NULL) {
    jniError("c_lines:%p, alloc memory failed", c_lines);
    return JNI_OUT_OF_MEMORY;
  }

  for (int i = 0; i < numLines; ++i) {
    jstring line = (jstring)((*env)->GetObjectArrayElement(env, lines, i));
    c_lines[i] = (char *)(*env)->GetStringUTFChars(env, line, 0);
  }

  TAOS_RES *tres = taos_schemaless_insert_ttl(taos, c_lines, numLines, protocol, precision, ttl);

  for (int i = 0; i < numLines; ++i) {
    jstring line = (jstring)((*env)->GetObjectArrayElement(env, lines, i));
    (*env)->ReleaseStringUTFChars(env, line, c_lines[i]);
  }

  taosMemoryFreeClear(c_lines);

  return (jlong)tres;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertWithTtlAndReqId(
    JNIEnv *env, jobject jobj, jlong conn, jobjectArray lines, jint protocol, jint precision, jint ttl, jlong reqId) {
  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  int    numLines = (*env)->GetArrayLength(env, lines);
  char **c_lines = taosMemoryCalloc(numLines, sizeof(char *));
  if (c_lines == NULL) {
    jniError("c_lines:%p, alloc memory failed", c_lines);
    return JNI_OUT_OF_MEMORY;
  }

  for (int i = 0; i < numLines; ++i) {
    jstring line = (jstring)((*env)->GetObjectArrayElement(env, lines, i));
    c_lines[i] = (char *)(*env)->GetStringUTFChars(env, line, 0);
  }

  TAOS_RES *tres = taos_schemaless_insert_ttl_with_reqid(taos, c_lines, numLines, protocol, precision, ttl, reqId);

  for (int i = 0; i < numLines; ++i) {
    jstring line = (jstring)((*env)->GetObjectArrayElement(env, lines, i));
    (*env)->ReleaseStringUTFChars(env, line, c_lines[i]);
  }

  taosMemoryFreeClear(c_lines);

  return (jlong)tres;
}

JNIEXPORT jobject createSchemalessResp(JNIEnv *env, int totalRows, int code, const char *msg) {
  // find class
  jclass schemaless_clazz = (*env)->FindClass(env, "com/taosdata/jdbc/SchemalessResp");
  // find methods
  jmethodID init_method = (*env)->GetMethodID(env, schemaless_clazz, "<init>", "()V");
  jmethodID setCode_method = (*env)->GetMethodID(env, schemaless_clazz, "setCode", "(I)V");
  jmethodID setMsg_method = (*env)->GetMethodID(env, schemaless_clazz, "setMsg", "(Ljava/lang/String;)V");
  jmethodID setTotalRows_method = (*env)->GetMethodID(env, schemaless_clazz, "setTotalRows", "(I)V");
  // new schemaless
  jobject schemaless_obj = (*env)->NewObject(env, schemaless_clazz, init_method);
  // set code
  (*env)->CallVoidMethod(env, schemaless_obj, setCode_method, code);
  // set totalRows
  (*env)->CallVoidMethod(env, schemaless_obj, setTotalRows_method, totalRows);
  // set message
  jstring message = (*env)->NewStringUTF(env, msg);
  (*env)->CallVoidMethod(env, schemaless_obj, setMsg_method, message);

  return schemaless_obj;
}

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertRaw(JNIEnv *env, jobject jobj,
                                                                                      jlong conn, jstring data,
                                                                                      jint protocol, jint precision) {
  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    char *msg = "JNI connection is NULL";
    return createSchemalessResp(env, 0, JNI_CONNECTION_NULL, msg);
  }

  char     *line = (char *)(*env)->GetStringUTFChars(env, data, NULL);
  jint      len = (*env)->GetStringUTFLength(env, data);
  int32_t   totalRows;
  TAOS_RES *tres = taos_schemaless_insert_raw(taos, line, len, &totalRows, protocol, precision);

  (*env)->ReleaseStringUTFChars(env, data, line);

  // if (tres == NULL) {
  //   jniError("jobj:%p, schemaless raw insert failed", jobj);
  //   char *msg = "JNI schemaless raw insert return null";
  //   return createSchemalessResp(env, 0, JNI_TDENGINE_ERROR, msg);
  // }

  int code = taos_errno(tres);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code: 0x%x, msg:%s", jobj, taos, code, taos_errstr(tres));
    jobject jobject = createSchemalessResp(env, 0, code, taos_errstr(tres));
    taos_free_result(tres);
    return jobject;
  }
  taos_free_result(tres);

  return createSchemalessResp(env, totalRows, JNI_SUCCESS, NULL);
}

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertRawWithReqId(
    JNIEnv *env, jobject jobj, jlong conn, jstring data, jint protocol, jint precision, jlong reqId) {
  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    char *msg = "JNI connection is NULL";
    return createSchemalessResp(env, 0, JNI_CONNECTION_NULL, msg);
  }

  char     *line = (char *)(*env)->GetStringUTFChars(env, data, NULL);
  jint      len = (*env)->GetStringUTFLength(env, data);
  int32_t   totalRows;
  TAOS_RES *tres = taos_schemaless_insert_raw_with_reqid(taos, line, len, &totalRows, protocol, precision, reqId);

  (*env)->ReleaseStringUTFChars(env, data, line);

  int code = taos_errno(tres);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code: 0x%x, msg:%s", jobj, taos, code, taos_errstr(tres));
    jobject jobject = createSchemalessResp(env, 0, code, taos_errstr(tres));
    taos_free_result(tres);
    return jobject;
  }
  taos_free_result(tres);

  return createSchemalessResp(env, totalRows, JNI_SUCCESS, NULL);
}

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertRawWithTtl(JNIEnv *env, jobject jobj,
                                                                                             jlong conn, jstring data,
                                                                                             jint protocol,
                                                                                             jint precision, jint ttl) {
  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    char *msg = "JNI connection is NULL";
    return createSchemalessResp(env, 0, JNI_CONNECTION_NULL, msg);
  }

  char     *line = (char *)(*env)->GetStringUTFChars(env, data, NULL);
  jint      len = (*env)->GetStringUTFLength(env, data);
  int32_t   totalRows;
  TAOS_RES *tres = taos_schemaless_insert_raw_ttl(taos, line, len, &totalRows, protocol, precision, ttl);

  (*env)->ReleaseStringUTFChars(env, data, line);

  int code = taos_errno(tres);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code: 0x%x, msg:%s", jobj, taos, code, taos_errstr(tres));
    jobject jobject = createSchemalessResp(env, 0, code, taos_errstr(tres));
    taos_free_result(tres);
    return jobject;
  }
  taos_free_result(tres);

  return createSchemalessResp(env, totalRows, JNI_SUCCESS, NULL);
}

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertRawWithTtlAndReqId(
    JNIEnv *env, jobject jobj, jlong conn, jstring data, jint protocol, jint precision, jint ttl, jlong reqId) {
  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    char *msg = "JNI connection is NULL";
    return createSchemalessResp(env, 0, JNI_CONNECTION_NULL, msg);
  }

  char     *line = (char *)(*env)->GetStringUTFChars(env, data, NULL);
  jint      len = (*env)->GetStringUTFLength(env, data);
  int32_t   totalRows;
  TAOS_RES *tres =
      taos_schemaless_insert_raw_ttl_with_reqid(taos, line, len, &totalRows, protocol, precision, ttl, reqId);

  (*env)->ReleaseStringUTFChars(env, data, line);

  int code = taos_errno(tres);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code: 0x%x, msg:%s", jobj, taos, code, taos_errstr(tres));
    jobject jobject = createSchemalessResp(env, 0, code, taos_errstr(tres));
    taos_free_result(tres);
    return jobject;
  }
  taos_free_result(tres);

  return createSchemalessResp(env, totalRows, JNI_SUCCESS, NULL);
}

// TABLE_VG_ID_FID_CACHE cache resp object for getTableVgID
typedef struct TABLE_VG_ID_FIELD_CACHE {
  int      cached;
  jclass   clazz;
  jfieldID codeField;
  jfieldID vgIDField;
} TABLE_VG_ID_FIELD_CACHE;

TABLE_VG_ID_FIELD_CACHE tableVgIdFieldCache;

void cacheTableVgIDField(JNIEnv *env, jobject jobj) {
  if (tableVgIdFieldCache.cached) {
    return;
  }

  tableVgIdFieldCache.clazz = (*env)->GetObjectClass(env, jobj);
  tableVgIdFieldCache.codeField = (*env)->GetFieldID(env, tableVgIdFieldCache.clazz, "code", "I");
  tableVgIdFieldCache.vgIDField = (*env)->GetFieldID(env, tableVgIdFieldCache.clazz, "vgID", "I");
  tableVgIdFieldCache.cached = 1;
}

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getTableVgID(JNIEnv *env, jobject jobj, jlong conn,
                                                                               jstring jdb, jstring jtable,
                                                                               jobject resp) {
  if (!tableVgIdFieldCache.cached) {
    cacheTableVgIDField(env, resp);
  }

  TAOS *taos = (TAOS *)conn;
  if (taos == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    (*env)->SetIntField(env, resp, tableVgIdFieldCache.codeField, JNI_CONNECTION_NULL);
    return resp;
  }

  const char *db = NULL;
  const char *table = NULL;
  int         vgID = 0;

  if (jdb != NULL) {
    db = (*env)->GetStringUTFChars(env, jdb, NULL);
  }
  if (jtable != NULL) {
    table = (*env)->GetStringUTFChars(env, jtable, NULL);
  }

  int code = taos_get_table_vgId(taos, db, table, &vgID);
  if (db != NULL) {
    (*env)->ReleaseStringUTFChars(env, jdb, db);
  }
  if (table != NULL) {
    (*env)->ReleaseStringUTFChars(env, jtable, table);
  }

  (*env)->SetIntField(env, resp, tableVgIdFieldCache.codeField, code);
  (*env)->SetIntField(env, resp, tableVgIdFieldCache.vgIDField, vgID);
  return resp;
}

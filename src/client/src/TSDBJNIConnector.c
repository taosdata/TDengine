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

#include "com_taosdata_jdbc_TSDBJNIConnector.h"

#define jniFatal(...) { if (jniDebugFlag & DEBUG_FATAL) { taosPrintLog("JNI FATAL ", tscEmbedded ? 255 : jniDebugFlag, __VA_ARGS__); }}
#define jniError(...) { if (jniDebugFlag & DEBUG_ERROR) { taosPrintLog("JNI ERROR ", tscEmbedded ? 255 : jniDebugFlag, __VA_ARGS__); }}
#define jniWarn(...)  { if (jniDebugFlag & DEBUG_WARN)  { taosPrintLog("JNI WARN ", tscEmbedded ? 255 : jniDebugFlag, __VA_ARGS__); }}
#define jniInfo(...)  { if (jniDebugFlag & DEBUG_INFO)  { taosPrintLog("JNI ", tscEmbedded ? 255 : jniDebugFlag, __VA_ARGS__); }}
#define jniDebug(...) { if (jniDebugFlag & DEBUG_DEBUG) { taosPrintLog("JNI ", jniDebugFlag, __VA_ARGS__); }}
#define jniTrace(...) { if (jniDebugFlag & DEBUG_TRACE) { taosPrintLog("JNI ", jniDebugFlag, __VA_ARGS__); }}

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

#define JNI_SUCCESS          0
#define JNI_TDENGINE_ERROR  -1
#define JNI_CONNECTION_NULL -2
#define JNI_RESULT_SET_NULL -3
#define JNI_NUM_OF_FIELDS_0 -4
#define JNI_SQL_NULL        -5
#define JNI_FETCH_END       -6
#define JNI_OUT_OF_MEMORY   -7

static void jniGetGlobalMethod(JNIEnv *env) {
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

  jclass blockdataClass = (*env)->FindClass(env, "com/taosdata/jdbc/TSDBResultSetBlockData");
  jclass g_blockdataClass = (*env)->NewGlobalRef(env, blockdataClass);
  g_blockdataSetByteArrayFp = (*env)->GetMethodID(env, g_blockdataClass, "setByteArray", "(II[B)V");
  g_blockdataSetNumOfRowsFp = (*env)->GetMethodID(env, g_blockdataClass, "setNumOfRows", "(I)V");
  g_blockdataSetNumOfColsFp = (*env)->GetMethodID(env, g_blockdataClass, "setNumOfCols", "(I)V");
  (*env)->DeleteLocalRef(env, blockdataClass);

  atomic_store_32(&__init, 2);
  jniDebug("native method register finished");
}

static int32_t check_for_params(jobject jobj, jlong conn, jlong res) {
  if ((TAOS*) conn == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if ((TAOS_RES *) res == NULL) {
    jniError("jobj:%p, conn:%p, res is null", jobj, (TAOS*) conn);
    return JNI_RESULT_SET_NULL;
  }

  return JNI_SUCCESS;
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
    if (confDir && strlen(confDir) != 0) {
      tstrncpy(configDir, confDir, TSDB_FILENAME_LEN);
    }
    (*env)->ReleaseStringUTFChars(env, jconfigDir, confDir);
  }

  jniGetGlobalMethod(env);
  jniDebug("jni initialized successfully, config directory: %s", configDir);
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
  jlong       ret  = 0;
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

  ret = (jlong) taos_connect((char *)host, (char *)user, (char *)pass, (char *)dbname, (uint16_t)jport);
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

  char *str = (char *) calloc(1, sizeof(char) * (len + 1));
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
  return (jlong) pSql;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrCodeImp(JNIEnv *env, jobject jobj, jlong con, jlong tres) {
  int32_t code = check_for_params(jobj, con, tres);
  if (code != JNI_SUCCESS) {
    return code;
  }

  return (jint)taos_errno((TAOS_RES*) tres);
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrMsgImp(JNIEnv *env, jobject jobj, jlong tres) {
  TAOS_RES *pSql = (TAOS_RES *)tres;
  return (*env)->NewStringUTF(env, (const char *)taos_errstr(pSql));
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getResultSetImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong tres) {
  TAOS *tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, tres);
  if (code != JNI_SUCCESS) {
    return code;
  }

  SSqlObj *pSql = (TAOS_RES *)tres;
  if (tscIsUpdateQuery(pSql)) {
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

  SSqlObj *pSql = (TAOS_RES *)tres;

  return (tscIsUpdateQuery(pSql)? 1:0);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_freeResultSetImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong res) {
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  taos_free_result((void *)res);
  jniDebug("jobj:%p, conn:%p, free resultset:%p", jobj, (TAOS*) con, (void *)res);

  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getAffectedRowsImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                  jlong res) {
  TAOS *tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  jint ret = taos_affected_rows((SSqlObj *)res);
  jniDebug("jobj:%p, conn:%p, sql:%p, res: %p, affect rows:%d", jobj, tscon, (TAOS *)con, (TAOS_RES *)res, (int32_t)ret);

  return ret;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getSchemaMetaDataImp(JNIEnv *env, jobject jobj,
                                                                                    jlong con, jlong res,
                                                                                    jobject arrayListObj) {
  TAOS *tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  TAOS_RES* tres = (TAOS_RES*) res;
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
    jniError("jobj:%p, conn:%p, resultset:%p, fields size %d", jobj, tscon, (void*)res, numOfFields);
    return JNI_NUM_OF_FIELDS_0;
  }

  TAOS_ROW row = taos_fetch_row(result);
  if (row == NULL) {
    int code = taos_errno(result);
    if (code == TSDB_CODE_SUCCESS) {
      jniDebug("jobj:%p, conn:%p, resultset:%p, fields size is %d, fetch row to the end", jobj, tscon, (void*)res, numOfFields);
      return JNI_FETCH_END;
    } else {
      jniDebug("jobj:%p, conn:%p, interrupted query", jobj, tscon);
      return JNI_RESULT_SET_NULL;
    }
  }

  int32_t* length = taos_fetch_lengths(result);

  char tmp[TSDB_MAX_BYTES_PER_ROW] = {0};

  for (int i = 0; i < numOfFields; i++) {
    if (row[i] == NULL) {
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_BOOL:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetBooleanFp, i, (jboolean)(*((char *)row[i]) == 1));
        break;
      case TSDB_DATA_TYPE_TINYINT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetByteFp, i, (jbyte) * ((int8_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetShortFp, i, (jshort) * ((int16_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_INT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetIntFp, i, (jint) * (int32_t *)row[i]);
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
      case TSDB_DATA_TYPE_TIMESTAMP:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetTimestampFp, i, (jlong) * ((int64_t *)row[i]));
        break;
      default:
        break;
    }
  }

  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_fetchBlockImp(JNIEnv *env, jobject jobj, jlong con,
                                                                           jlong res, jobject rowobj) {
  TAOS *  tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  TAOS_RES *  tres = (TAOS_RES *)res;
  TAOS_FIELD *fields = taos_fetch_fields(tres);

  int32_t numOfFields = taos_num_fields(tres);
  assert(numOfFields > 0);

  TAOS_ROW row = NULL;
  int32_t  numOfRows = taos_fetch_block(tres, &row);
  if (numOfRows == 0) {
    code = taos_errno(tres);
    if (code == JNI_SUCCESS) {
      jniDebug("jobj:%p, conn:%p, resultset:%p, numOfFields:%d, no data to retrieve", jobj, tscon, (void *)res,
               numOfFields);
      return JNI_FETCH_END;
    } else {
      jniDebug("jobj:%p, conn:%p, query interrupted", jobj, tscon);
      return JNI_RESULT_SET_NULL;
    }
  }

  (*env)->CallVoidMethod(env, rowobj, g_blockdataSetNumOfRowsFp, (jint)numOfRows);
  (*env)->CallVoidMethod(env, rowobj, g_blockdataSetNumOfColsFp, (jint)numOfFields);

  for (int i = 0; i < numOfFields; i++) {
    (*env)->CallVoidMethod(env, rowobj, g_blockdataSetByteArrayFp, i, fields[i].bytes * numOfRows,
                           jniFromNCharToByteArray(env, (char *)row[i], fields[i].bytes * numOfRows));
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
    jniDebug("jobj:%p, conn:%p, close connection success", jobj, tscon);
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
  jniDebug("jobj:%p, in TSDBJNIConnector_subscribeImp", jobj);

  if (jtopic != NULL) {
    topic = (char *)(*env)->GetStringUTFChars(env, jtopic, NULL);
  }
  if (jsql != NULL) {
    sql = (char *)(*env)->GetStringUTFChars(env, jsql, NULL);
  }

  if (topic == NULL || sql == NULL) {
    jniDebug("jobj:%p, invalid argument: topic or sql is NULL", jobj);
    return sub;
  }

  TAOS_SUB *tsub = taos_subscribe(taos, (int)restart, topic, sql, NULL, NULL, jinterval);
  sub = (jlong)tsub;

  if (sub == 0) {
    jniDebug("jobj:%p, failed to subscribe: topic:%s", jobj, topic);
  } else {
    jniDebug("jobj:%p, successfully subscribe: topic: %s", jobj, topic);
  }

  (*env)->ReleaseStringUTFChars(env, jtopic, topic);
  (*env)->ReleaseStringUTFChars(env, jsql, sql);

  return sub;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_consumeImp(JNIEnv *env, jobject jobj, jlong sub) {
  jniDebug("jobj:%p, in TSDBJNIConnector_consumeImp, sub:%lld", jobj, sub);
  jniGetGlobalMethod(env);

  TAOS_SUB *tsub = (TAOS_SUB *)sub;
  TAOS_RES *res = taos_consume(tsub);

  if (res == NULL) {
    jniError("jobj:%p, tsub:%p, taos_consume returns NULL", jobj, tsub);
    return 0l;
  }

  return (jlong)res;
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

  char *str = (char *)calloc(1, sizeof(char) * (len + 1));
  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)str);
  if ((*env)->ExceptionCheck(env)) {
    // todo handle error
  }

  int code = taos_validate_sql(tscon, str);
  jniDebug("jobj:%p, conn:%p, code is %d", jobj, tscon, code);

  free(str);
  return code;
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getTsCharset(JNIEnv *env, jobject jobj) {
  return (*env)->NewStringUTF(env, (const char *)tsCharset);
}

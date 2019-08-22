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

#include <stdbool.h>

#include "os.h"
#include "com_taosdata_jdbc_TSDBJNIConnector.h"
#include "taos.h"
#include "tlog.h"
#include "tsclient.h"
#include "tscUtil.h"

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
  if (__sync_val_compare_and_swap_32(&__init, 0, 1) == 1) {
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

  jniTrace("native method register finished");
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
    const char *timezone = (*env)->GetStringUTFChars(env, optionValue, NULL);
    if (timezone && strlen(timezone) != 0) {
      res = taos_options(TSDB_OPTION_TIMEZONE, timezone);
      jniTrace("set timezone to %s, result:%d", timezone, res);
    } else {
      jniTrace("input timezone is empty");
    }
    (*env)->ReleaseStringUTFChars(env, optionValue, timezone);
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
    jniError("jobj:%p, taos:%p, connect to tdengine failed, host=%s, user=%s, dbname=%s, port=%d", jobj, (void *)ret,
             (char *)host, (char *)user, (char *)dbname, jport);
  } else {
    jniTrace("jobj:%p, taos:%p, connect to tdengine succeed, host=%s, user=%s, dbname=%s, port=%d", jobj, (void *)ret,
             (char *)host, (char *)user, (char *)dbname, jport);
  }

  if (host != NULL) (*env)->ReleaseStringUTFChars(env, jhost, host);
  if (dbname != NULL) (*env)->ReleaseStringUTFChars(env, jdbName, dbname);
  if (user != NULL && user != tsDefaultUser) (*env)->ReleaseStringUTFChars(env, juser, user);
  if (pass != NULL && pass != tsDefaultPass) (*env)->ReleaseStringUTFChars(env, jpass, pass);

  return ret;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_executeQueryImp(JNIEnv *env, jobject jobj,
                                                                               jbyteArray jsql, jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if (jsql == NULL) {
    jniError("jobj:%p, taos:%p, sql is null", jobj, tscon);
    return JNI_SQL_NULL;
  }

  jsize len = (*env)->GetArrayLength(env, jsql);

  char *dst = (char *)calloc(1, sizeof(char) * (len + 1));
  if (dst == NULL) {
    return JNI_OUT_OF_MEMORY;
  }

  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)dst);
  if ((*env)->ExceptionCheck(env)) {
    //todo handle error
  }

  int code = taos_query(tscon, dst);
  if (code != 0) {
    jniError("jobj:%p, taos:%p, code:%d, msg:%s, sql:%s", jobj, tscon, code, taos_errstr(tscon), dst);
    free(dst);
    return JNI_TDENGINE_ERROR;
  } else {
    int32_t  affectRows = 0;
    SSqlObj *pSql = ((STscObj *)tscon)->pSql;

    if (pSql->cmd.command == TSDB_SQL_INSERT) {
      affectRows = taos_affected_rows(tscon);
      jniTrace("jobj:%p, taos:%p, code:%d, affect rows:%d, sql:%s", jobj, tscon, code, affectRows, dst);
    } else {
      jniTrace("jobj:%p, taos:%p, code:%d, sql:%s", jobj, tscon, code, dst);
    }

    free(dst);
    return affectRows;
  }
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrCodeImp(JNIEnv *env, jobject jobj, jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return (jint)-TSDB_CODE_INVALID_CONNECTION;
  }

  return (jint)-taos_errno(tscon);
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrMsgImp(JNIEnv *env, jobject jobj, jlong con) {
  TAOS *tscon = (TAOS *)con;
  return (*env)->NewStringUTF(env, (const char *)taos_errstr(tscon));
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getResultSetImp(JNIEnv *env, jobject jobj, jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  int num_fields = taos_field_count(tscon);
  if (num_fields != 0) {
    jlong ret = (jlong)taos_use_result(tscon);
    jniTrace("jobj:%p, taos:%p, get resultset:%p", jobj, tscon, (void *)ret);
    return ret;
  }

  jniError("jobj:%p, taos:%p, no resultset", jobj, tscon);
  return 0;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_freeResultSetImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong res) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if ((void *)res == NULL) {
    jniError("jobj:%p, taos:%p, resultset is null", jobj, tscon);
    return JNI_RESULT_SET_NULL;
  }

  taos_free_result((void *)res);
  jniTrace("jobj:%p, taos:%p, free resultset:%p", jobj, tscon, (void *)res);
  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getAffectedRowsImp(JNIEnv *env, jobject jobj,
                                                                                  jlong con) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  jint ret = taos_affected_rows(tscon);

  jniTrace("jobj:%p, taos:%p, affect rows:%d", jobj, tscon, (void *)con, ret);

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
    jniError("jobj:%p, taos:%p, resultset is null", jobj, tscon);
    return JNI_RESULT_SET_NULL;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);
  int         num_fields = taos_num_fields(result);

  // jobject arrayListObj = (*env)->NewObject(env, g_arrayListClass, g_arrayListConstructFp, "");

  if (num_fields == 0) {
    jniError("jobj:%p, taos:%p, resultset:%p, fields size is %d", jobj, tscon, res, num_fields);
    return JNI_NUM_OF_FIELDS_0;
  } else {
    jniTrace("jobj:%p, taos:%p, resultset:%p, fields size is %d", jobj, tscon, res, num_fields);
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
  int        len = (int)strlen(nchar);
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
    jniError("jobj:%p, taos:%p, resultset is null", jobj, tscon);
    return JNI_RESULT_SET_NULL;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);
  int         num_fields = taos_num_fields(result);

  if (num_fields == 0) {
    jniError("jobj:%p, taos:%p, resultset:%p, fields size is %d", jobj, tscon, res, num_fields);
    return JNI_NUM_OF_FIELDS_0;
  }

  TAOS_ROW row = taos_fetch_row(result);
  if (row == NULL) {
    jniTrace("jobj:%p, taos:%p, resultset:%p, fields size is %d, fetch row to the end", jobj, tscon, res, num_fields);
    return JNI_FETCH_END;
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
      case TSDB_DATA_TYPE_FLOAT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetFloatFp, i, (jfloat) * ((float *)row[i]));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetDoubleFp, i, (jdouble) * ((double *)row[i]));
        break;
      case TSDB_DATA_TYPE_BINARY:{
        strncpy(tmp, row[i], (size_t) fields[i].bytes);  // handle the case that terminated does not exist
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetStringFp, i, (*env)->NewStringUTF(env, tmp));

        memset(tmp, 0, (size_t) fields[i].bytes);
        break;
      }
      case TSDB_DATA_TYPE_NCHAR:{
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetByteArrayFp, i,
                               jniFromNCharToByteArray(env, (char*)row[i], fields[i].bytes));
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
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  } else {
    jniTrace("jobj:%p, taos:%p, close connection success", jobj, tscon);
    taos_close(tscon);
    return JNI_SUCCESS;
  }
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_subscribeImp(JNIEnv *env, jobject jobj, jstring jhost,
                                                                             jstring juser, jstring jpass, jstring jdb,
                                                                             jstring jtable, jlong jtime,
                                                                             jint jperiod) {
  TAOS_SUB *tsub;
  jlong     sub = 0;
  char *    host = NULL;
  char *    user = NULL;
  char *    pass = NULL;
  char *    db = NULL;
  char *    table = NULL;
  int64_t   time = 0;
  int       period = 0;

  jniGetGlobalMethod(env);
  jniTrace("jobj:%p, in TSDBJNIConnector_subscribeImp", jobj);

  if (jhost != NULL) {
    host = (char *)(*env)->GetStringUTFChars(env, jhost, NULL);
  }
  if (juser != NULL) {
    user = (char *)(*env)->GetStringUTFChars(env, juser, NULL);
  }
  if (jpass != NULL) {
    pass = (char *)(*env)->GetStringUTFChars(env, jpass, NULL);
  }
  if (jdb != NULL) {
    db = (char *)(*env)->GetStringUTFChars(env, jdb, NULL);
  }
  if (jtable != NULL) {
    table = (char *)(*env)->GetStringUTFChars(env, jtable, NULL);
  }
  time = (int64_t)jtime;
  period = (int)jperiod;

  if (user == NULL) {
    jniTrace("jobj:%p, user is null, use tsDefaultUser", jobj);
    user = tsDefaultUser;
  }
  if (pass == NULL) {
    jniTrace("jobj:%p, pass is null, use tsDefaultPass", jobj);
    pass = tsDefaultPass;
  }

  jniTrace("jobj:%p, host:%s, user:%s, pass:%s, db:%s, table:%s, time:%d, period:%d", jobj, host, user, pass, db, table,
           time, period);
  tsub = taos_subscribe(host, user, pass, db, table, time, period);
  sub = (jlong)tsub;

  if (sub == 0) {
    jniTrace("jobj:%p, failed to subscribe to db:%s, table:%s", jobj, db, table);
  } else {
    jniTrace("jobj:%p, successfully subscribe to db:%s, table:%s, sub:%ld, tsub:%p", jobj, db, table, sub, tsub);
  }

  if (host != NULL) (*env)->ReleaseStringUTFChars(env, jhost, host);
  if (user != NULL && user != tsDefaultUser) (*env)->ReleaseStringUTFChars(env, juser, user);
  if (pass != NULL && pass != tsDefaultPass) (*env)->ReleaseStringUTFChars(env, jpass, pass);
  if (db != NULL) (*env)->ReleaseStringUTFChars(env, jdb, db);
  if (table != NULL) (*env)->ReleaseStringUTFChars(env, jtable, table);

  return sub;
}

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_consumeImp(JNIEnv *env, jobject jobj, jlong sub) {
  jniTrace("jobj:%p, in TSDBJNIConnector_consumeImp, sub:%ld", jobj, sub);

  TAOS_SUB *  tsub = (TAOS_SUB *)sub;
  TAOS_ROW    row = taos_consume(tsub);
  TAOS_FIELD *fields = taos_fetch_subfields(tsub);
  int         num_fields = taos_subfields_count(tsub);

  jniGetGlobalMethod(env);

  jniTrace("jobj:%p, check fields:%p, num_fields=%d", jobj, fields, num_fields);

  jobject rowobj = (*env)->NewObject(env, g_rowdataClass, g_rowdataConstructor, num_fields);
  jniTrace("created a rowdata object, rowobj:%p", rowobj);

  if (row == NULL) {
    jniTrace("jobj:%p, tsub:%p, fields size is %d, fetch row to the end", jobj, tsub, num_fields);
    return NULL;
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
      case TSDB_DATA_TYPE_FLOAT:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetFloatFp, i, (jfloat) * ((float *)row[i]));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetDoubleFp, i, (jdouble) * ((double *)row[i]));
        break;
      case TSDB_DATA_TYPE_BINARY: {
        strncpy(tmp, row[i], (size_t) fields[i].bytes);  // handle the case that terminated does not exist

        (*env)->CallVoidMethod(env, rowobj, g_rowdataSetStringFp, i, (*env)->NewStringUTF(env, (char *)row[i]));
        memset(tmp, 0, (size_t) fields[i].bytes);
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
  jniTrace("jobj:%p, rowdata retrieved, rowobj:%p", jobj, rowobj);
  return rowobj;
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_unsubscribeImp(JNIEnv *env, jobject jobj, jlong sub) {
  TAOS_SUB *tsub = (TAOS_SUB *)sub;
  taos_unsubscribe(tsub);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_validateCreateTableSqlImp(JNIEnv *env, jobject jobj,
                                                                                         jlong con, jbyteArray jsql) {
  TAOS *tscon = (TAOS *)con;
  if (tscon == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
    return JNI_CONNECTION_NULL;
  }

  if (jsql == NULL) {
    jniError("jobj:%p, taos:%p, sql is null", jobj, tscon);
    return JNI_SQL_NULL;
  }

  jsize len = (*env)->GetArrayLength(env, jsql);

  char *dst = (char *)calloc(1, sizeof(char) * (len + 1));
  (*env)->GetByteArrayRegion(env, jsql, 0, len, (jbyte *)dst);
  if ((*env)->ExceptionCheck(env)) {
    //todo handle error
  }

  int code = taos_validate_sql(tscon, dst);
  jniTrace("jobj:%p, conn:%p, code is %d", jobj, tscon, code);

  free(dst);
  return code;
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getTsCharset(JNIEnv *env, jobject jobj) {
  return (*env)->NewStringUTF(env, (const char *)tsCharset);
}
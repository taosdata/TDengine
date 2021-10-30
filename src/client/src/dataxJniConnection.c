#include "os.h"
#include "taos.h"
#include "tlog.h"
#include "tscUtil.h"

#include "com_alibaba_datax_plugin_writer_JniConnection.h"
#include "jniCommon.h"

JNIEXPORT void JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_initImp(JNIEnv *env, jobject jobj,
                                                                                  jstring jconfigDir) {
  if (jconfigDir != NULL) {
    const char *confDir = (*env)->GetStringUTFChars(env, jconfigDir, NULL);
    if (confDir && strlen(confDir) != 0) {
      tstrncpy(configDir, confDir, TSDB_FILENAME_LEN);
    }
    (*env)->ReleaseStringUTFChars(env, jconfigDir, confDir);
  }

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

JNIEXPORT void JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_freeResultSetImp(JNIEnv *env, jobject jobj,
                                                                                           jlong con, jlong res) {
  if ((TAOS *)con == NULL) {
    jniError("jobj:%p, connection is closed", jobj);
  }
  if ((TAOS_RES *)res == NULL) {
    jniError("jobj:%p, conn:%p, res is null", jobj, (TAOS *)con);
  }
  taos_free_result((TAOS_RES *)res);
  jniDebug("jobj:%p, conn:%p, free resultset:%p", jobj, (TAOS *)con, (void *)res);
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

JNIEXPORT jlong JNICALL Java_com_alibaba_datax_plugin_writer_JniConnection_insertOpentsdbJson(JNIEnv *env, jobject jobj,
                                                                                              jstring json, jlong con) {
  // check connection
  TAOS *conn = (TAOS *)con;
  if (conn == NULL) {
    jniError("jobj:%p, connection already closed", jobj);
    return JNI_CONNECTION_NULL;
  }
  // java.lang.String -> char *
  char *payload = NULL;
  if (json != NULL) {
    payload = (char *)(*env)->GetStringUTFChars(env, json, NULL);
  }
  // check payload
  if (payload == NULL) {
    jniDebug("jobj:%p, invalid argument: opentsdb insert json is NULL", jobj);
    return JNI_SQL_NULL;
  }
  // schemaless insert
  char *payload_arr[1];
  payload_arr[0] = payload;
  TAOS_RES *result;
  result = taos_schemaless_insert(conn, payload_arr, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NOT_CONFIGURED);

  int code = taos_errno(result);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, conn:%p, code:%s, msg:%s", jobj, conn, tstrerror(code), taos_errstr(result));
  } else {
    int32_t affectRows = taos_affected_rows(result);
    jniDebug("jobj:%p, conn:%p, code:%s, affect rows:%d", jobj, conn, tstrerror(code), affectRows);
  }

  (*env)->ReleaseStringUTFChars(env, json, payload);
  return (jlong)result;
}

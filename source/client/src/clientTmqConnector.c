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

#include "com_taosdata_jdbc_tmq_TMQConnector.h"
#include "jniCommon.h"
#include "taos.h"

int       __init_tmq = 0;
jmethodID g_offsetCallback;

jclass    g_assignmentClass;
jmethodID g_assignmentConstructor;
jmethodID g_assignmentSetVgId;
jmethodID g_assignmentSetCurrentOffset;
jmethodID g_assignmentSetBegin;
jmethodID g_assignmentSetEnd;

void tmqGlobalMethod(JNIEnv *env) {
  // make sure init function executed once
  switch (atomic_val_compare_exchange_32(&__init_tmq, 0, 1)) {
    case 0:
      break;
    case 1:
      do {
        taosMsleep(0);
      } while (atomic_load_32(&__init_tmq) == 1);
    case 2:
      return;
  }

  if (g_vm == NULL) {
    (*env)->GetJavaVM(env, &g_vm);
  }

  jclass offset = (*env)->FindClass(env, "com/taosdata/jdbc/tmq/OffsetWaitCallback");
  jclass g_offsetCallbackClass = (*env)->NewGlobalRef(env, offset);
  g_offsetCallback = (*env)->GetMethodID(env, g_offsetCallbackClass, "commitCallbackHandler", "(I)V");
  (*env)->DeleteLocalRef(env, offset);

  atomic_store_32(&__init_tmq, 2);
  jniDebug("tmq method register finished");
}

int  __init_assignment = 0;
void tmqAssignmentMethod(JNIEnv *env) {
  // make sure init function executed once
  switch (atomic_val_compare_exchange_32(&__init_assignment, 0, 1)) {
    case 0:
      break;
    case 1:
      do {
        taosMsleep(0);
      } while (atomic_load_32(&__init_assignment) == 1);
    case 2:
      return;
  }

  if (g_vm == NULL) {
    (*env)->GetJavaVM(env, &g_vm);
  }

  jclass assignment = (*env)->FindClass(env, "com/taosdata/jdbc/tmq/Assignment");
  g_assignmentClass = (*env)->NewGlobalRef(env, assignment);
  g_assignmentConstructor = (*env)->GetMethodID(env, g_assignmentClass, "<init>", "()V");
  g_assignmentSetVgId = (*env)->GetMethodID(env, g_assignmentClass, "setVgId", "(I)V");                    // int
  g_assignmentSetCurrentOffset = (*env)->GetMethodID(env, g_assignmentClass, "setCurrentOffset", "(J)V");  // long
  g_assignmentSetBegin = (*env)->GetMethodID(env, g_assignmentClass, "setBegin", "(J)V");                  // long
  g_assignmentSetEnd = (*env)->GetMethodID(env, g_assignmentClass, "setEnd", "(J)V");                      // long

  (*env)->DeleteLocalRef(env, assignment);

  atomic_store_32(&__init_assignment, 2);
  jniDebug("tmq method assignment finished");
}

// deprecated
void commit_cb(tmq_t *tmq, int32_t code, void *param) {
  JNIEnv *env = NULL;
  int     status = (*g_vm)->GetEnv(g_vm, (void **)&env, JNI_VERSION_1_6);
  bool    needDetach = false;
  if (status < 0) {
    if ((*g_vm)->AttachCurrentThread(g_vm, (void **)&env, NULL) != 0) {
      return;
    }
    needDetach = true;
  }

  jobject obj = (jobject)param;
  (*env)->CallVoidMethod(env, obj, g_commitCallback, code);

  (*env)->DeleteGlobalRef(env, obj);
  param = NULL;

  if (needDetach) {
    (*g_vm)->DetachCurrentThread(g_vm);
  }
  env = NULL;
}

void consumer_callback(tmq_t *tmq, int32_t code, void *param) {
  JNIEnv *env = NULL;
  int     status = (*g_vm)->GetEnv(g_vm, (void **)&env, JNI_VERSION_1_6);
  bool    needDetach = false;
  if (status < 0) {
    if ((*g_vm)->AttachCurrentThread(g_vm, (void **)&env, NULL) != 0) {
      return;
    }
    needDetach = true;
  }

  jobject obj = (jobject)param;
  (*env)->CallVoidMethod(env, obj, g_offsetCallback, code);
  (*env)->DeleteGlobalRef(env, obj);
  param = NULL;

  if (needDetach) {
    (*g_vm)->DetachCurrentThread(g_vm);
  }
  env = NULL;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConfNewImp(JNIEnv *env, jobject jobj) {
  tmq_conf_t *conf = tmq_conf_new();
  jniGetGlobalMethod(env);
  return (jlong)conf;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConfSetImp(JNIEnv *env, jobject jobj, jlong conf,
                                                                             jstring jkey, jstring jvalue) {
  if (jkey == NULL) {
    jniError("jobj:%p, failed set tmq config. key is null", jobj);
    return TMQ_CONF_KEY_NULL;
  }
  const char *key = (*env)->GetStringUTFChars(env, jkey, NULL);

  if (jvalue == NULL) {
    jniError("jobj:%p, failed set tmq config. key %s, value is null", jobj, key);
    (*env)->ReleaseStringUTFChars(env, jkey, key);
    return TMQ_CONF_VALUE_NULL;
  }
  const char *value = (*env)->GetStringUTFChars(env, jvalue, NULL);

  tmq_conf_res_t res = tmq_conf_set((tmq_conf_t *)conf, key, value);
  (*env)->ReleaseStringUTFChars(env, jkey, key);
  (*env)->ReleaseStringUTFChars(env, jvalue, value);
  return (jint)res;
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConfDestroyImp(JNIEnv *env, jobject jobj,
                                                                                 jlong jconf) {
  tmq_conf_t *conf = (tmq_conf_t *)jconf;
  if (conf == NULL) {
    jniDebug("jobj:%p, tmq config is already destroyed", jobj);
  } else {
    jniDebug("jobj:%p, config:%p, tmq successfully destroy config", jobj, conf);
    tmq_conf_destroy(conf);
  }
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConsumerNewImp(JNIEnv *env, jobject jobj,
                                                                                  jlong jconf, jobject jconsumer) {
  tmq_conf_t *conf = (tmq_conf_t *)jconf;
  if (conf == NULL) {
    jniError("jobj:%p, tmq config is already destroyed", jobj);
    return TMQ_CONF_NULL;
  }
  int   len = 1024;
  char *msg = (char *)taosMemoryCalloc(1, sizeof(char) * (len + 1));
  if (msg == NULL) {
    jniError("jobj:%p, config:%p, tmq alloc memory failed", jobj, conf);
    return JNI_OUT_OF_MEMORY;
  }
  tmq_t *tmq = tmq_consumer_new((tmq_conf_t *)conf, msg, len);
  if (strlen(msg) > 0) {
    jniError("jobj:%p, config:%p, tmq create consumer error: %s", jobj, conf, msg);
    (*env)->CallVoidMethod(env, jconsumer, g_createConsumerErrorCallback, (*env)->NewStringUTF(env, msg));
    taosMemoryFreeClear(msg);
    return TMQ_CONSUMER_CREATE_ERROR;
  }
  taosMemoryFreeClear(msg);
  return (jlong)tmq;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqTopicNewImp(JNIEnv *env, jobject jobj, jlong jtmq) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniError("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }

  tmq_list_t *topics = tmq_list_new();
  return (jlong)topics;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqTopicAppendImp(JNIEnv *env, jobject jobj,
                                                                                 jlong jtopic, jstring jname) {
  tmq_list_t *topic = (tmq_list_t *)jtopic;
  if (topic == NULL) {
    jniError("jobj:%p, tmq topic list is null", jobj);
    return TMQ_TOPIC_NULL;
  }
  if (jname == NULL) {
    jniDebug("jobj:%p, tmq topic append jname is null", jobj);
    return TMQ_TOPIC_NAME_NULL;
  }

  const char *name = (*env)->GetStringUTFChars(env, jname, NULL);

  int32_t res = tmq_list_append((tmq_list_t *)topic, name);
  (*env)->ReleaseStringUTFChars(env, jname, name);
  return (jint)res;
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqTopicDestroyImp(JNIEnv *env, jobject jobj,
                                                                                  jlong jtopic) {
  tmq_list_t *topic = (tmq_list_t *)jtopic;
  if (topic == NULL) {
    jniDebug("jobj:%p, tmq topic list is already destroyed", jobj);
  } else {
    tmq_list_destroy((tmq_list_t *)topic);
    jniDebug("jobj:%p, tmq successfully destroy topic list", jobj);
  }
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqSubscribeImp(JNIEnv *env, jobject jobj, jlong jtmq,
                                                                               jlong jtopic) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniError("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }

  tmq_list_t *topic = (tmq_list_t *)jtopic;
  if (topic == NULL) {
    jniDebug("jobj:%p, tmq topic list is already destroyed", jobj);
    return TMQ_TOPIC_NULL;
  }

  int32_t res = tmq_subscribe(tmq, topic);
  return (jint)res;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqSubscriptionImp(JNIEnv *env, jobject jobj, jlong jtmq,
                                                                                  jobject jconsumer) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniError("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }

  tmq_list_t *topicList = NULL;
  int32_t     res = tmq_subscription((tmq_t *)tmq, &topicList);
  if (res != JNI_SUCCESS) {
    tmq_list_destroy(topicList);
    jniError("jobj:%p, tmq:%p, tmq get subscription error: %s", jobj, tmq, tmq_err2str(res));
    return (jint)res;
  }

  char  **topics = tmq_list_to_c_array(topicList);
  int32_t sz = tmq_list_get_size(topicList);

  jobjectArray arr = (jobjectArray)(*env)->NewObjectArray(env, sz, (*env)->FindClass(env, "java/lang/String"),
                                                          (*env)->NewStringUTF(env, ""));
  for (int32_t i = 0; i < sz; i++) {
    (*env)->SetObjectArrayElement(env, arr, i, (*env)->NewStringUTF(env, topics[i]));
  }
  (*env)->CallVoidMethod(env, jconsumer, g_topicListCallback, arr);
  tmq_list_destroy(topicList);
  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommitSync(JNIEnv *env, jobject jobj, jlong jtmq,
                                                                             jlong jres) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniError("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }
  TAOS_RES *res = (TAOS_RES *)jres;
  return tmq_commit_sync(tmq, res);
}
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommitAllSync(JNIEnv *env, jobject jobj, jlong jtmq) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniError("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }

  return tmq_commit_sync(tmq, NULL);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommitOffsetSyncImp(JNIEnv *env, jobject jobj,
                                                                                      jlong jtmq, jstring jtopic,
                                                                                      jint vgId, jlong offset) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniDebug("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }

  if (jtopic == NULL) {
    jniDebug("jobj:%p, topic is null", jobj);
    return TMQ_TOPIC_NULL;
  }
  const char *topicName = (*env)->GetStringUTFChars(env, jtopic, NULL);

  int code = tmq_commit_offset_sync(tmq, topicName, vgId, offset);
  if (code != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, tmq commit offset error, code:%d, msg:%s", jobj, code, tmq_err2str(code));
  }

  (*env)->ReleaseStringUTFChars(env, jtopic, topicName);
  return code;
}

// deprecated
JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommitAsync(JNIEnv *env, jobject jobj, jlong jtmq,
                                                                              jlong jres, jobject consumer) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniError("jobj:%p, tmq is closed", jobj);
    return;
  }
  TAOS_RES *res = (TAOS_RES *)jres;
  consumer = (*env)->NewGlobalRef(env, consumer);
  tmq_commit_async(tmq, res, commit_cb, consumer);
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_consumerCommitAsync(JNIEnv *env, jobject jobj,
                                                                                   jlong jtmq, jlong jres,
                                                                                   jobject offset) {
  tmqGlobalMethod(env);
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniError("jobj:%p, tmq is closed", jobj);
    return;
  }
  TAOS_RES *res = (TAOS_RES *)jres;
  offset = (*env)->NewGlobalRef(env, offset);
  tmq_commit_async(tmq, res, consumer_callback, offset);
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_consumerCommitAllAsync(JNIEnv *env, jobject jobj,
                                                                                      jlong jtmq, jobject offset) {
  tmqGlobalMethod(env);
  tmq_t *tmq = (tmq_t *)jtmq;

  offset = (*env)->NewGlobalRef(env, offset);
  tmq_commit_async(tmq, NULL, consumer_callback, offset);
}

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_consumerCommitOffsetAsync(JNIEnv *env, jobject jobj,
                                                                                         jlong jtmq, jstring jtopic,
                                                                                         jint vgId, jlong offset,
                                                                                         jobject callback) {
  tmqGlobalMethod(env);
  tmq_t      *tmq = (tmq_t *)jtmq;
  const char *topicName = (*env)->GetStringUTFChars(env, jtopic, NULL);

  callback = (*env)->NewGlobalRef(env, callback);
  tmq_commit_offset_async(tmq, topicName, vgId, offset, consumer_callback, callback);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqUnsubscribeImp(JNIEnv *env, jobject jobj,
                                                                                 jlong jtmq) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniError("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }
  jniDebug("jobj:%p, tmq:%p, successfully unsubscribe", jobj, tmq);
  return tmq_unsubscribe((tmq_t *)tmq);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConsumerCloseImp(JNIEnv *env, jobject jobj,
                                                                                   jlong jtmq) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniDebug("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }
  return tmq_consumer_close((tmq_t *)tmq);
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_getErrMsgImp(JNIEnv *env, jobject jobj, jint code) {
  return (*env)->NewStringUTF(env, tmq_err2str(code));
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConsumerPoll(JNIEnv *env, jobject jobj, jlong jtmq,
                                                                                jlong time) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniDebug("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }
  return (jlong)tmq_consumer_poll((tmq_t *)tmq, time);
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetTopicName(JNIEnv *env, jobject jobj,
                                                                                  jlong jres) {
  TAOS_RES *res = (TAOS_RES *)jres;
  if (res == NULL) {
    jniDebug("jobj:%p, invalid res handle", jobj);
    return NULL;
  }
  return (*env)->NewStringUTF(env, tmq_get_topic_name(res));
}
JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetDbName(JNIEnv *env, jobject jobj, jlong jres) {
  TAOS_RES *res = (TAOS_RES *)jres;
  if (res == NULL) {
    jniDebug("jobj:%p, invalid res handle", jobj);
    return NULL;
  }
  return (*env)->NewStringUTF(env, tmq_get_db_name(res));
}
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetVgroupId(JNIEnv *env, jobject jobj, jlong jres) {
  TAOS_RES *res = (TAOS_RES *)jres;
  if (res == NULL) {
    jniDebug("jobj:%p, invalid res handle", jobj);
    return JNI_RESULT_SET_NULL;
  }
  return tmq_get_vgroup_id(res);
}

JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetTableName(JNIEnv *env, jobject jobj,
                                                                                  jlong jres) {
  TAOS_RES *res = (TAOS_RES *)jres;
  if (res == NULL) {
    jniDebug("jobj:%p, invalid res handle", jobj);
    return NULL;
  }
  return (*env)->NewStringUTF(env, tmq_get_table_name(res));
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetOffset(JNIEnv *env, jobject jobj, jlong jres) {
  TAOS_RES *res = (TAOS_RES *)jres;
  if (res == NULL) {
    jniDebug("jobj:%p, invalid res handle", jobj);
    return JNI_RESULT_SET_NULL;
  }
  return tmq_get_vgroup_offset(res);
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_fetchRawBlockImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong res, jobject rowobj,
                                                                                jobject arrayListObj) {
  TAOS   *tscon = (TAOS *)con;
  int32_t code = check_for_params(jobj, con, res);
  if (code != JNI_SUCCESS) {
    return code;
  }

  TAOS_RES *tres = (TAOS_RES *)res;

  void   *data = NULL;
  int32_t numOfRows = 0;
  int     error_code = taos_fetch_raw_block(tres, &numOfRows, &data);
  if (numOfRows == 0) {
    if (error_code == JNI_SUCCESS) {
      jniDebug("jobj:%p, conn:%p, resultset:%p, no data to retrieve", jobj, tscon, (void *)res);
      return JNI_FETCH_END;
    } else {
      jniError("jobj:%p, conn:%p, query interrupted, tmq fetch block error code:%d, msg:%s", jobj, tscon, error_code,
               taos_errstr(tres));
      return JNI_RESULT_SET_NULL;
    }
  }

  int32_t numOfFields = taos_num_fields(tres);
  if (numOfFields == 0) {
    jniError("jobj:%p, conn:%p, resultset:%p, fields size is %d", jobj, tscon, tres, numOfFields);
    return JNI_NUM_OF_FIELDS_0;
  }

  TAOS_FIELD *fields = taos_fetch_fields(tres);
  jniDebug("jobj:%p, conn:%p, resultset:%p, fields size is %d", jobj, tscon, tres, numOfFields);
  for (int i = 0; i < numOfFields; ++i) {
    jobject metadataObj = (*env)->NewObject(env, g_metadataClass, g_metadataConstructFp);
    (*env)->SetIntField(env, metadataObj, g_metadataColtypeField, fields[i].type);
    (*env)->SetIntField(env, metadataObj, g_metadataColsizeField, fields[i].bytes);
    (*env)->SetIntField(env, metadataObj, g_metadataColindexField, i);
    jstring metadataObjColname = (*env)->NewStringUTF(env, fields[i].name);
    (*env)->SetObjectField(env, metadataObj, g_metadataColnameField, metadataObjColname);
    (*env)->CallBooleanMethod(env, arrayListObj, g_arrayListAddFp, metadataObj);
  }

  (*env)->CallVoidMethod(env, rowobj, g_blockdataSetNumOfRowsFp, (jint)numOfRows);
  (*env)->CallVoidMethod(env, rowobj, g_blockdataSetNumOfColsFp, (jint)numOfFields);

  int32_t len = *(int32_t *)(((char *)data) + 4);
  (*env)->CallVoidMethod(env, rowobj, g_blockdataSetByteArrayFp, jniFromNCharToByteArray(env, (char *)data, len));
  return JNI_SUCCESS;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqSeekImp(JNIEnv *env, jobject jobj, jlong jtmq,
                                                                          jstring jtopic, jint partition,
                                                                          jlong offset) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniDebug("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }

  if (jtopic == NULL) {
    jniDebug("jobj:%p, topic is null", jobj);
    return TMQ_TOPIC_NULL;
  }
  const char *topicName = (*env)->GetStringUTFChars(env, jtopic, NULL);

  int32_t res = tmq_offset_seek(tmq, topicName, partition, offset);

  if (res != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, tmq seek error, code:%d, msg:%s", jobj, res, tmq_err2str(res));
  }

  (*env)->ReleaseStringUTFChars(env, jtopic, topicName);
  return (jint)res;
}

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetTopicAssignmentImp(JNIEnv *env, jobject jobj,
                                                                                        jlong jtmq, jstring jtopic,
                                                                                        jobject jarrayList) {
  tmqAssignmentMethod(env);
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniDebug("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }

  if (jtopic == NULL) {
    jniDebug("jobj:%p, topic is null", jobj);
    return TMQ_TOPIC_NULL;
  }

  const char *topicName = (*env)->GetStringUTFChars(env, jtopic, NULL);

  tmq_topic_assignment *pAssign = NULL;
  int32_t               numOfAssignment = 0;
  int32_t               res = tmq_get_topic_assignment(tmq, topicName, &pAssign, &numOfAssignment);

  if (res != TSDB_CODE_SUCCESS) {
    jniError("jobj:%p, tmq get topic assignment error, topic:%s, code:%d, msg:%s", jobj, topicName, res,
             tmq_err2str(res));
    (*env)->ReleaseStringUTFChars(env, jtopic, topicName);
    tmq_free_assignment(pAssign);
    return (jint)res;
  }

  (*env)->ReleaseStringUTFChars(env, jtopic, topicName);

  for (int i = 0; i < numOfAssignment; ++i) {
    tmq_topic_assignment assignment = pAssign[i];
    jobject              jassignment = (*env)->NewObject(env, g_assignmentClass, g_assignmentConstructor);
    (*env)->CallVoidMethod(env, jassignment, g_assignmentSetVgId, assignment.vgId);
    (*env)->CallVoidMethod(env, jassignment, g_assignmentSetCurrentOffset, assignment.currentOffset);
    (*env)->CallVoidMethod(env, jassignment, g_assignmentSetBegin, assignment.begin);
    (*env)->CallVoidMethod(env, jassignment, g_assignmentSetEnd, assignment.end);
    (*env)->CallBooleanMethod(env, jarrayList, g_arrayListAddFp, jassignment);
  }
  tmq_free_assignment(pAssign);
  return JNI_SUCCESS;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommittedImp(JNIEnv *env, jobject jobj, jlong jtmq,
                                                                                jstring jtopic, jint vgId) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniDebug("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }

  if (jtopic == NULL) {
    jniDebug("jobj:%p, topic is null", jobj);
    return TMQ_TOPIC_NULL;
  }

  const char *topicName = (*env)->GetStringUTFChars(env, jtopic, NULL);

  int64_t offset = tmq_committed(tmq, topicName, vgId);

  if (offset < JNI_SUCCESS && offset != -2147467247) {
    jniError("jobj:%p, tmq get committed offset error, topic:%s, vgId:%d, code:0x%" PRIx64 ", msg:%s", jobj, topicName,
             vgId, offset, tmq_err2str(offset));
  }

  (*env)->ReleaseStringUTFChars(env, jtopic, topicName);
  return (jlong)offset;
}

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqPositionImp(JNIEnv *env, jobject jobj, jlong jtmq,
                                                                               jstring jtopic, jint vgId) {
  tmq_t *tmq = (tmq_t *)jtmq;
  if (tmq == NULL) {
    jniDebug("jobj:%p, tmq is closed", jobj);
    return TMQ_CONSUMER_NULL;
  }

  if (jtopic == NULL) {
    jniDebug("jobj:%p, topic is null", jobj);
    return TMQ_TOPIC_NULL;
  }

  const char *topicName = (*env)->GetStringUTFChars(env, jtopic, NULL);

  int64_t offset = tmq_position(tmq, topicName, vgId);

  if (offset < JNI_SUCCESS) {
    jniError("jobj:%p, tmq get position error, topic:%s, vgId:%d, code:0x%" PRIx64 ", msg:%s", jobj, topicName, vgId,
             offset, tmq_err2str(offset));
  }

  (*env)->ReleaseStringUTFChars(env, jtopic, topicName);
  return (jlong)offset;
}
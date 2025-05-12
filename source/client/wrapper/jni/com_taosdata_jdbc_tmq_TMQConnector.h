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
/* Header for class com_taosdata_jdbc_tmq_TMQConnector */

#ifndef _Included_com_taosdata_jdbc_tmq_TMQConnector
#define _Included_com_taosdata_jdbc_tmq_TMQConnector
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqConfNewImp
 * Signature: (Lcom/taosdata/jdbc/tmq/TAOSConsumer;)J
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConfNewImp(JNIEnv *, jobject);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqConfSetImp
 * Signature: (JLjava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConfSetImp(JNIEnv *, jobject, jlong, jstring,
                                                                             jstring);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqConfDestroyImp
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConfDestroyImp(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqConsumerNewImp
 * Signature: (JLcom/taosdata/jdbc/tmq/TMQConnector;)J
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConsumerNewImp(JNIEnv *, jobject, jlong, jobject);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqTopicNewImp
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqTopicNewImp(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqTopicAppendImp
 * Signature: (JLjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqTopicAppendImp(JNIEnv *, jobject, jlong, jstring);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqTopicDestroyImp
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqTopicDestroyImp(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqSubscribeImp
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqSubscribeImp(JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqSubscriptionImp
 * Signature: (JLcom/taosdata/jdbc/tmq/TMQConnector;)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqSubscriptionImp(JNIEnv *, jobject, jlong, jobject);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqCommitSync
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommitSync(JNIEnv *, jobject, jlong, jlong);

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommitAllSync(JNIEnv *, jobject, jlong);

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommitOffsetSyncImp(JNIEnv *, jobject, jlong, jstring,
                                                                                      jint, jlong);
/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqCommitAsync
 * Signature: (JJLcom/taosdata/jdbc/tmq/TAOSConsumer;)V
 */
JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommitAsync(JNIEnv *, jobject, jlong, jlong, jobject);

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_consumerCommitAsync(JNIEnv *, jobject, jlong, jlong,
                                                                                   jobject);

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_consumerCommitAllAsync(JNIEnv *, jobject, jlong,
                                                                                      jobject);

JNIEXPORT void JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_consumerCommitOffsetAsync(JNIEnv *, jobject, jlong,
                                                                                         jstring, jint, jlong, jobject);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqUnsubscribeImp
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqUnsubscribeImp(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqConsumerCloseImp
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConsumerCloseImp(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    getErrMsgImp
 * Signature: (I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_getErrMsgImp(JNIEnv *, jobject, jint);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqConsumerPoll
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqConsumerPoll(JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqGetTopicName
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetTopicName(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqGetDbName
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetDbName(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqGetVgroupId
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetVgroupId(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqGetTableName
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetTableName(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    tmqGetOffset
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetOffset(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_tmq_TMQConnector
 * Method:    fetchBlockImp
 * Signature: (JJLcom/taosdata/jdbc/TSDBResultSetBlockData;ILjava/util/List;)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_fetchRawBlockImp(JNIEnv *, jobject, jlong, jlong,
                                                                                jobject, jobject);

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqSeekImp(JNIEnv *, jobject, jlong, jstring, jint,
                                                                          jlong);

JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqGetTopicAssignmentImp(JNIEnv *, jobject, jlong,
                                                                                        jstring, jobject);

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqCommittedImp(JNIEnv *, jobject, jlong, jstring,
                                                                                jint);

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_tmq_TMQConnector_tmqPositionImp(JNIEnv *, jobject, jlong, jstring, jint);

#ifdef __cplusplus
}
#endif
#endif

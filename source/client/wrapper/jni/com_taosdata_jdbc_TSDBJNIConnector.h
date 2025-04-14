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
/* Header for class com_taosdata_jdbc_TSDBJNIConnector */

#ifndef _Included_com_taosdata_jdbc_TSDBJNIConnector
#define _Included_com_taosdata_jdbc_TSDBJNIConnector
#ifdef __cplusplus
extern "C" {
#endif
#undef com_taosdata_jdbc_TSDBJNIConnector_INVALID_CONNECTION_POINTER_VALUE
#define com_taosdata_jdbc_TSDBJNIConnector_INVALID_CONNECTION_POINTER_VALUE 0LL

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    initImp
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_initImp(JNIEnv *, jclass, jstring);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    setOptions
 * Signature: (ILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setOptions(JNIEnv *, jclass, jint, jstring);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    setConfigImp
 * Signature: (Ljava/lang/String;)Lcom/taosdata/jdbc/TSDBException;
 */
JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setConfigImp(JNIEnv *, jclass, jstring);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    getTsCharset
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getTsCharset(JNIEnv *, jclass);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    getResultTimePrecisionImp
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TDDBJNIConnector_getResultTimePrecisionImp(JNIEnv *, jobject, jlong,
                                                                                         jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    connectImp
 * Signature: (Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_connectImp(JNIEnv *, jobject, jstring, jint, jstring,
                                                                           jstring, jstring);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    executeQueryImp
 * Signature: ([BJ)I
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_executeQueryImp(JNIEnv *, jobject, jbyteArray, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    executeQueryWithReqId
 * Signature: ([BJJ)I
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_executeQueryWithReqId(JNIEnv *, jobject, jbyteArray,
                                                                                      jlong, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    getErrCodeImp
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrCodeImp(JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    getErrMsgImp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getErrMsgImp(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    getResultSetImp
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getResultSetImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong tres);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    isUpdateQueryImp
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_isUpdateQueryImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                jlong tres);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    freeResultSetImp
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_freeResultSetImp(JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    getAffectedRowsImp
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getAffectedRowsImp(JNIEnv *env, jobject jobj, jlong con,
                                                                                  jlong res);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    getSchemaMetaDataImp
 * Signature: (JJLjava/util/List;)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getSchemaMetaDataImp(JNIEnv *, jobject, jlong, jlong,
                                                                                    jobject);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    fetchRowImp
 * Signature: (JJLcom/taosdata/jdbc/TSDBResultSetRowData;)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_fetchRowImp(JNIEnv *, jobject, jlong, jlong, jobject);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    fetchBlockImp
 * Signature: (JJLcom/taosdata/jdbc/TSDBResultSetBlockData;)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_fetchBlockImp(JNIEnv *, jobject, jlong, jlong, jobject);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    closeConnectionImp
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_closeConnectionImp(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    subscribeImp
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;JI)J
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_subscribeImp(JNIEnv *, jobject, jlong, jboolean,
                                                                             jstring, jstring, jint);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    consumeImp
 * Signature: (J)Lcom/taosdata/jdbc/TSDBResultSetRowData;
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_consumeImp(JNIEnv *, jobject, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    unsubscribeImp
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_unsubscribeImp(JNIEnv *, jobject, jlong, jboolean);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    validateCreateTableSqlImp
 * Signature: (J[B)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_validateCreateTableSqlImp(JNIEnv *, jobject, jlong,
                                                                                         jbyteArray);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    prepareStmtImp
 * Signature: ([BJ)I
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_prepareStmtImp(JNIEnv *, jobject, jbyteArray, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    prepareStmtWithReqId
 * Signature: ([BJJ)I
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_prepareStmtWithReqId(JNIEnv *, jobject, jbyteArray,
                                                                                     jlong, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    setBindTableNameImp
 * Signature: (JLjava/lang/String;J)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setBindTableNameImp(JNIEnv *, jobject, jlong, jstring,
                                                                                   jlong);

/**
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    setTableNameTagsImp
 * Signature: (JLjava/lang/String;I[B[B[B[BJ)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_setTableNameTagsImp(JNIEnv *, jobject, jlong, jstring,
                                                                                   jint, jbyteArray, jbyteArray,
                                                                                   jbyteArray, jbyteArray, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    bindColDataImp
 * Signature: (J[B[B[BIIIIJ)J
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_bindColDataImp(JNIEnv *, jobject, jlong, jbyteArray,
                                                                               jbyteArray, jbyteArray, jint, jint, jint,
                                                                               jint, jlong);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    stmt_add_batch
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_addBatchImp(JNIEnv *env, jobject jobj, jlong stmt,
                                                                           jlong con);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    executeBatchImp
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_executeBatchImp(JNIEnv *env, jobject jobj, jlong stmt,
                                                                               jlong con);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    closeStmt
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_closeStmt(JNIEnv *env, jobject jobj, jlong stmt,
                                                                         jlong con);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    stmt_errstr
 * Signature: (JJ)I
 */
JNIEXPORT jstring JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_stmtErrorMsgImp(JNIEnv *env, jobject jobj, jlong stmt,
                                                                                  jlong con);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    insertLinesImp
 * Signature: ([Ljava/lang/String;JII)I
 */
JNIEXPORT jint JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_insertLinesImp(JNIEnv *, jobject, jobjectArray, jlong,
                                                                              jint, jint);

/*
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    schemalessInsertImp
 * Signature: (J[B[B[BIIIIJ)J
 */
JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertImp(JNIEnv *, jobject, jobjectArray,
                                                                                    jlong, jint, jint);

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertWithReqId(JNIEnv *, jobject, jlong,
                                                                                          jobjectArray, jint, jint,
                                                                                          jlong);

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertWithTtl(JNIEnv *, jobject, jlong,
                                                                                        jobjectArray, jint, jint, jint);

JNIEXPORT jlong JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertWithTtlAndReqId(JNIEnv *, jobject,
                                                                                                jlong, jobjectArray,
                                                                                                jint, jint, jint,
                                                                                                jlong);

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertRaw(JNIEnv *, jobject, jlong, jstring,
                                                                                      jint, jint);

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertRawWithReqId(JNIEnv *, jobject, jlong,
                                                                                               jstring, jint, jint,
                                                                                               jlong);

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertRawWithTtl(JNIEnv *, jobject, jlong,
                                                                                             jstring, jint, jint, jint);

JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_schemalessInsertRawWithTtlAndReqId(JNIEnv *, jobject,
                                                                                                     jlong, jstring,
                                                                                                     jint, jint, jint,
                                                                                                     jlong);
/**
 * Class:     com_taosdata_jdbc_TSDBJNIConnector
 * Method:    getTableVgID
 * Signature: (Ljava/lang/String;Ljava/lang/String)Lcom/taosdata/jdbc/VGroupIDResp
 */
JNIEXPORT jobject JNICALL Java_com_taosdata_jdbc_TSDBJNIConnector_getTableVgID(JNIEnv *, jobject, jlong, jstring,
                                                                               jstring, jobject);

#ifdef __cplusplus
}
#endif
#endif

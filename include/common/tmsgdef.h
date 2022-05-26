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

// clang-format off

#if 0
#undef TD_MSG_INFO_
#undef TD_MSG_NUMBER_
#undef TD_MSG_DICT_
#undef TD_MSG_SEG_CODE_
#endif

#if defined(TD_MSG_INFO_)

#undef TD_NEW_MSG_SEG
#undef TD_DEF_MSG_TYPE
#define TD_NEW_MSG_SEG(TYPE) "null",
#define TD_DEF_MSG_TYPE(TYPE, MSG, REQ, RSP) MSG, MSG "-rsp",

char *tMsgInfo[] = {

#elif defined(TD_MSG_NUMBER_)

#undef TD_NEW_MSG_SEG
#undef TD_DEF_MSG_TYPE
#define TD_NEW_MSG_SEG(TYPE) TYPE##_NUM,
#define TD_DEF_MSG_TYPE(TYPE, MSG, REQ, RSP) TYPE##_NUM, TYPE##_RSP_NUM,

enum {

#elif defined(TD_MSG_DICT_)

#undef TD_NEW_MSG_SEG
#undef TD_DEF_MSG_TYPE
#define TD_NEW_MSG_SEG(TYPE) TYPE##_NUM,
#define TD_DEF_MSG_TYPE(TYPE, MSG, REQ, RSP)

int32_t tMsgDict[] = {

#elif defined(TD_MSG_SEG_CODE_)

#undef TD_NEW_MSG_SEG
#undef TD_DEF_MSG_TYPE
#define TD_NEW_MSG_SEG(TYPE) TYPE##_SEG_CODE,
#define TD_DEF_MSG_TYPE(TYPE, MSG, REQ, RSP)

enum {

#else

#undef TD_NEW_MSG_SEG
#undef TD_DEF_MSG_TYPE
#define TD_NEW_MSG_SEG(TYPE) TYPE = ((TYPE##_SEG_CODE) << 8),
#define TD_DEF_MSG_TYPE(TYPE, MSG, REQ, RSP) TYPE, TYPE##_RSP,

enum {
#endif
  // Requests handled by DNODE
  TD_NEW_MSG_SEG(TDMT_DND_MSG)
  TD_DEF_MSG_TYPE(TDMT_DND_CREATE_MNODE, "dnode-create-mnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_ALTER_MNODE, "dnode-alter-mnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_DROP_MNODE, "dnode-drop-mnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_CREATE_QNODE, "dnode-create-qnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_ALTER_QNODE, "dnode-alter-qnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_DROP_QNODE, "dnode-drop-qnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_CREATE_SNODE, "dnode-create-snode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_ALTER_SNODE, "dnode-alter-snode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_DROP_SNODE, "dnode-drop-snode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_CREATE_BNODE, "dnode-create-bnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_ALTER_BNODE, "dnode-alter-bnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_DROP_BNODE, "dnode-drop-bnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_CREATE_VNODE, "dnode-create-vnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_DROP_VNODE, "dnode-drop-vnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_CONFIG_DNODE, "dnode-config-dnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_SERVER_STATUS, "dnode-server-status", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_DND_NET_TEST, "dnode-net-test", NULL, NULL)

  // Requests handled by MNODE
  TD_NEW_MSG_SEG(TDMT_MND_MSG)
  TD_DEF_MSG_TYPE(TDMT_MND_CONNECT, "mnode-connect", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_ACCT, "mnode-create-acct", NULL, NULL)	
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_ACCT, "mnode-alter-acct", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_ACCT, "mnode-drop-acct", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_USER, "mnode-create-user", NULL, NULL)	
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_USER, "mnode-alter-user", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_USER, "mnode-drop-user", NULL, NULL) 
  TD_DEF_MSG_TYPE(TDMT_MND_GET_USER_AUTH, "mnode-get-user-auth", NULL, NULL) 
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_DNODE, "mnode-create-dnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CONFIG_DNODE, "mnode-config-dnode", NULL, NULL) 
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_DNODE, "mnode-alter-dnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_DNODE, "mnode-drop-dnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_MNODE, "mnode-create-mnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_MNODE, "mnode-alter-mnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_MNODE, "mnode-drop-mnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_QNODE, "mnode-create-qnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_QNODE, "mnode-alter-qnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_QNODE, "mnode-drop-qnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_SNODE, "mnode-create-snode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_SNODE, "mnode-alter-snode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_SNODE, "mnode-drop-snode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_BNODE, "mnode-create-bnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_BNODE, "mnode-alter-bnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_BNODE, "mnode-drop-bnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_DB, "mnode-create-db", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_DB, "mnode-drop-db", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_USE_DB, "mnode-use-db", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_DB, "mnode-alter-db", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_SYNC_DB, "mnode-sync-db", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_COMPACT_DB, "mnode-compact-db", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_FUNC, "mnode-create-func", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_RETRIEVE_FUNC, "mnode-retrieve-func", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_FUNC, "mnode-drop-func", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_STB, "mnode-create-stb", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_STB, "mnode-alter-stb", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_STB, "mnode-drop-stb", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_SMA, "mnode-create-sma", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_SMA, "mnode-drop-sma", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_TABLE_META, "mnode-table-meta", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_VGROUP_LIST, "mnode-vgroup-list", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_QNODE_LIST, "mnode-qnode-list", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_KILL_QUERY, "mnode-kill-query", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_KILL_CONN, "mnode-kill-conn", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_HEARTBEAT, "mnode-heartbeat", SClientHbBatchReq, SClientHbBatchRsp)
  TD_DEF_MSG_TYPE(TDMT_MND_SHOW, "mnode-show", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_SYSTABLE_RETRIEVE, "mnode-systable-retrieve", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_STATUS, "mnode-status", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_TRANS_TIMER, "mnode-trans-tmr", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_KILL_TRANS, "mnode-kill-trans", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_TELEM_TIMER, "mnode-telem-tmr", SMTimerReq, SMTimerReq)
  TD_DEF_MSG_TYPE(TDMT_MND_GRANT, "mnode-grant", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_AUTH, "mnode-auth", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_TOPIC, "mnode-create-topic", SMCreateTopicReq, SMCreateTopicRsp)
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_TOPIC, "mnode-alter-topic", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_TOPIC, "mnode-drop-topic", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_CGROUP, "mnode-drop-cgroup", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_SUBSCRIBE, "mnode-subscribe", SCMSubscribeReq, SCMSubscribeRsp)
  TD_DEF_MSG_TYPE(TDMT_MND_MQ_ASK_EP, "mnode-mq-ask-ep", SMqAskEpReq, SMqAskEpRsp)
  TD_DEF_MSG_TYPE(TDMT_MND_MQ_TIMER, "mnode-mq-tmr", SMTimerReq, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_MQ_CONSUMER_LOST, "mnode-mq-consumer-lost", SMqConsumerLostMsg, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_MQ_CONSUMER_RECOVER, "mnode-mq-consumer-recover", SMqConsumerRecoverMsg, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_MQ_DO_REBALANCE, "mnode-mq-do-rebalance", SMqDoRebalanceMsg, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_MQ_COMMIT_OFFSET, "mnode-mq-commit-offset", SMqCMCommitOffsetReq, SMqCMCommitOffsetRsp)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_STREAM, "mnode-create-stream", SCMCreateStreamReq, SCMCreateStreamRsp)
  TD_DEF_MSG_TYPE(TDMT_MND_ALTER_STREAM, "mnode-alter-stream", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_STREAM, "mnode-drop-stream", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_CREATE_INDEX, "mnode-create-index", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_DROP_INDEX, "mnode-drop-index", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_GET_DB_CFG, "mnode-get-db-cfg", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_GET_INDEX, "mnode-get-index", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MND_APPLY_MSG, "mnode-apply-msg", NULL, NULL)

  // Requests handled by VNODE
  TD_NEW_MSG_SEG(TDMT_VND_MSG)
  TD_DEF_MSG_TYPE(TDMT_VND_SUBMIT, "vnode-submit", SSubmitReq, SSubmitRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_QUERY, "vnode-query", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_FETCH, "vnode-fetch", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_CREATE_TABLE, "vnode-create-table", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_ALTER_TABLE, "vnode-alter-table", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_DROP_TABLE, "vnode-drop-table", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_UPDATE_TAG_VAL, "vnode-update-tag-val", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_TABLE_META, "vnode-table-meta", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_TABLES_META, "vnode-tables-meta", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_CREATE_STB, "vnode-create-stb", SVCreateStbReq, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_ALTER_STB, "vnode-alter-stb", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_DROP_STB, "vnode-drop-stb", SVDropStbReq, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_MQ_CONSUME, "vnode-mq-consume", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_MQ_QUERY, "vnode-mq-query", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_MQ_CONNECT, "vnode-mq-connect", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_MQ_DISCONNECT, "vnode-mq-disconnect", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_MQ_VG_CHANGE, "vnode-mq-vg-change", SMqRebVgReq, SMqRebVgRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_MQ_VG_DELETE, "vnode-mq-vg-delete", SMqVDeleteReq, SMqVDeleteRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_RES_READY, "vnode-res-ready", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_TASKS_STATUS, "vnode-tasks-status", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_CANCEL_TASK, "vnode-cancel-task", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_DROP_TASK, "vnode-drop-task", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_CREATE_TOPIC, "vnode-create-topic", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_ALTER_TOPIC, "vnode-alter-topic", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_DROP_TOPIC, "vnode-drop-topic", NULL, NULL)
//  TD_DEF_MSG_TYPE(TDMT_VND_SHOW_TABLES, "vnode-show-tables", SVShowTablesReq, SVShowTablesRsp)
//  TD_DEF_MSG_TYPE(TDMT_VND_SHOW_TABLES_FETCH, "vnode-show-tables-fetch", SVShowTablesFetchReq, SVShowTablesFetchRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_QUERY_CONTINUE, "vnode-query-continue", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_QUERY_HEARTBEAT, "vnode-query-heartbeat", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_EXPLAIN, "vnode-explain", NULL, NULL)

  TD_DEF_MSG_TYPE(TDMT_VND_SUBSCRIBE, "vnode-subscribe", SMVSubscribeReq, SMVSubscribeRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_CONSUME, "vnode-consume", SMqCVConsumeReq, SMqCVConsumeRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_TASK_DEPLOY, "vnode-task-deploy", SStreamTaskDeployReq, SStreamTaskDeployRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_TASK_PIPE_EXEC, "vnode-task-pipe-exec", SStreamTaskExecReq, SStreamTaskExecRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_TASK_MERGE_EXEC, "vnode-task-merge-exec", SStreamTaskExecReq, SStreamTaskExecRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_TASK_WRITE_EXEC, "vnode-task-write-exec", SStreamTaskExecReq, SStreamTaskExecRsp)
  TD_DEF_MSG_TYPE(TDMT_VND_STREAM_TRIGGER, "vnode-stream-trigger", NULL, NULL)

  TD_DEF_MSG_TYPE(TDMT_VND_TASK_RUN, "vnode-stream-task-run", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_TASK_DISPATCH, "vnode-stream-task-dispatch", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_TASK_RECOVER, "vnode-stream-task-recover", NULL, NULL)

  TD_DEF_MSG_TYPE(TDMT_VND_CREATE_SMA, "vnode-create-sma", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_CANCEL_SMA, "vnode-cancel-sma", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_DROP_SMA, "vnode-drop-sma", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SUBMIT_RSMA, "vnode-submit-rsma", SSubmitReq, SSubmitRsp)

  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_TIMEOUT, "vnode-sync-timeout", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_PING, "vnode-sync-ping", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_PING_REPLY, "vnode-sync-ping-reply", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_CLIENT_REQUEST, "vnode-sync-client-request", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_CLIENT_REQUEST_REPLY, "vnode-sync-client-request-reply", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_REQUEST_VOTE, "vnode-sync-request-vote", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_REQUEST_VOTE_REPLY, "vnode-sync-request-vote-reply", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_APPEND_ENTRIES, "vnode-sync-append-entries", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_APPEND_ENTRIES_REPLY, "vnode-sync-append-entries-reply", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_NOOP, "vnode-sync-noop", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_UNKNOWN, "vnode-sync-unknown", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_COMMON_RESPONSE, "vnode-sync-common-response", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_APPLY_MSG, "vnode-sync-apply-msg", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_CONFIG_CHANGE, "vnode-sync-config-change", NULL, NULL)
  
  TD_DEF_MSG_TYPE(TDMT_VND_SYNC_VNODE, "vnode-sync-vnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_ALTER_VNODE, "vnode-alter-vnode", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_VND_COMPACT_VNODE, "vnode-compact-vnode", NULL, NULL)

  // Requests handled by QNODE
  TD_NEW_MSG_SEG(TDMT_QND_MSG)

  // Requests handled by SNODE
  TD_NEW_MSG_SEG(TDMT_SND_MSG)
  TD_DEF_MSG_TYPE(TDMT_SND_TASK_DEPLOY, "snode-task-deploy", SStreamTaskDeployReq, SStreamTaskDeployRsp)
  TD_DEF_MSG_TYPE(TDMT_SND_TASK_EXEC, "snode-task-exec", SStreamTaskExecReq, SStreamTaskExecRsp)
  TD_DEF_MSG_TYPE(TDMT_SND_TASK_PIPE_EXEC, "snode-task-pipe-exec", SStreamTaskExecReq, SStreamTaskExecRsp)
  TD_DEF_MSG_TYPE(TDMT_SND_TASK_MERGE_EXEC, "snode-task-merge-exec", SStreamTaskExecReq, SStreamTaskExecRsp)

  // Requests handled by SCHEDULER
  TD_NEW_MSG_SEG(TDMT_SCH_MSG)
  TD_DEF_MSG_TYPE(TDMT_SCH_LINK_BROKEN, "scheduler-link-broken", NULL, NULL)
  
  // Monitor info exchange between processes
  TD_NEW_MSG_SEG(TDMT_MON_MSG)
  TD_DEF_MSG_TYPE(TDMT_MON_MM_INFO, "monitor-minfo", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MON_VM_INFO, "monitor-vinfo", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MON_QM_INFO, "monitor-qinfo", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MON_SM_INFO, "monitor-sinfo", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MON_BM_INFO, "monitor-binfo", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MON_VM_LOAD, "monitor-vload", NULL, NULL)
  TD_DEF_MSG_TYPE(TDMT_MON_MM_LOAD, "monitor-mload", NULL, NULL)
  
#if defined(TD_MSG_NUMBER_)
  TDMT_MAX
#endif
};

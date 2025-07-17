/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */
#include <time.h>
#include <bench.h>
#include "benchLog.h"
#include <benchData.h>

typedef struct {
    tmq_t* tmq;
    int64_t  totalMsgs;
    int64_t  totalRows;

    int      id;
    FILE*    fpOfRowsFile;
} tmqThreadInfo;

static int running = 1;

void printfTmqConfigIntoFile() {
  if (NULL == g_arguments->fpOfInsertResult) {
      return;
  }

  infoPrintToFile( "%s\n", "============================================");

  SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;
  infoPrintToFile( "concurrent: %d\n", pConsumerInfo->concurrent);
  infoPrintToFile( "pollDelay: %d\n", pConsumerInfo->pollDelay);
  infoPrintToFile( "groupId: %s\n", pConsumerInfo->groupId);
  infoPrintToFile( "clientId: %s\n", pConsumerInfo->clientId);
  infoPrintToFile( "autoOffsetReset: %s\n", pConsumerInfo->autoOffsetReset);
  infoPrintToFile( "enableAutoCommit: %s\n", pConsumerInfo->enableAutoCommit);
  infoPrintToFile( "autoCommitIntervalMs: %d\n", pConsumerInfo->autoCommitIntervalMs);
  infoPrintToFile( "snapshotEnable: %s\n", pConsumerInfo->snapshotEnable);
  infoPrintToFile( "msgWithTableName: %s\n", pConsumerInfo->msgWithTableName);
  infoPrintToFile( "rowsFile: %s\n", pConsumerInfo->rowsFile);
  infoPrintToFile( "expectRows: %d\n", pConsumerInfo->expectRows);
  
  for (int i = 0; i < pConsumerInfo->topicCount; ++i) {
      infoPrintToFile( "topicName[%d]: %s\n", i, pConsumerInfo->topicName[i]);
      infoPrintToFile( "topicSql[%d]: %s\n", i, pConsumerInfo->topicSql[i]);
  }  
}


static int create_topic() {
    SBenchConn* conn = initBenchConn();
    if (conn == NULL) {
        return -1;
    }
    TAOS* taos = conn->taos;
    char command[SHORT_1K_SQL_BUFF_LEN];
    memset(command, 0, SHORT_1K_SQL_BUFF_LEN);

    SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;
    for (int i = 0; i < pConsumerInfo->topicCount; ++i) {
        char buffer[SHORT_1K_SQL_BUFF_LEN];
        memset(buffer, 0, SHORT_1K_SQL_BUFF_LEN);
        snprintf(buffer, SHORT_1K_SQL_BUFF_LEN, "create topic if not exists %s as %s",
                         pConsumerInfo->topicName[i], pConsumerInfo->topicSql[i]);
        TAOS_RES *res = taos_query(taos, buffer);

        if (taos_errno(res) != 0) {
            errorPrint("failed to create topic: %s, reason: %s\n",
                     pConsumerInfo->topicName[i], taos_errstr(res));
            taos_free_result(res);
            closeBenchConn(conn);
            return -1;
        }

        infoPrint("successfully create topic: %s\n", pConsumerInfo->topicName[i]);
        taos_free_result(res);
        if (g_arguments->terminate) {
            infoPrint("%s\n", "user cancel , so exit testing.");
            taos_free_result(res);
            closeBenchConn(conn);
            return -1;
        }
        
    }
    closeBenchConn(conn);
    return 0;
}

static tmq_list_t * buildTopicList() {
    tmq_list_t * topic_list = tmq_list_new();
    SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;
    for (int i = 0; i < pConsumerInfo->topicCount; ++i) {
        tmq_list_append(topic_list, pConsumerInfo->topicName[i]);
    }
    infoPrint("%s", "successfully build topic list\n");
    return topic_list;
}

static int32_t data_msg_process(TAOS_RES* msg, tmqThreadInfo* pInfo, int32_t msgIndex) {
  char* buf = (char*)calloc(1, 64*1024+8);
  if (NULL == buf) {
      errorPrint("consumer id %d calloc memory fail.\n", pInfo->id);
      return 0;
  }

  int32_t totalRows = 0;

  // infoPrint("topic: %s\n", tmq_get_topic_name(msg));
  int32_t     vgroupId = tmq_get_vgroup_id(msg);
  const char* dbName = tmq_get_db_name(msg);
  const char* tblName = tmq_get_table_name(msg);

  if (pInfo->fpOfRowsFile) {
    fprintf(pInfo->fpOfRowsFile, "consumerId: %d, msg index:%d\n", pInfo->id, msgIndex);
    fprintf(pInfo->fpOfRowsFile, "dbName: %s, tblname: %s, topic: %s, vgroupId: %d\n", dbName, tblName != NULL ? tblName : "invalid table",
                  tmq_get_topic_name(msg), vgroupId);
  }

  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);

    if (row == NULL) break;

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    const char* tbName = tmq_get_table_name(msg);

    taos_print_row(buf, row, fields, numOfFields);

    if (pInfo->fpOfRowsFile) {
        fprintf(pInfo->fpOfRowsFile, "tbname:%s, rows[%d]:\n%s\n", (tbName != NULL ? tbName : "null table"), totalRows, buf);
    }

    totalRows++;
  }
  free(buf);
  return totalRows;
}

int buildConsumerAndSubscribe(tmqThreadInfo * pThreadInfo, char* groupId) {
    int ret = 0;
    char tmpBuff[128] = {0};

    SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;

    tmq_list_t * topic_list = buildTopicList();
	
    tmq_conf_t * conf = tmq_conf_new();
	
    tmq_conf_set(conf, "td.connect.user", g_arguments->user);
    tmq_conf_set(conf, "td.connect.pass", g_arguments->password);
    tmq_conf_set(conf, "td.connect.ip", g_arguments->host);

    memset(tmpBuff, 0, sizeof(tmpBuff));
    snprintf(tmpBuff, 16, "%d", g_arguments->port);
    tmq_conf_set(conf, "td.connect.port", tmpBuff);

    tmq_conf_set(conf, "group.id", groupId);

    memset(tmpBuff, 0, sizeof(tmpBuff));
    snprintf(tmpBuff, 16, "%s_%d", pConsumerInfo->clientId, pThreadInfo->id);
    tmq_conf_set(conf, "client.id", tmpBuff);

    tmq_conf_set(conf, "auto.offset.reset", pConsumerInfo->autoOffsetReset);
    tmq_conf_set(conf, "enable.auto.commit", pConsumerInfo->enableAutoCommit);

    memset(tmpBuff, 0, sizeof(tmpBuff));
    snprintf(tmpBuff, 16, "%d", pConsumerInfo->autoCommitIntervalMs);
    tmq_conf_set(conf, "auto.commit.interval.ms", tmpBuff);

    tmq_conf_set(conf, "experimental.snapshot.enable", pConsumerInfo->snapshotEnable);
    tmq_conf_set(conf, "msg.with.table.name", pConsumerInfo->msgWithTableName);

    pThreadInfo->tmq = tmq_consumer_new(conf, NULL, 0);
    tmq_conf_destroy(conf);
    if (pThreadInfo->tmq == NULL) {
        errorPrint("%s", "failed to execute tmq_consumer_new\n");
        ret = -1;
	    tmq_list_destroy(topic_list);
        return ret;
    }
    infoPrint("thread[%d]: successfully create consumer\n", pThreadInfo->id);

	int32_t code = tmq_subscribe(pThreadInfo->tmq, topic_list);
    if (code) {
        errorPrint("failed to execute tmq_subscribe, reason: %s\n", tmq_err2str(code));
        ret = -1;
	    tmq_list_destroy(topic_list);
        return ret;
    }
    infoPrint("thread[%d]: successfully subscribe topics\n", pThreadInfo->id);
	tmq_list_destroy(topic_list);

    return ret;
}

static void* tmqConsume(void* arg) {
    tmqThreadInfo* pThreadInfo = (tmqThreadInfo*)arg;
	SConsumerInfo* pConsumerInfo = &g_tmqInfo.consumerInfo;
    char groupId[16] = {0};
	
	// "sequential" or "parallel"
	if (pConsumerInfo->createMode && 0 != strncasecmp(pConsumerInfo->createMode, "sequential", 10)) {			

        char* tPtr = pConsumerInfo->groupId;
	    // "share" or "independent"
	    if (pConsumerInfo->groupMode && 0 != strncasecmp(pConsumerInfo->groupMode, "share", 5)) {

			if ((NULL == pConsumerInfo->groupId) || (0 == strlen(pConsumerInfo->groupId))) {
				// rand string
				memset(groupId, 0, sizeof(groupId));
				rand_string(groupId, sizeof(groupId) - 1, 0);
				infoPrint("consumer id: %d generate rand group id: %s\n", pThreadInfo->id, groupId);
			    tPtr = groupId;
			}
	    }

		int ret = buildConsumerAndSubscribe(pThreadInfo, tPtr);
        if (0 != ret) {
            infoPrint("%s\n", "buildConsumerAndSubscribe() fail in tmqConsume()");
            return NULL;
        }
	}	

    int64_t totalMsgs = 0;
    int64_t totalRows = 0;
	int32_t manualCommit = 0;

    infoPrint("consumer id %d start to loop pull msg\n", pThreadInfo->id);

	if ((NULL != pConsumerInfo->enableManualCommit) && (0 == strncmp("true", pConsumerInfo->enableManualCommit, 4))) {
        manualCommit = 1;		
        infoPrint("consumer id %d enable manual commit\n", pThreadInfo->id);
	}

    int64_t  lastTotalMsgs = 0;
    int64_t  lastTotalRows = 0;
    uint64_t lastPrintTime = toolsGetTimestampMs();

    int32_t consumeDelay = pConsumerInfo->pollDelay == -1 ? -1 : pConsumerInfo->pollDelay;
    while (running) {
      TAOS_RES* tmqMsg = tmq_consumer_poll(pThreadInfo->tmq, consumeDelay);
      if (tmqMsg) {
        tmq_res_t msgType = tmq_get_res_type(tmqMsg);
        if (msgType == TMQ_RES_DATA) {
          totalRows += data_msg_process(tmqMsg, pThreadInfo, totalMsgs);
        } else {
          errorPrint("consumer id %d get error msg type: %d.\n", pThreadInfo->id, msgType);
          taos_free_result(tmqMsg);
          break;
        }

		if (0 != manualCommit) {
            tmq_commit_sync(pThreadInfo->tmq, tmqMsg);
		}
		
        taos_free_result(tmqMsg);

        totalMsgs++;

        int64_t currentPrintTime = toolsGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 10 * 1000) {
          infoPrint("consumer id %d has poll total msgs: %" PRId64 ", period rate: %.3f msgs/s, total rows: %" PRId64 ", period rate: %.3f rows/s\n",
              pThreadInfo->id, totalMsgs, (totalMsgs - lastTotalMsgs) * 1000.0 / (currentPrintTime - lastPrintTime), totalRows, (totalRows - lastTotalRows) * 1000.0 / (currentPrintTime - lastPrintTime));
          lastPrintTime = currentPrintTime;
          lastTotalMsgs = totalMsgs;
          lastTotalRows = totalRows;
        }

        if ((pConsumerInfo->expectRows > 0) && (totalRows > pConsumerInfo->expectRows)) {
            infoPrint("consumer id %d consumed rows: %" PRId64 " over than expect rows: %d, exit consume\n", pThreadInfo->id, totalRows, pConsumerInfo->expectRows);
            break;
        }
      } else {
        infoPrint("consumer id %d no poll more msg when time over, break consume\n", pThreadInfo->id);
        break;
      }
      if (g_arguments->terminate) {
        infoPrint("%s\n", "user cancel , so exit testing.");
        break;
      }
    }

    pThreadInfo->totalMsgs = totalMsgs;
    pThreadInfo->totalRows = totalRows;

    int32_t code;
    //code = tmq_unsubscribe(pThreadInfo->tmq);
    //if (code != 0) {
    //  errorPrint("thread %d tmq_unsubscribe() fail, reason: %s\n", i, tmq_err2str(code));
    //}

    code = tmq_consumer_close(pThreadInfo->tmq);
    if (code != 0) {
        errorPrint("thread %d tmq_consumer_close() fail, reason: %s\n",
                   pThreadInfo->id, tmq_err2str(code));
    }
    pThreadInfo->tmq = NULL;

    infoPrint("consumerId: %d, consume msgs: %" PRId64 ", consume rows: %" PRId64 "\n", pThreadInfo->id, totalMsgs, totalRows);
    infoPrintToFile(
            "consumerId: %d, consume msgs: %" PRId64 ", consume rows: %" PRId64 "\n", pThreadInfo->id, totalMsgs, totalRows);

    return NULL;
}

int subscribeTestProcess() {
    printfTmqConfigIntoFile();
    int ret = 0;
    SConsumerInfo*  pConsumerInfo = &g_tmqInfo.consumerInfo;
    if (pConsumerInfo->topicCount > 0) {
        if (create_topic()) {
            return -1;
        }
    }
    char groupId[16] = {0};
    char* tPtr = pConsumerInfo->groupId;

    // "share" or "independent"
    if (pConsumerInfo->groupMode && 0 == strncasecmp(pConsumerInfo->groupMode, "share", 5)) {
		if ((NULL == pConsumerInfo->groupId) || (0 == strlen(pConsumerInfo->groupId))) {
			// rand string
			memset(groupId, 0, sizeof(groupId));
			rand_string(groupId, sizeof(groupId) - 1, 0);
			infoPrint("rand generate group id: %s\n", groupId);
		    tPtr = groupId;
		}
    }
	
    pthread_t * pids = benchCalloc(pConsumerInfo->concurrent, sizeof(pthread_t), true);
    tmqThreadInfo *infos = benchCalloc(pConsumerInfo->concurrent, sizeof(tmqThreadInfo), true);

    for (int i = 0; i < pConsumerInfo->concurrent; ++i) {
        char tmpBuff[128] = {0};

        tmqThreadInfo * pThreadInfo = infos + i;
        pThreadInfo->totalMsgs = 0;
        pThreadInfo->totalRows = 0;
        pThreadInfo->id = i;

        if ( pConsumerInfo->rowsFile && strlen(pConsumerInfo->rowsFile)) {
            memset(tmpBuff, 0, sizeof(tmpBuff));
            snprintf(tmpBuff, 64, "%s_%d", pConsumerInfo->rowsFile, i);
            pThreadInfo->fpOfRowsFile = fopen(tmpBuff, "a");
            if (NULL == pThreadInfo->fpOfRowsFile) {
                errorPrint("failed to open %s file for save rows\n", pConsumerInfo->rowsFile);
                ret = -1;
                goto tmq_over;
            }
        }

        // "sequential" or "parallel"
		if (pConsumerInfo->createMode && 0 == strncasecmp(pConsumerInfo->createMode, "sequential", 10)) {			
            int retVal = buildConsumerAndSubscribe(pThreadInfo, tPtr);
            if (0 != retVal) {
                infoPrint("%s\n", "buildConsumerAndSubscribe() fail!");
                ret = -1;
                goto tmq_over;
            }
		}
        pthread_create(pids + i, NULL, tmqConsume, pThreadInfo);
    }

    for (int i = 0; i < pConsumerInfo->concurrent; i++) {
        pthread_join(pids[i], NULL);
    }

    int64_t totalMsgs = 0;
    int64_t totalRows = 0;

    for (int i = 0; i < pConsumerInfo->concurrent; i++) {
        tmqThreadInfo * pThreadInfo = infos + i;

        if (pThreadInfo->fpOfRowsFile) {
            fclose(pThreadInfo->fpOfRowsFile);
            pThreadInfo->fpOfRowsFile = NULL;
        }

        totalMsgs += pThreadInfo->totalMsgs;
        totalRows += pThreadInfo->totalRows;
    }

    infoPrint("Consumed total msgs: %" PRId64 ", total rows: %" PRId64 "\n",
              totalMsgs, totalRows);
    infoPrintToFile(
                    "Consumed total msgs: %" PRId64 ","
                    "total rows: %" PRId64 "\n", totalMsgs, totalRows);
    
    if (g_arguments->output_json_file) {
        tools_cJSON *root = tools_cJSON_CreateObject();
        if (root) {
            tools_cJSON_AddNumberToObject(root, "total_msgs", totalMsgs); 
            tools_cJSON_AddNumberToObject(root, "total_rows", totalRows); 
            char *jsonStr = tools_cJSON_PrintUnformatted(root);
            if (jsonStr) {
                FILE *fp = fopen(g_arguments->output_json_file, "w");
                if (fp) {
                    fprintf(fp, "%s\n", jsonStr);
                    fclose(fp);
                } else {
                    errorPrint("Failed to open output JSON file, file name %s\n",
                            g_arguments->output_json_file);
                }
                free(jsonStr);
            }
            tools_cJSON_Delete(root);
        }
    }
    
tmq_over:
    free(pids);
    free(infos);
    return ret;
}

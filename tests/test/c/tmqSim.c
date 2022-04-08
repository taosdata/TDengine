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

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <dirent.h>

#include "taos.h"
#include "taoserror.h"
#include "tlog.h"

#define GREEN "\033[1;32m"
#define NC "\033[0m"
#define min(a, b) (((a) < (b)) ? (a) : (b))

#define  MAX_SQL_STR_LEN    (1024 * 1024)
#define  MAX_ROW_STR_LEN    (16 * 1024)

typedef struct {
    // input from argvs
	char	dbName[32];
    char    topicString[256];
	char    keyString[1024];
	int32_t showMsgFlag;

    // save result after parse agrvs	
	int32_t numOfTopic;
	char    topics[32][64];
	
	int32_t numOfKey;
	char    key[32][64];
	char    value[32][64];
} SConfInfo;

static SConfInfo g_stConfInfo;

//char* g_pRowValue = NULL;
//TdFilePtr g_fp = NULL;

static void printHelp() {
  char indent[10] = "        ";
  printf("Used to test the tmq feature with sim cases\n");

  printf("%s%s\n", indent, "-c");
  printf("%s%s%s%s\n", indent, indent, "Configuration directory, default is ", configDir);
  printf("%s%s\n", indent, "-d");
  printf("%s%s%s\n", indent, indent, "The name of the database for cosumer, no default ");
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s\n", indent, indent, "The topic string for cosumer, no default ");
  printf("%s%s\n", indent, "-k");
  printf("%s%s%s\n", indent, indent, "The key-value string for cosumer, no default ");
  printf("%s%s\n", indent, "-g");
  printf("%s%s%s%d\n", indent, indent, "showMsgFlag, default is ", g_stConfInfo.showMsgFlag);
  exit(EXIT_SUCCESS);
}

void parseArgument(int32_t argc, char *argv[]) {

  memset(&g_stConfInfo, 0, sizeof(SConfInfo));
  
  for (int32_t i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(0);
    } else if (strcmp(argv[i], "-d") == 0) {
      strcpy(g_stConfInfo.dbName, argv[++i]);
    } else if (strcmp(argv[i], "-c") == 0) {
      strcpy(configDir, argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0) {
      strcpy(g_stConfInfo.topicString, argv[++i]);
    } else if (strcmp(argv[i], "-k") == 0) {
      strcpy(g_stConfInfo.keyString, argv[++i]);
    } else if (strcmp(argv[i], "-g") == 0) {
      g_stConfInfo.showMsgFlag = atol(argv[++i]);
    } else {
      printf("%s unknow para: %s %s", GREEN, argv[++i], NC);
	  exit(-1);
    }
  }

#if 0
  pPrint("%s configDir:%s %s", GREEN, configDir, NC);
  pPrint("%s dbName:%s %s", GREEN, g_stConfInfo.dbName, NC);
  pPrint("%s topicString:%s %s", GREEN, g_stConfInfo.topicString, NC);
  pPrint("%s keyString:%s %s", GREEN, g_stConfInfo.keyString, NC);  
  pPrint("%s showMsgFlag:%d %s", GREEN, g_stConfInfo.showMsgFlag, NC);
#endif  
}

void splitStr(char **arr, char *str, const char *del) {
  char *s = strtok(str, del);
  while(s != NULL) {
    *arr++ = s;
    s = strtok(NULL, del);
  }
}

void ltrim(char *str) 
{ 
    if (str == NULL || *str == '\0') 
    { 
        return; 
    } 
    int len = 0; 
    char *p = str;
    while (*p != '\0' && isspace(*p)) 
    { 
        ++p; ++len; 
    } 
    memmove(str, p, strlen(str) - len + 1); 
    //return str; 
}


void parseInputString() {
  //printf("topicString: %s\n", g_stConfInfo.topicString);
  //printf("keyString: %s\n\n", g_stConfInfo.keyString);  

  char *token;
  const char delim[2] = ",";
  const char ch = ':';

  token = strtok(g_stConfInfo.topicString, delim);
  while(token != NULL) {
    //printf("%s\n", token );
	strcpy(g_stConfInfo.topics[g_stConfInfo.numOfTopic], token);  
    ltrim(g_stConfInfo.topics[g_stConfInfo.numOfTopic]);
	//printf("%s\n", g_stConfInfo.topics[g_stConfInfo.numOfTopic]);
	g_stConfInfo.numOfTopic++;
	
    token = strtok(NULL, delim);
  }

  token = strtok(g_stConfInfo.keyString, delim);
  while(token != NULL) {
    //printf("%s\n", token );
  	{
	char* pstr = token;
	ltrim(pstr);
	char *ret = strchr(pstr, ch);
	memcpy(g_stConfInfo.key[g_stConfInfo.numOfKey], pstr, ret-pstr);	
	strcpy(g_stConfInfo.value[g_stConfInfo.numOfKey], ret+1);
	//printf("key: %s, value: %s\n", g_stConfInfo.key[g_stConfInfo.numOfKey], g_stConfInfo.value[g_stConfInfo.numOfKey]);
	g_stConfInfo.numOfKey++;
    }
	
    token = strtok(NULL, delim);
  }
}


static int  running = 1;
static void msg_process(tmq_message_t* message) { tmqShowMsg(message); }


int queryDB(TAOS *taos, char *command) {
	TAOS_RES *pRes = taos_query(taos, command);
	int code = taos_errno(pRes);
	//if ((code != 0) && (code != TSDB_CODE_RPC_AUTH_REQUIRED)) {
	if (code != 0) {
	  pError("failed to reason:%s, sql: %s", tstrerror(code), command);
	  taos_free_result(pRes);
	  return -1;
	}
	taos_free_result(pRes);
	return 0 ;
}

tmq_t* build_consumer() {
  char sqlStr[1024] = {0};
  
  TAOS* pConn = taos_connect(NULL, "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  sprintf(sqlStr, "use %s", g_stConfInfo.dbName);
  TAOS_RES* pRes = taos_query(pConn, sqlStr);
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
	taos_free_result(pRes);
	exit(-1);
  }
  taos_free_result(pRes);

  tmq_conf_t* conf = tmq_conf_new();
  //tmq_conf_set(conf, "group.id", "tg2");
  for (int32_t i = 0; i < g_stConfInfo.numOfKey; i++) {
    tmq_conf_set(conf, g_stConfInfo.key[i], g_stConfInfo.value[i]);
  }
  tmq_t* tmq = tmq_consumer_new(pConn, conf, NULL, 0);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  //tmq_list_append(topic_list, "test_stb_topic_1");
  for (int32_t i = 0; i < g_stConfInfo.numOfTopic; i++) {
    tmq_list_append(topic_list, g_stConfInfo.topics[i]);
  }
  return topic_list;
}

void loop_consume(tmq_t* tmq) {
  tmq_resp_err_t err;

  int32_t totalMsgs = 0;
  int32_t totalRows = 0;
  int32_t skipLogNum = 0;
  while (running) {
    tmq_message_t* tmqMsg = tmq_consumer_poll(tmq, 4000);
    if (tmqMsg) {
	  totalMsgs++;

	  #if 0
	  TAOS_ROW row;
	  while (NULL != (row = tmq_get_row(tmqMsg))) {
        totalRows++;
	  }
	  #endif
	  
      skipLogNum += tmqGetSkipLogNum(tmqMsg);
	  if (0 != g_stConfInfo.showMsgFlag) {
        msg_process(tmqMsg);
	  }
      tmq_message_destroy(tmqMsg);
    } else {
      break;
    }
  }

  err = tmq_consumer_close(tmq);
  if (err) {
    printf("tmq_consumer_close() fail, reason: %s\n", tmq_err2str(err));
	exit(-1);
  }
  
  printf("{consume success: %d, %d}", totalMsgs, totalRows);
}

int main(int32_t argc, char *argv[]) {
  parseArgument(argc, argv);
  parseInputString();
    
  tmq_t*      tmq        = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  if ((NULL == tmq) || (NULL == topic_list)){
    return -1;
  }
  
  tmq_resp_err_t err = tmq_subscribe(tmq, topic_list);
  if (err) {
    printf("tmq_subscribe() fail, reason: %s\n", tmq_err2str(err));
    exit(-1);
  }
  
  loop_consume(tmq);

  err = tmq_unsubscribe(tmq);
  if (err) {
    printf("tmq_unsubscribe() fail, reason: %s\n", tmq_err2str(err));
    exit(-1);
  }

  return 0;
}


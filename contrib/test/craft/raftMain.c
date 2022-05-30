#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <getopt.h>
#include <time.h>
#include <stdlib.h>
#include <getopt.h>
#include <raft.h>
#include <raft/uv.h>
#include "raftServer.h"
#include "tcommon.h"

const char *exe_name;

void parseAddr(const char *addr, char *host, int len, uint32_t *port) {
	char* tmp = (char*)malloc(strlen(addr) + 1);
	strcpy(tmp, addr);

	char* context;
	char* separator = ":";
	char* token = strtok_r(tmp, separator, &context);
	if (token) {
		snprintf(host, len, "%s", token);
	}

	token = strtok_r(NULL, separator, &context);
	if (token) {
		sscanf(token, "%u", port);
	}

	free(tmp);
}

// only parse 3 tokens
int parseCommand(const char* str, char* token1, char* token2, char* token3, int len)
{
    char* tmp = (char*)malloc(strlen(str) + 1);
    strcpy(tmp, str);

    char* context;
    char* separator = " ";
    int n = 0;

    char* token = strtok_r(tmp, separator, &context);
    if (!token) {
 	   goto ret;
    }
    if (strcmp(token, "") != 0) {
    	strncpy(token1, token, len);
    	n++;
    }

    token = strtok_r(NULL, separator, &context);
    if (!token) {
    	goto ret;
   	}
    if (strcmp(token, "") != 0) {
    	strncpy(token2, token, len);
    	n++;
    }

    token = strtok_r(NULL, separator, &context);
    if (!token) {
    	goto ret;
    }
    if (strcmp(token, "") != 0) {
    	strncpy(token3, token, len);
    	n++;
    }

ret:
    return n;
    free(tmp);
}

void *startServerFunc(void *param) {
	SRaftServer *pServer = (SRaftServer*)param;
	int32_t r = raftServerStart(pServer);
	assert(r == 0);

	return NULL;
}

// Console ---------------------------------
const char* state2String(unsigned short state) {
    if (state == RAFT_UNAVAILABLE) {
        return "RAFT_UNAVAILABLE";

    } else if (state == RAFT_FOLLOWER) {
        return "RAFT_FOLLOWER";

    } else if (state == RAFT_CANDIDATE) {
        return "RAFT_CANDIDATE";

    } else if (state == RAFT_LEADER) {
        return "RAFT_LEADER";

    }
    return "UNKNOWN_RAFT_STATE";
}

void printRaftConfiguration(struct raft_configuration *c) {
	printf("configuration: \n");
	for (int i = 0; i < c->n; ++i) {
		printf("%llu -- %d -- %s\n", c->servers[i].id, c->servers[i].role, c->servers[i].address);
	}
}

void printRaftState(struct raft *r) {
    printf("----Raft State: -----------\n");
    printf("my_id: %llu \n", r->id);
    printf("address: %s \n", r->address);
    printf("current_term: %llu \n", r->current_term);
    printf("voted_for: %llu \n", r->voted_for);
    printf("role: %s \n", state2String(r->state));
    printf("commit_index: %llu \n", r->commit_index);
    printf("last_applied: %llu \n", r->last_applied);
    printf("last_stored: %llu \n", r->last_stored);

    printf("configuration_index: %llu \n", r->configuration_index);
    printf("configuration_uncommitted_index: %llu \n", r->configuration_uncommitted_index);
	printRaftConfiguration(&r->configuration);

	printf("----------------------------\n");
}

void putValueCb(struct raft_apply *req, int status, void *result) {
    raft_free(req);
	struct raft *r = req->data;
	if (status != 0) {
		printf("putValueCb: %s \n", raft_errmsg(r));
	} else {
		printf("putValueCb: %s \n", "ok");
	}
}

void putValue(struct raft *r, const char *value) {
    struct raft_buffer buf;

    buf.len = TOKEN_LEN;;
    buf.base = raft_malloc(buf.len);
	snprintf(buf.base, buf.len, "%s", value);

    struct raft_apply *req = raft_malloc(sizeof(struct raft_apply));
	req->data = r;
    int ret = raft_apply(r, req, &buf, 1, putValueCb);
	if (ret == 0) {
    	printf("put %s \n", (char*)buf.base);
	} else {
		printf("put error: %s \n", raft_errmsg(r));
	}
}

void getValue(const char *key) {
	char *ptr = getKV(key);
	if (ptr) {
		printf("get value: [%s] \n", ptr);
	} else {
		printf("value not found for key: [%s] \n", key);
	}
}

void raft_change_cb_add(struct raft_change *req, int status) {
	printf("raft_change_cb_add status:%d ... \n", status);
}

void raft_change_cb_assign(struct raft_change *req, int status) {
	printf("raft_change_cb_assign status:%d ... \n", status);
}

void raft_change_cb_remove(struct raft_change *req, int status) {
	printf("raft_change_cb_remove status:%d ... \n", status);
}

void console(SRaftServer *pRaftServer) {
	while (1) {
		char cmd_buf[COMMAND_LEN];
		memset(cmd_buf, 0, sizeof(cmd_buf));
		char *ret = fgets(cmd_buf, COMMAND_LEN, stdin);
		if (!ret) {
			exit(-1);
		}

		int pos = strlen(cmd_buf);                  
		if(cmd_buf[pos - 1] == '\n') {
			cmd_buf[pos - 1] = '\0';
		}

		if (strncmp(cmd_buf, "", COMMAND_LEN) == 0) {
			continue;
		}

		char cmd[TOKEN_LEN];
		memset(cmd, 0, sizeof(cmd));

		char param1[TOKEN_LEN];
		memset(param1, 0, sizeof(param1));

		char param2[TOKEN_LEN];
		memset(param2, 0, sizeof(param2));

		parseCommand(cmd_buf, cmd, param1, param2, TOKEN_LEN);
		if (strcmp(cmd, "addnode") == 0) {
			//printf("not support \n");

			char host[HOST_LEN];
			uint32_t port;
			parseAddr(param1, host, HOST_LEN, &port);
			uint64_t rid = raftId(host, port);

			struct raft_change *req = raft_malloc(sizeof(*req));
			int r = raft_add(&pRaftServer->raft, req, rid, param1, raft_change_cb_add);
			if (r != 0) {
				printf("raft_add error: %s \n", raft_errmsg(&pRaftServer->raft));
			}
			printf("add node: %lu %s \n", rid, param1);

			struct raft_change *req2 = raft_malloc(sizeof(*req2));
			r = raft_assign(&pRaftServer->raft, req2, rid, RAFT_VOTER, raft_change_cb_assign);
			if (r != 0) {
				printf("raft_assign error: %s \n", raft_errmsg(&pRaftServer->raft));
			}
			printf("raft_assign: %s %d \n", param1, RAFT_VOTER);

		} else if (strcmp(cmd, "activate") == 0) {
			char host[HOST_LEN];
			uint32_t port;
			parseAddr(param1, host, HOST_LEN, &port);
			uint64_t rid = raftId(host, port);


			struct raft_change *req2 = raft_malloc(sizeof(*req2));
			int r = raft_assign(&pRaftServer->raft, req2, rid, RAFT_VOTER, raft_change_cb_assign);
			if (r != 0) {
				printf("raft_assign error: %s \n", raft_errmsg(&pRaftServer->raft));
			}
			printf("raft_assign: %s %d \n", param1, RAFT_VOTER);





		} else if (strcmp(cmd, "dropnode") == 0) {
			char host[HOST_LEN] = {0};
			uint32_t port;
			parseAddr(param1, host, HOST_LEN, &port);
			uint64_t rid = raftId(host, port);
			
			struct raft_change *req = raft_malloc(sizeof(*req));
			int r = raft_remove(&pRaftServer->raft, req, rid, raft_change_cb_remove);
			if (r != 0) {
				printf("raft_remove: %s \n", raft_errmsg(&pRaftServer->raft));
			}
			printf("drop node: %lu %s \n", rid, param1);
			


		} else if (strcmp(cmd, "put") == 0) {
			char buf[256] = {0};
			snprintf(buf, sizeof(buf), "%s--%s", param1, param2);
			putValue(&pRaftServer->raft, buf);

		} else if (strcmp(cmd, "get") == 0) {
			getValue(param1);

		} else if (strcmp(cmd, "state") == 0) {
			printRaftState(&pRaftServer->raft);

		} else if (strcmp(cmd, "snapshot") == 0) {
			printf("not support \n");

		} else if (strcmp(cmd, "help") == 0) {
			printf("addnode \"127.0.0.1:8888\" \n");
			printf("activate \"127.0.0.1:8888\" \n");
			printf("dropnode \"127.0.0.1:8888\" \n");
			printf("put key value \n");
			printf("get key \n");
			printf("state \n");

		} else {
			printf("unknown command: [%s], type \"help\" to see help \n", cmd);
		}

		//printf("cmd_buf: [%s] \n", cmd_buf);
	}
}

void *startConsoleFunc(void *param) {
	SRaftServer *pServer = (SRaftServer*)param;
	console(pServer);
	return NULL;
}

// Config ---------------------------------
void usage() {
	printf("\nusage: \n");
	printf("%s --me=127.0.0.1:10000 --dir=./data --voter \n", exe_name);
	printf("%s --me=127.0.0.1:10001 --dir=./data \n", exe_name);
	printf("%s --me=127.0.0.1:10002 --dir=./data \n", exe_name);
	printf("\n");
	printf("%s --me=127.0.0.1:10000 --peers=127.0.0.1:10001,127.0.0.1:10002 --dir=./data \n", exe_name);
	printf("%s --me=127.0.0.1:10001 --peers=127.0.0.1:10000,127.0.0.1:10002 --dir=./data \n", exe_name);
	printf("%s --me=127.0.0.1:10002 --peers=127.0.0.1:10000,127.0.0.1:10001 --dir=./data \n", exe_name);
	printf("\n");
}

void parseConf(int argc, char **argv, SRaftServerConfig *pConf) {
	memset(pConf, 0, sizeof(*pConf));

    int option_index, option_value;
    option_index = 0;
    static struct option long_options[] = {
        {"help", no_argument, NULL, 'h'},
        {"voter", no_argument, NULL, 'v'},
        {"peers", required_argument, NULL, 'p'},
        {"me", required_argument, NULL, 'm'},
        {"dir", required_argument, NULL, 'd'},
        {NULL, 0, NULL, 0}
    };

	pConf->voter = 0;
    while ((option_value = getopt_long(argc, argv, "hvp:m:d:", long_options, &option_index)) != -1) {
        switch (option_value) {
        	case 'm': {
				parseAddr(optarg, pConf->me.host, sizeof(pConf->me.host), &pConf->me.port);
				break;
        	}

			case 'p': {
				char tokens[MAX_PEERS][MAX_TOKEN_LEN];
				int peerCount = splitString(optarg, ",", tokens, MAX_PEERS);
				pConf->peersCount = peerCount;
				for (int i = 0; i < peerCount; ++i) {
					Addr *pAddr = &pConf->peers[i];
					parseAddr(tokens[i], pAddr->host, sizeof(pAddr->host), &pAddr->port);
				}
				break;
			}

			case 'v': {
				pConf->voter = 1;
				break;
			}

			case 'd': {
				snprintf(pConf->dir, sizeof(pConf->dir), "%s", optarg);
				break;
			}

			case 'h': {
				usage();
				exit(-1);
			}

			default: {
				usage();
				exit(-1);
			}
		}
	}
	snprintf(pConf->dataDir, sizeof(pConf->dataDir), "%s/%s:%u", pConf->dir, pConf->me.host, pConf->me.port);
}

void printConf(SRaftServerConfig *pConf) {
	printf("\nconf: \n");
	printf("me: %s:%u \n", pConf->me.host, pConf->me.port);
	printf("peersCount: %d \n", pConf->peersCount);
	for (int i = 0; i < pConf->peersCount; ++i) {
		Addr *pAddr = &pConf->peers[i];
		printf("peer%d: %s:%u \n", i, pAddr->host, pAddr->port);
	}
	printf("dataDir: %s \n\n", pConf->dataDir);

}


int main(int argc, char **argv) { 
	taosSeedRand(time(NULL));
	int32_t ret;

	exe_name = argv[0];
	if (argc < 3) {
		usage();
		exit(-1);
	}

	signal(SIGPIPE, SIG_IGN);

	SRaftServerConfig conf;
	parseConf(argc, argv, &conf);
	printConf(&conf);	

	char cmd_buf[COMMAND_LEN];
    snprintf(cmd_buf, sizeof(cmd_buf), "mkdir -p %s", conf.dataDir);
    system(cmd_buf);

	struct raft_fsm fsm;
	initFsm(&fsm);

	SRaftServer raftServer;
	ret = raftServerInit(&raftServer, &conf, &fsm);
	assert(ret == 0);

	pthread_t tidRaftServer;
	pthread_create(&tidRaftServer, NULL, startServerFunc, &raftServer);

	pthread_t tidConsole;
	pthread_create(&tidConsole, NULL, startConsoleFunc, &raftServer);

	while (1) {
		sleep(10);
	}

	return 0;
}

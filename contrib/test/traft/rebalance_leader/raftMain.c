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
#include "common.h"

const char *exe_name;

typedef struct LeaderState {
	char address[48];
	int leaderCount;

} LeaderState;

#define NODE_COUNT 3
LeaderState leaderStates[NODE_COUNT];

void printLeaderCount() {
	for (int i = 0; i < NODE_COUNT; ++i) {
		printf("%s: leaderCount:%d \n", leaderStates[i].address, leaderStates[i].leaderCount);
	}
}

void updateLeaderStates(SRaftServer *pRaftServer) {
	for (int i = 0; i < pRaftServer->instance[0].raft.configuration.n; ++i) {
		snprintf(leaderStates[i].address, sizeof(leaderStates[i].address), "%s", pRaftServer->instance[0].raft.configuration.servers[i].address);
		leaderStates[i].leaderCount = 0;
	}

	for (int i = 0; i < pRaftServer->instanceCount; ++i) {
	    struct raft *r = &pRaftServer->instance[i].raft;

    	char leaderAddress[128];
    	memset(leaderAddress, 0, sizeof(leaderAddress));

    	if (r->state == RAFT_LEADER) {
    	    snprintf(leaderAddress, sizeof(leaderAddress), "%s", r->address);
    	} else if (r->state == RAFT_FOLLOWER) {
    	    snprintf(leaderAddress, sizeof(leaderAddress), "%s", r->follower_state.current_leader.address);
    	}

		for (int j = 0; j < NODE_COUNT; j++) {
			if (strcmp(leaderAddress, leaderStates[j].address) == 0) {
				leaderStates[j].leaderCount++;
			}
		}
	}
}


void raftTransferCb(struct raft_transfer *req) {
	SRaftServer *pRaftServer = req->data;
	raft_free(req);

	//printf("raftTransferCb: \n");
	updateLeaderStates(pRaftServer);
	//printLeaderCount();

	int myLeaderCount;
	for (int i = 0; i < NODE_COUNT; ++i) {
		if (strcmp(pRaftServer->address, leaderStates[i].address) == 0) {
			myLeaderCount = leaderStates[i].leaderCount;
		}
	}

	//printf("myLeaderCount:%d waterLevel:%d \n", myLeaderCount, pRaftServer->instanceCount / NODE_COUNT);
	if (myLeaderCount > pRaftServer->instanceCount / NODE_COUNT) {
		struct raft *r;
		for (int j = 0; j < pRaftServer->instanceCount; ++j) {
			if (pRaftServer->instance[j].raft.state == RAFT_LEADER) {
				r = &pRaftServer->instance[j].raft;
				break;
			}
		}

		struct raft_transfer *transfer = raft_malloc(sizeof(*transfer));
		transfer->data = pRaftServer;

		uint64_t destRaftId;
		int minIndex = -1;
		int minLeaderCount = myLeaderCount;
		for (int j = 0; j < NODE_COUNT; ++j) {
			if (strcmp(leaderStates[j].address, pRaftServer->address) == 0) {
				continue;
			}

			if (leaderStates[j].leaderCount <= minLeaderCount) {
				minLeaderCount = leaderStates[j].leaderCount;
				minIndex = j;
			}
		}


		char myHost[48];
		uint16_t myPort;
		uint16_t myVid;
		decodeRaftId(r->id, myHost, sizeof(myHost), &myPort, &myVid);
	
	
		//printf("raftTransferCb transfer leader: vid[%u] choose: index:%d, leaderStates[%d].address:%s, leaderStates[%d].leaderCount:%d \n", minIndex, minIndex, leaderStates[minIndex].address, minIndex, leaderStates[minIndex].leaderCount);

		char *destAddress = leaderStates[minIndex].address;

		char tokens[MAX_PEERS][MAX_TOKEN_LEN];
		splitString(destAddress, ":", tokens, 2);
		char *destHost = tokens[0];
		uint16_t destPort = atoi(tokens[1]);
		destRaftId = encodeRaftId(destHost, destPort, myVid);

		printf("\nraftTransferCb transfer leader: vgroupId:%u from:%s:%u --> to:%s:%u ", myVid, myHost, myPort, destHost, destPort);
		fflush(stdout);

		raft_transfer(r, transfer, destRaftId, raftTransferCb);			
	}

}


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
int parseCommand3(const char* str, char* token1, char* token2, char* token3, int len)
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

// only parse 4 tokens
int parseCommand4(const char* str, char* token1, char* token2, char* token3, char *token4, int len)
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

    token = strtok_r(NULL, separator, &context);
    if (!token) {
    	goto ret;
    }
    if (strcmp(token, "") != 0) {
    	strncpy(token4, token, len);
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


void printRaftState2(struct raft *r) {
	char leaderAddress[128];
	memset(leaderAddress, 0, sizeof(leaderAddress));

	if (r->state == RAFT_LEADER) {
		snprintf(leaderAddress, sizeof(leaderAddress), "%s", r->address);
	} else if (r->state == RAFT_FOLLOWER) {
		snprintf(leaderAddress, sizeof(leaderAddress), "%s", r->follower_state.current_leader.address);
	}

	for (int i = 0; i < r->configuration.n; ++i) {
		char tmpAddress[128];
		snprintf(tmpAddress, sizeof(tmpAddress), "%s", r->configuration.servers[i].address);

		uint64_t raftId = r->configuration.servers[i].id;
		char host[128];
		uint16_t port;
		uint16_t vid;
		decodeRaftId(raftId, host, 128, &port, &vid);

		char buf[512];
		memset(buf, 0, sizeof(buf));
		if (strcmp(tmpAddress, leaderAddress) == 0) {
			snprintf(buf, sizeof(buf), "<%s:%u-%u-LEADER>\t", host, port, vid);
		} else {
			snprintf(buf, sizeof(buf), "<%s:%u-%u-FOLLOWER>\t", host, port, vid);
		}
		printf("%s", buf);
	}
	printf("\n");
}

void printRaftConfiguration(struct raft_configuration *c) {
	printf("configuration: \n");
	for (int i = 0; i < c->n; ++i) {
		printf("%llu -- %d -- %s\n", c->servers[i].id, c->servers[i].role, c->servers[i].address);
	}
}

void printRaftState(struct raft *r) {
    printf("----Raft State: -----------\n");
    printf("mem_addr: %p \n", r);
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

void console(SRaftServer *pRaftServer) {
	while (1) {
		char cmd_buf[COMMAND_LEN];
		memset(cmd_buf, 0, sizeof(cmd_buf));
		printf("(console)> ");
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

		char param3[TOKEN_LEN];
		memset(param2, 0, sizeof(param2));

		parseCommand4(cmd_buf, cmd, param1, param2, param3, TOKEN_LEN);
		if (strcmp(cmd, "addnode") == 0) {
			printf("not support \n");

			/*
			char host[HOST_LEN];
			uint32_t port;
			parseAddr(param1, host, HOST_LEN, &port);
			uint64_t rid = raftId(host, port);

			struct raft_change *req = raft_malloc(sizeof(*req));
			int r = raft_add(&pRaftServer->raft, req, rid, param1, NULL);
			if (r != 0) {
				printf("raft_add: %s \n", raft_errmsg(&pRaftServer->raft));
			}
			printf("add node: %lu %s \n", rid, param1);

			struct raft_change *req2 = raft_malloc(sizeof(*req2));
			r = raft_assign(&pRaftServer->raft, req2, rid, RAFT_VOTER, NULL);
			if (r != 0) {
				printf("raft_assign: %s \n", raft_errmsg(&pRaftServer->raft));
			}
			*/

		} else if (strcmp(cmd, "dropnode") == 0) {
			printf("not support \n");

		} else if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
			exit(0);

		} else if (strcmp(cmd, "rebalance") == 0 && strcmp(param1, "leader") == 0) {

			/*
			updateLeaderStates(pRaftServer);

		    int myLeaderCount;
		    for (int i = 0; i < NODE_COUNT; ++i) {
		        if (strcmp(pRaftServer->address, leaderStates[i].address) == 0) {
		            myLeaderCount = leaderStates[i].leaderCount;
		        }
		    }
		
		    while (myLeaderCount > pRaftServer->instanceCount / NODE_COUNT) {
		    	printf("myLeaderCount:%d waterLevel:%d \n", myLeaderCount, pRaftServer->instanceCount / NODE_COUNT);

		        struct raft *r;
		        for (int j = 0; j < pRaftServer->instanceCount; ++j) {
		            if (pRaftServer->instance[j].raft.state == RAFT_LEADER) {
		                r = &pRaftServer->instance[j].raft;
		            }
		        }
		
		        struct raft_transfer *transfer = raft_malloc(sizeof(*transfer));
		        transfer->data = pRaftServer;
		
		        uint64_t destRaftId;
		        int minIndex = -1;
		        int minLeaderCount = myLeaderCount;
		        for (int j = 0; j < NODE_COUNT; ++j) {
		            if (strcmp(leaderStates[j].address, pRaftServer->address) == 0) continue;

					printf("-----leaderStates[%d].leaderCount:%d \n", j, leaderStates[j].leaderCount);
		            if (leaderStates[j].leaderCount <= minLeaderCount) {
		                minIndex = j;
						printf("++++ assign minIndex : %d \n", minIndex);
		            }
		        }
		
				printf("minIndex:%d minLeaderCount:%d \n", minIndex, minLeaderCount);

		        char myHost[48];
		        uint16_t myPort;
		        uint16_t myVid;
		        decodeRaftId(r->id, myHost, sizeof(myHost), &myPort, &myVid);
		
		        char *destAddress = leaderStates[minIndex].address;
		
		        char tokens[MAX_PEERS][MAX_TOKEN_LEN];
		        splitString(destAddress, ":", tokens, 2);
		        char *destHost = tokens[0];
		        uint16_t destPort = atoi(tokens[1]);
		        destRaftId = encodeRaftId(destHost, destPort, myVid);
		
				printf("destHost:%s destPort:%u myVid:%u", destHost, destPort, myVid);
		        raft_transfer(r, transfer, destRaftId, raftTransferCb);
				sleep(1);

			    for (int i = 0; i < NODE_COUNT; ++i) {
		        	if (strcmp(pRaftServer->address, leaderStates[i].address) == 0) {
		        	    myLeaderCount = leaderStates[i].leaderCount;
		        	}
		    	}
		    }
			*/

			
			int leaderCount = 0;

			struct raft *firstR;
			for (int i = 0; i < pRaftServer->instanceCount; ++i) {
				struct raft *r = &pRaftServer->instance[i].raft;
				if (r->state == RAFT_LEADER) {
					leaderCount++;
					firstR = r;
				}
			}

			if (leaderCount > pRaftServer->instanceCount / NODE_COUNT) {
				struct raft_transfer *transfer = raft_malloc(sizeof(*transfer));
				transfer->data = pRaftServer;
				raft_transfer(firstR, transfer, 0, raftTransferCb);			
			}


		} else if (strcmp(cmd, "put") == 0) {
			char buf[256];
			uint16_t vid;
			sscanf(param1, "%hu", &vid);
			snprintf(buf, sizeof(buf), "%s--%s", param2, param3);
			putValue(&pRaftServer->instance[vid].raft, buf);

		} else if (strcmp(cmd, "get") == 0) {
			getValue(param1);

		} else if (strcmp(cmd, "transfer") == 0) {
			uint16_t vid;
			sscanf(param1, "%hu", &vid);

			struct raft_transfer transfer;
			raft_transfer(&pRaftServer->instance[vid].raft, &transfer, 0, NULL);			


		} else if (strcmp(cmd, "state") == 0) {
			for (int i = 0; i < pRaftServer->instanceCount; ++i) {
				printf("instance %d: ", i);
				printRaftState(&pRaftServer->instance[i].raft);
			}

		} else if (strcmp(cmd, "leader") == 0 && strcmp(param1, "state") == 0) {
			updateLeaderStates(pRaftServer);
			printf("\n--------------------------------------------\n");
			printLeaderCount();
			for (int i = 0; i < pRaftServer->instanceCount; ++i) {
				printRaftState2(&pRaftServer->instance[i].raft);
			}
			printf("--------------------------------------------\n");

		} else if (strcmp(cmd, "snapshot") == 0) {
			printf("not support \n");

		} else if (strcmp(cmd, "help") == 0) {
			printf("addnode \"127.0.0.1:8888\" \n");
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
	printf("%s --me=127.0.0.1:10000 --dir=./data \n", exe_name);
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
        {"peers", required_argument, NULL, 'p'},
        {"me", required_argument, NULL, 'm'},
        {"dir", required_argument, NULL, 'd'},
        {NULL, 0, NULL, 0}
    };

    while ((option_value = getopt_long(argc, argv, "hp:m:d:", long_options, &option_index)) != -1) {
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
	snprintf(pConf->dataDir, sizeof(pConf->dataDir), "%s/%s_%u", pConf->dir, pConf->me.host, pConf->me.port);
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
	srand(time(NULL));
	int32_t ret;

	exe_name = argv[0];
	if (argc < 3) {
		usage();
		exit(-1);
	}

	SRaftServerConfig conf;
	parseConf(argc, argv, &conf);
	printConf(&conf);	

	signal(SIGPIPE, SIG_IGN);

	/*
	char cmd_buf[COMMAND_LEN];
    snprintf(cmd_buf, sizeof(cmd_buf), "mkdir -p %s", conf.dataDir);
    system(cmd_buf);
	*/


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

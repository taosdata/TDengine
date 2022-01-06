#include <unistd.h>
#include <stdlib.h>
#include "common.h"
#include "raftServer.h"

//char *keys = malloc(MAX_RECORD_COUNT * MAX_KV_LEN);;
//char *values = malloc(MAX_RECORD_COUNT * MAX_KV_LEN);


char keys[MAX_KV_LEN][MAX_RECORD_COUNT];
char values[MAX_KV_LEN][MAX_RECORD_COUNT];
int writeIndex = 0;

void initStore() {
}

void destroyStore() {
	//free(keys);
	//free(values);
}

void putKV(const char *key, const char *value) {
	if (writeIndex < MAX_RECORD_COUNT) {
		strncpy(keys[writeIndex], key, MAX_KV_LEN);
		strncpy(values[writeIndex], value, MAX_KV_LEN);
		writeIndex++;
	}
}

char *getKV(const char *key) {
	for (int i = 0; i < MAX_RECORD_COUNT; ++i) {
		if (strcmp(keys[i], key) == 0) {
			return values[i];
		}
	}
	return NULL;
}


int splitString(const char* str, char* separator, char (*arr)[MAX_TOKEN_LEN], int n_arr)
{
    if (n_arr <= 0) {
    	return -1;
    }

    char* tmp = (char*)malloc(strlen(str) + 1);
    strcpy(tmp, str);
    char* context;
    int n = 0;

    char* token = strtok_r(tmp, separator, &context);
    if (!token) {
    	goto ret;
    }
    strncpy(arr[n], token, MAX_TOKEN_LEN);
    n++;

    while (1) {
    token = strtok_r(NULL, separator, &context);
    	if (!token || n >= n_arr) {
    	    goto ret;
    	}
    	strncpy(arr[n], token, MAX_TOKEN_LEN);
    	n++;
    }

ret:
    free(tmp);
    return n;
}

/*
uint64_t raftId(const char *host, uint32_t port) {
    uint32_t host_uint32 = (uint32_t)inet_addr(host);
    assert(host_uint32 != (uint32_t)-1);
    uint64_t code = ((uint64_t)host_uint32) << 32 | port;
	return code;
}
*/


/*
uint64_t encodeRaftId(const char *host, uint16_t port, uint16_t vid) {
  uint64_t raftId;
  uint32_t host_uint32 = (uint32_t)inet_addr(host);
  assert(host_uint32 != (uint32_t)-1);

  raftId = (((uint64_t)host_uint32) << 32) | (((uint32_t)port) << 16) | vid;
  return raftId;
}

void decodeRaftId(uint64_t raftId, char *host, int32_t len, uint16_t *port, uint16_t *vid) {
  uint32_t host32 = (uint32_t)((raftId >> 32) & 0x00000000FFFFFFFF);

  struct in_addr addr;
  addr.s_addr = host32;
  snprintf(host, len, "%s", inet_ntoa(addr));

  *port = (uint16_t)((raftId & 0x00000000FFFF0000) >> 16);
  *vid = (uint16_t)(raftId & 0x000000000000FFFF);
}
*/




int32_t raftServerInit(SRaftServer *pRaftServer, const SRaftServerConfig *pConf, struct raft_fsm *pFsm) {
	int ret;

    snprintf(pRaftServer->host, sizeof(pRaftServer->host), "%s", pConf->me.host);
	pRaftServer->port = pConf->me.port;
    snprintf(pRaftServer->address, sizeof(pRaftServer->address), "%s:%u", pRaftServer->host, pRaftServer->port);
	//strncpy(pRaftServer->dir, pConf->dataDir, sizeof(pRaftServer->dir));

	ret = uv_loop_init(&pRaftServer->loop);
	if (ret != 0) {
		fprintf(stderr, "uv_loop_init error: %s \n", uv_strerror(ret));
		assert(0);
	}

	ret = raft_uv_tcp_init(&pRaftServer->transport, &pRaftServer->loop);
	if (ret != 0) {
		fprintf(stderr, "raft_uv_tcp_init: error %d \n", ret);
		assert(0);
	}


	uint16_t vid;
	pRaftServer->instanceCount = 20;
	

	for (int i = 0; i < pRaftServer->instanceCount; ++i)
	{
		//vid = 0;
		vid = i;


		pRaftServer->instance[vid].raftId = encodeRaftId(pRaftServer->host, pRaftServer->port, vid);
		snprintf(pRaftServer->instance[vid].dir, sizeof(pRaftServer->instance[vid].dir), "%s_%llu", pConf->dataDir, pRaftServer->instance[vid].raftId);

	    char cmd_buf[COMMAND_LEN];
	    snprintf(cmd_buf, sizeof(cmd_buf), "mkdir -p %s", pRaftServer->instance[vid].dir);
	    system(cmd_buf);
		sleep(1);

		pRaftServer->instance[vid].fsm = pFsm;

		ret = raft_uv_init(&pRaftServer->instance[vid].io, &pRaftServer->loop, pRaftServer->instance[vid].dir, &pRaftServer->transport);
		if (ret != 0) {
			fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->instance[vid].raft));
			assert(0);
		}

		ret = raft_init(&pRaftServer->instance[vid].raft, &pRaftServer->instance[vid].io, pRaftServer->instance[vid].fsm, pRaftServer->instance[vid].raftId, pRaftServer->address);
		if (ret != 0) {
			fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->instance[vid].raft));
			assert(0);
		}

    	struct raft_configuration conf;
    	raft_configuration_init(&conf);
    	raft_configuration_add(&conf, pRaftServer->instance[vid].raftId, pRaftServer->address, RAFT_VOTER);
		printf("add myself: %llu - %s \n", pRaftServer->instance[vid].raftId, pRaftServer->address);
		for (int i = 0; i < pConf->peersCount; ++i) {
			const Addr *pAddr = &pConf->peers[i];

			raft_id rid = encodeRaftId(pAddr->host, pAddr->port, vid);

			char addrBuf[ADDRESS_LEN];
			snprintf(addrBuf, sizeof(addrBuf), "%s:%u", pAddr->host, pAddr->port);
			raft_configuration_add(&conf, rid, addrBuf, RAFT_VOTER);
			printf("add peers: %llu - %s \n", rid, addrBuf);
		}

    	raft_bootstrap(&pRaftServer->instance[vid].raft, &conf);

	}







	return 0;
}

int32_t raftServerStart(SRaftServer *pRaftServer) {
	int ret;

	for (int i = 0; i < pRaftServer->instanceCount; ++i) {
		ret = raft_start(&pRaftServer->instance[i].raft);
		if (ret != 0) {
			fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->instance[i].raft));
		}
		
	}


	uv_run(&pRaftServer->loop, UV_RUN_DEFAULT);
}


void raftServerClose(SRaftServer *pRaftServer) {

}


int fsmApplyCb(struct raft_fsm *pFsm, const struct raft_buffer *buf, void **result) {
    char *msg = (char*)buf->base;
    printf("fsm apply: %s \n", msg);

	char arr[2][MAX_TOKEN_LEN];
	splitString(msg, "--", arr, 2);
	putKV(arr[0], arr[1]);

    return 0;
}

int32_t initFsm(struct raft_fsm *fsm) {
	initStore();
	fsm->apply = fsmApplyCb;
	return 0;
}

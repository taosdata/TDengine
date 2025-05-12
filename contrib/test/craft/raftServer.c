#include <stdlib.h>
#include "tcommon.h"
#include "raftServer.h"

char *keys;
char *values;

void initStore() {
	keys = malloc(MAX_RECORD_COUNT * MAX_KV_LEN);
	values = malloc(MAX_RECORD_COUNT * MAX_KV_LEN);
	writeIndex = 0;
}

void destroyStore() {
	free(keys);
	free(values);
}

void putKV(const char *key, const char *value) {
	if (writeIndex < MAX_RECORD_COUNT) {
		strncpy(&keys[writeIndex], key, MAX_KV_LEN);
		strncpy(&values[writeIndex], value, MAX_KV_LEN);
		writeIndex++;
	}
}

char *getKV(const char *key) {
	for (int i = 0; i < MAX_RECORD_COUNT; ++i) {
		if (strcmp(&keys[i], key) == 0) {
			return &values[i];
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

uint64_t raftId(const char *host, uint32_t port) {
    uint32_t host_uint32 = (uint32_t)inet_addr(host);
    assert(host_uint32 != (uint32_t)-1);
    uint64_t code = ((uint64_t)host_uint32) << 32 | port;
	return code;
}

int32_t raftServerInit(SRaftServer *pRaftServer, const SRaftServerConfig *pConf, struct raft_fsm *pFsm) {
	int ret;

    snprintf(pRaftServer->host, sizeof(pRaftServer->host), "%s", pConf->me.host);
	pRaftServer->port = pConf->me.port;
    snprintf(pRaftServer->address, sizeof(pRaftServer->address), "%s:%u", pRaftServer->host, pRaftServer->port);
	strncpy(pRaftServer->dir, pConf->dataDir, sizeof(pRaftServer->dir));

	pRaftServer->raftId = raftId(pRaftServer->host, pRaftServer->port);
	pRaftServer->fsm = pFsm;

	ret = uv_loop_init(&pRaftServer->loop);
	if (ret != 0) {
		fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->raft));
		assert(0);
	}

	ret = raft_uv_tcp_init(&pRaftServer->transport, &pRaftServer->loop);
	if (ret != 0) {
		fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->raft));
		assert(0);
	}

	ret = raft_uv_init(&pRaftServer->io, &pRaftServer->loop, pRaftServer->dir, &pRaftServer->transport);
	if (ret != 0) {
		fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->raft));
		assert(0);
	}

	ret = raft_init(&pRaftServer->raft, &pRaftServer->io, pRaftServer->fsm, pRaftServer->raftId, pRaftServer->address);
	if (ret != 0) {
		fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->raft));
		assert(0);
	}

    struct raft_configuration conf;
    raft_configuration_init(&conf);

	if (pConf->voter == 0) {
    	raft_configuration_add(&conf, pRaftServer->raftId, pRaftServer->address, RAFT_SPARE);

	} else {
    	raft_configuration_add(&conf, pRaftServer->raftId, pRaftServer->address, RAFT_VOTER);

	}

	

	printf("add myself: %llu - %s \n", pRaftServer->raftId, pRaftServer->address);


	for (int i = 0; i < pConf->peersCount; ++i) {
		const Addr *pAddr = &pConf->peers[i];
		raft_id rid = raftId(pAddr->host, pAddr->port);
		char addrBuf[ADDRESS_LEN];
		snprintf(addrBuf, sizeof(addrBuf), "%s:%u", pAddr->host, pAddr->port);
		raft_configuration_add(&conf, rid, addrBuf, RAFT_VOTER);
		printf("add peers: %llu - %s \n", rid, addrBuf);
	}

    raft_bootstrap(&pRaftServer->raft, &conf);

	return 0;
}

int32_t raftServerStart(SRaftServer *pRaftServer) {
	int ret;
	ret = raft_start(&pRaftServer->raft);
	if (!ret) {
		fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->raft));
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

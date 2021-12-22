#include <stdlib.h>
#include "common.h"
#include "raftServer.h"

uint64_t raftId(const char *host, uint32_t port) {
    uint32_t host_uint32 = (uint32_t)inet_addr(host);
    assert(host_uint32 != (uint32_t)-1);
    uint64_t code = ((uint64_t)host_uint32) << 32 | port;
	return code;
}

int32_t raftServerInit(SRaftServer *pRaftServer, const char *host, uint32_t port, const char *dir, struct raft_fsm *pFsm) {
	int ret;

	char cmd_buf[COMMAND_LEN];
	snprintf(cmd_buf, sizeof(cmd_buf), "mkdir -p %s", dir);
	system(cmd_buf);

    snprintf(pRaftServer->host, sizeof(pRaftServer->host), "%s", host);
	pRaftServer->port = port;
    snprintf(pRaftServer->address, sizeof(pRaftServer->address), "%s:%u", host, port);
	strncpy(pRaftServer->dir, dir, sizeof(pRaftServer->dir));

	pRaftServer->raftId = raftId(pRaftServer->host, pRaftServer->port);
	pRaftServer->fsm = pFsm;

	ret = uv_loop_init(&pRaftServer->loop);
	if (!ret) {
		fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->raft));
	}

	ret = raft_uv_tcp_init(&pRaftServer->transport, &pRaftServer->loop);
	if (!ret) {
		fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->raft));
	}

	ret = raft_uv_init(&pRaftServer->io, &pRaftServer->loop, dir, &pRaftServer->transport);
	if (!ret) {
		fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->raft));
	}

	ret = raft_init(&pRaftServer->raft, &pRaftServer->io, pRaftServer->fsm, pRaftServer->raftId, pRaftServer->address);
	if (!ret) {
		fprintf(stderr, "%s \n", raft_errmsg(&pRaftServer->raft));
	}

    struct raft_configuration conf;
    raft_configuration_init(&conf);
    raft_configuration_add(&conf, pRaftServer->raftId, pRaftServer->address, RAFT_VOTER);
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
    printf("%s \n", msg);

    return 0;
}

int32_t initFsm(struct raft_fsm *fsm) {
	fsm->apply = fsmApplyCb;
	return 0;
}

#ifndef TDENGINE_RAFT_SERVER_H
#define TDENGINE_RAFT_SERVER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <string.h>
#include "raft.h"
#include "raft/uv.h"
#include "common.h"


// simulate a db store, just for test
#define MAX_KV_LEN 20
#define MAX_RECORD_COUNT 16


//char *keys;
//char *values;
//int writeIndex;

void initStore();
void destroyStore();
void putKV(const char *key, const char *value);
char *getKV(const char *key);

typedef struct {
	char dir[DIR_LEN + HOST_LEN * 4];  /* Data dir of UV I/O backend */
	raft_id raftId;                    /* For vote */
	struct raft_fsm *fsm;              /* Sample application FSM */
	struct raft raft;                   /* Raft instance */
	struct raft_io io;                  /* UV I/O backend */

} SInstance;

typedef struct {
	char host[HOST_LEN];
    uint32_t port;
	char address[ADDRESS_LEN];         /* Raft instance address */

	struct uv_loop_s loop;              /* UV loop */
	struct raft_uv_transport transport; /* UV I/O backend transport */

	SInstance instance[MAX_INSTANCE_NUM];
	int32_t instanceCount;

} SRaftServer;

#define MAX_TOKEN_LEN 32
int splitString(const char* str, char* separator, char (*arr)[MAX_TOKEN_LEN], int n_arr);

int32_t raftServerInit(SRaftServer *pRaftServer, const SRaftServerConfig *pConf, struct raft_fsm *pFsm);
int32_t raftServerStart(SRaftServer *pRaftServer);
void raftServerClose(SRaftServer *pRaftServer);


int initFsm(struct raft_fsm *fsm);




#ifdef __cplusplus
}
#endif

#endif // TDENGINE_RAFT_SERVER_H

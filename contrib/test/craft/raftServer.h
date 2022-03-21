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
#include "tcommon.h"


// simulate a db store, just for test
#define MAX_KV_LEN 100
#define MAX_RECORD_COUNT 500
char *keys;
char *values;
int writeIndex;

void initStore();
void destroyStore();
void putKV(const char *key, const char *value);
char *getKV(const char *key);

typedef struct {
	char dir[DIR_LEN + HOST_LEN * 2];  /* Data dir of UV I/O backend */
	char host[HOST_LEN];
    uint32_t port;
	char address[ADDRESS_LEN];         /* Raft instance address */
	raft_id raftId;                    /* For vote */
	struct raft_fsm *fsm;              /* Sample application FSM */

	struct raft raft;                   /* Raft instance */
	struct raft_io io;                  /* UV I/O backend */
	struct uv_loop_s loop;              /* UV loop */
	struct raft_uv_transport transport; /* UV I/O backend transport */
} SRaftServer;

#define MAX_TOKEN_LEN 32
int splitString(const char* str, char* separator, char (*arr)[MAX_TOKEN_LEN], int n_arr);

uint64_t raftId(const char *host, uint32_t port);
int32_t raftServerInit(SRaftServer *pRaftServer, const SRaftServerConfig *pConf, struct raft_fsm *pFsm);
int32_t raftServerStart(SRaftServer *pRaftServer);
void raftServerClose(SRaftServer *pRaftServer);

int initFsm(struct raft_fsm *fsm);

const char* state2String(unsigned short state);
void printRaftConfiguration(struct raft_configuration *c);
void printRaftState(struct raft *r);


#ifdef __cplusplus
}
#endif

#endif // TDENGINE_RAFT_SERVER_H

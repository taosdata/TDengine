#include <stdio.h>
#include <raft.h>
#include <raft/uv.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <getopt.h>

const char* exe_name;

// simulate ------------------------
typedef struct SVnode {
    int vid;
} SVnode;


#define VNODE_COUNT 10
SVnode vnodes[VNODE_COUNT];

int vnodeApplyWMsg(SVnode *pVnode, char *pMsg, void **pRsp) {
	printf("put value to tsdb, vid:%d msg:%s \n", pVnode->vid, pMsg);
	return 0;
}

int applyCB(struct raft_fsm *fsm,
            const struct raft_buffer *buf,
            void **result) {
	char *msg = (char*)buf->base;
	//printf("%s \n", msg);

	// parse msg
	char* context;
    char* token = strtok_r(msg, ":", &context);
	int vid = atoi(token);

	token = strtok_r(NULL, ":", &context);
	char *value = token;

	SVnode* tmp_vnodes = (SVnode*)(fsm->data);
	vnodeApplyWMsg(&tmp_vnodes[vid], value, NULL);

    return 0;
}

// Config ------------------------
#define HOST_LEN 32
#define MAX_PEERS 10
typedef struct Address {
	char host[HOST_LEN];
	uint32_t port;
} Address;

uint64_t raftId(Address *addr) {
	// test in a single machine, port is unique
	// if in multi machines, use host and port
	return addr->port;
}

typedef struct Config {
	Address me;
	Address peers[MAX_PEERS];
    int peer_count;
} Config;

Config gConf;

void printConf(Config *c) {
	printf("me: %s:%u \n", c->me.host, c->me.port);
	for (int i = 0; i < c->peer_count; ++i) {
		printf("peer%d: %s:%u \n", i, c->peers[i].host, c->peers[i].port);
	}
}

// RaftServer ------------------------
typedef struct RaftServer {
	struct uv_loop_s loop;
	struct raft_uv_transport transport;
	struct raft_io io;
	struct raft_fsm fsm;
	struct raft raft;
	struct raft_configuration conf;
} RaftServer;

RaftServer gRaftServer;

static void* startRaftServer(void *param) {
	//RaftServer* rs = (RaftServer*)param;
	RaftServer* rs = &gRaftServer;
    raft_start(&rs->raft);
    uv_run(&rs->loop, UV_RUN_DEFAULT);
}

static const char* state2String(unsigned short state) {
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

static void printRaftState(struct raft *r) {
	printf("\n");
	printf("my_id: %llu \n", r->id);
	printf("address: %s \n", r->address);
	printf("current_term: %llu \n", r->current_term);
	printf("voted_for: %llu \n", r->voted_for);
	printf("role: %s \n", state2String(r->state));
	printf("commit_index: %llu \n", r->commit_index);
	printf("last_applied: %llu \n", r->last_applied);
	printf("last_stored: %llu \n", r->last_stored);
	printf("\n");
}

// console -----------------------------------------
#define PROPOSE_VALUE_LEN 128
static void proposeValue(struct raft *r) {
    struct raft_buffer buf;

    // need free
    buf.len = PROPOSE_VALUE_LEN;
    buf.base = raft_malloc(buf.len);

    // mock ts value
    int vid = taosRand() % VNODE_COUNT;
    snprintf(buf.base, buf.len, "%d:value_%ld", vid, time(NULL));

	printf("propose value: %s \n", (char*)buf.base);

    // need free
    struct raft_apply *req = raft_malloc(sizeof(struct raft_apply));
    raft_apply(r, req, &buf, 1, NULL);
}

static void* console(void *param) {
    while (1) {
        // notice! memory buffer overflow!
        char buf[128];
		memset(buf, 0, sizeof(buf));
		fgets(buf, 128, stdin);
		if (strlen(buf) == 1) {
			continue;
		}	
		buf[strlen(buf)-1] = '\0';

        // do not use strcmp
        if (strcmp(buf, "state") == 0) {
            printRaftState(&gRaftServer.raft);

        } else if (strcmp(buf, "put") == 0) {
            proposeValue(&gRaftServer.raft);

        } else {
            printf("unknown command: [%s], support command: state, put \n", buf);
        }
    }
}

// -----------------------------------------
void usage() {
	printf("\n");
	printf("%s my_port peer1_port peer2_port ... \n", exe_name);
	printf("\n");
}

int main(int argc, char **argv) {
	taosSeedRand(time(NULL));

	exe_name = argv[0];
	if (argc < 2) {
        usage();
        exit(-1);
    }

	// read conf from argv
	strncpy(gConf.me.host, "127.0.0.1", HOST_LEN);
	sscanf(argv[1], "%u", &gConf.me.port);

	gConf.peer_count = 0;
	for (int i = 2; i < argc; ++i) {
		strncpy(gConf.peers[gConf.peer_count].host, "127.0.0.1", HOST_LEN);
		sscanf(argv[i], "%u", &gConf.peers[gConf.peer_count].port);
		gConf.peer_count++;
	}
	printConf(&gConf);

	// mkdir
	char dir[128];
	snprintf(dir, sizeof(dir), "./%s_%u", gConf.me.host, gConf.me.port);

	char cmd[128];
	snprintf(cmd, sizeof(cmd), "rm -rf ./%s", dir);
	system(cmd);
	snprintf(cmd, sizeof(cmd), "mkdir -p ./%s", dir);
	system(cmd);

    // init io
    uv_loop_init(&gRaftServer.loop);
    raft_uv_tcp_init(&gRaftServer.transport, &gRaftServer.loop);
    raft_uv_init(&gRaftServer.io, &gRaftServer.loop, dir, &gRaftServer.transport);

	// init fsm
	gRaftServer.fsm.apply = applyCB;
	gRaftServer.fsm.data = vnodes;
	for (int i = 0; i < VNODE_COUNT; ++i) {
		vnodes[i].vid = i;
	}

    // init raft instance with io and fsm
	char address_buf[128];
	snprintf(address_buf, sizeof(address_buf), "%s:%u", gConf.me.host, gConf.me.port);
	
	// test in a single machine, port is unique
	uint64_t raft_id = raftId(&gConf.me);
    raft_init(&gRaftServer.raft, &gRaftServer.io, &gRaftServer.fsm, raft_id, address_buf);
    //raft_init(&gRaftServer.raft, &gRaftServer.io, &gRaftServer.fsm, 11, "127.0.0.1:9000");

    // init cluster configuration
	struct raft_configuration conf;
    raft_configuration_init(&conf);
    raft_configuration_add(&conf, raftId(&gConf.me), address_buf, RAFT_VOTER);
	for (int i = 0; i < gConf.peer_count; ++i) {
		char address_buf[128];
		snprintf(address_buf, sizeof(address_buf), "%s:%u", gConf.peers[i].host, gConf.peers[i].port);
		raft_configuration_add(&conf, raftId(&gConf.peers[i]), address_buf, RAFT_VOTER);
	}
    raft_bootstrap(&gRaftServer.raft, &conf);

    // start raft server and loop
    pthread_t tid;
    pthread_create(&tid, NULL, startRaftServer, &gRaftServer);

    // simulate console
	pthread_t tid2;
	pthread_create(&tid2, NULL, console, NULL);

	while (1) {
		sleep(10);
	}

	return 0;
}

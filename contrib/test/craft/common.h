#ifndef TDENGINE_COMMON_H
#define TDENGINE_COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#define MAX_PEERS 10
#define COMMAND_LEN 512
#define TOKEN_LEN 128
#define DIR_LEN 256
#define HOST_LEN 64
#define ADDRESS_LEN (HOST_LEN + 16)

typedef struct {
    char host[HOST_LEN];
    uint32_t port;
} Addr;

typedef struct {
	int voter;
    Addr me;
    Addr peers[MAX_PEERS];
    int peersCount;
    char dir[DIR_LEN];
    char dataDir[DIR_LEN + HOST_LEN * 2];
} SRaftServerConfig;

#ifdef __cplusplus
}
#endif

#endif // TDENGINE_COMMON_H

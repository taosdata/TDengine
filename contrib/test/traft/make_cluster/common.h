#ifndef TRAFT_COMMON_H
#define TRAFT_COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#define COMMAND_LEN 512
#define MAX_CMD_COUNT 10
#define TOKEN_LEN 128
#define MAX_PEERS_COUNT 19

#define HOST_LEN 64
#define ADDRESS_LEN (HOST_LEN * 2)
#define BASE_DIR_LEN 128

#ifdef __cplusplus
}
#endif

#endif

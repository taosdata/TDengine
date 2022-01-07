#ifndef TRAFT_CONSOLE_H
#define TRAFT_CONSOLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <getopt.h>
#include <stdint.h>
#include "common.h"
#include "raftServer.h"

void console(RaftServer *pRaftServer);

#ifdef __cplusplus
}
#endif

#endif

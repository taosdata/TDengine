#ifndef TRAFT_CONFIG_H
#define TRAFT_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <getopt.h>
#include <stdint.h>
#include "common.h"

typedef struct {
  char     host[HOST_LEN];
  uint16_t port;
} Addr;

typedef struct {
  Addr me;
  char baseDir[BASE_DIR_LEN];
} RaftServerConfig;

void addrToString(const char *host, uint16_t port, char *addr, int len);
void parseAddr(const char *addr, char *host, int len, uint16_t *port);
int  parseConf(int argc, char **argv, RaftServerConfig *pConf);
void printConf(RaftServerConfig *pConf);

#ifdef __cplusplus
}
#endif

#endif

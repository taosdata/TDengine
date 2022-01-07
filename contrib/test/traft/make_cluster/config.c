#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void addrToString(const char *host, uint16_t port, char *addr, int len) { snprintf(addr, len, "%s:%hu", host, port); }

void parseAddr(const char *addr, char *host, int len, uint16_t *port) {
  char *tmp = (char *)malloc(strlen(addr) + 1);
  strcpy(tmp, addr);

  char *context;
  char *separator = ":";
  char *token = strtok_r(tmp, separator, &context);
  if (token) {
    snprintf(host, len, "%s", token);
  }

  token = strtok_r(NULL, separator, &context);
  if (token) {
    sscanf(token, "%hu", port);
  }

  free(tmp);
}

int parseConf(int argc, char **argv, RaftServerConfig *pConf) {
  memset(pConf, 0, sizeof(*pConf));

  int option_index, option_value;
  option_index = 0;
  static struct option long_options[] = {{"help", no_argument, NULL, 'h'},
                                         {"addr", required_argument, NULL, 'a'},
                                         {"dir", required_argument, NULL, 'd'},
                                         {NULL, 0, NULL, 0}};

  while ((option_value = getopt_long(argc, argv, "ha:d:", long_options, &option_index)) != -1) {
    switch (option_value) {
      case 'a': {
        parseAddr(optarg, pConf->me.host, sizeof(pConf->me.host), &pConf->me.port);
        break;
      }

      case 'd': {
        snprintf(pConf->baseDir, sizeof(pConf->baseDir), "%s", optarg);
        break;
      }

      case 'h': {
        return -2;
      }

      default: { return -2; }
    }
  }

  return 0;
}

void printConf(RaftServerConfig *pConf) {
  printf("\n---printConf: \n");
  printf("me: [%s:%hu] \n", pConf->me.host, pConf->me.port);
  printf("dataDir: [%s] \n\n", pConf->baseDir);
}

#include "console.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "raftServer.h"
#include "util.h"

void printHelp() {
  printf("---------------------\n");
  printf("help: \n\n");
  printf("create a vgroup with 3 replicas: \n");
  printf("create vnode voter vid 100 peers 127.0.0.1:10001 127.0.0.1:10002 \n");
  printf("create vnode voter vid 100 peers 127.0.0.1:10000 127.0.0.1:10002 \n");
  printf("create vnode voter vid 100 peers 127.0.0.1:10000 127.0.0.1:10001 \n");
  printf("\n");
  printf("create a vgroup with only one replica: \n");
  printf("create vnode voter vid 200 \n");
  printf("\n");
  printf("add vnode into vgroup: \n");
  printf("create vnode spare vid 100   ---- run at 127.0.0.1:10003\n");
  printf("join vnode vid 100 addr 127.0.0.1:10003   ---- run at leader of vgroup 100\n");
  printf("\n");
  printf("run \n");
  printf("put 0 key value \n");
  printf("get 0 key \n");
  printf("---------------------\n");
}

void console(RaftServer *pRaftServer) {
  while (1) {
    int  ret;
    char cmdBuf[COMMAND_LEN];
    memset(cmdBuf, 0, sizeof(cmdBuf));
    printf("(console)> ");
    char *retp = fgets(cmdBuf, COMMAND_LEN, stdin);
    if (!retp) {
      exit(-1);
    }

    int pos = strlen(cmdBuf);
    if (cmdBuf[pos - 1] == '\n') {
      cmdBuf[pos - 1] = '\0';
    }

    if (strncmp(cmdBuf, "", COMMAND_LEN) == 0) {
      continue;
    }

    char cmds[MAX_CMD_COUNT][TOKEN_LEN];
    memset(cmds, 0, sizeof(cmds));

    int cmdCount;
    cmdCount = splitString(cmdBuf, " ", cmds, MAX_CMD_COUNT);

    if (strcmp(cmds[0], "create") == 0 && strcmp(cmds[1], "vnode") == 0 && strcmp(cmds[3], "vid") == 0) {
      uint16_t vid;
      sscanf(cmds[4], "%hu", &vid);

      if (strcmp(cmds[2], "voter") == 0) {
        char peers[MAX_PEERS_COUNT][ADDRESS_LEN];
        memset(peers, 0, sizeof(peers));
        uint32_t peersCount = 0;

        if (strcmp(cmds[5], "peers") == 0 && cmdCount > 6) {
          // create vnode voter vid 100 peers 127.0.0.1:10001 127.0.0.1:10002
          for (int i = 6; i < cmdCount; ++i) {
            snprintf(peers[i - 6], ADDRESS_LEN, "%s", cmds[i]);
            peersCount++;
          }
        } else {
          // create vnode voter vid 200
        }
        ret = addRaftVoter(pRaftServer, peers, peersCount, vid);
        if (ret == 0) {
          printf("create vnode voter ok \n");
        } else {
          printf("create vnode voter error \n");
        }
      } else if (strcmp(cmds[2], "spare") == 0) {
        ret = addRaftSpare(pRaftServer, vid);
        if (ret == 0) {
          printf("create vnode spare ok \n");
        } else {
          printf("create vnode spare error \n");
        }
      } else {
        printHelp();
      }

    } else if (strcmp(cmds[0], "join") == 0 && strcmp(cmds[1], "vnode") == 0 && strcmp(cmds[2], "vid") == 0 &&
               strcmp(cmds[4], "addr") == 0 && cmdCount == 6) {
      // join vnode vid 100 addr 127.0.0.1:10004

      char *   address = cmds[5];
      char     host[64];
      uint16_t port;
      parseAddr(address, host, sizeof(host), &port);

      uint16_t vid;
      sscanf(cmds[3], "%hu", &vid);

      HashNode **pp = pRaftServer->raftInstances.find(&pRaftServer->raftInstances, vid);
      if (*pp == NULL) {
        printf("vid:%hu not found \n", vid);
        break;
      }
      RaftInstance *pRaftInstance = (*pp)->data;

      uint64_t destRaftId = encodeRaftId(host, port, vid);

      struct raft_change *req = raft_malloc(sizeof(*req));
      RaftJoin *          pRaftJoin = raft_malloc(sizeof(*pRaftJoin));
      pRaftJoin->r = &pRaftInstance->raft;
      pRaftJoin->joinId = destRaftId;
      req->data = pRaftJoin;
      ret = raft_add(&pRaftInstance->raft, req, destRaftId, address, raftChangeAddCb);
      if (ret != 0) {
        printf("raft_add error: %s \n", raft_errmsg(&pRaftInstance->raft));
      }

    } else if (strcmp(cmds[0], "dropnode") == 0) {
    } else if (strcmp(cmds[0], "state") == 0) {
      pRaftServer->raftInstances.print(&pRaftServer->raftInstances);
      for (size_t i = 0; i < pRaftServer->raftInstances.length; ++i) {
        HashNode *ptr = pRaftServer->raftInstances.table[i];
        if (ptr != NULL) {
          while (ptr != NULL) {
            RaftInstance *pRaftInstance = ptr->data;
            printf("instance vid:%hu raftId:%llu \n", ptr->vgroupId, pRaftInstance->raftId);
            printRaftState(&pRaftInstance->raft);
            printf("\n");
            ptr = ptr->next;
          }
          printf("\n");
        }
      }

    } else if (strcmp(cmds[0], "put") == 0 && cmdCount == 4) {
      uint16_t vid;
      sscanf(cmds[1], "%hu", &vid);
      char *     key = cmds[2];
      char *     value = cmds[3];
      HashNode **pp = pRaftServer->raftInstances.find(&pRaftServer->raftInstances, vid);
      if (*pp == NULL) {
        printf("vid:%hu not found \n", vid);
        break;
      }
      RaftInstance *pRaftInstance = (*pp)->data;

      char *raftValue = malloc(TOKEN_LEN * 2 + 3);
      snprintf(raftValue, TOKEN_LEN * 2 + 3, "%s--%s", key, value);
      putValue(&pRaftInstance->raft, raftValue);
      free(raftValue);

    } else if (strcmp(cmds[0], "run") == 0) {
      pthread_t tidRaftServer;
      pthread_create(&tidRaftServer, NULL, startServerFunc, pRaftServer);

    } else if (strcmp(cmds[0], "get") == 0 && cmdCount == 3) {
      uint16_t vid;
      sscanf(cmds[1], "%hu", &vid);
      char *     key = cmds[2];
      HashNode **pp = pRaftServer->raftInstances.find(&pRaftServer->raftInstances, vid);
      if (*pp == NULL) {
        printf("vid:%hu not found \n", vid);
        break;
      }
      RaftInstance *   pRaftInstance = (*pp)->data;
      SimpleHash *     pKV = pRaftInstance->fsm.data;
      SimpleHashNode **ppNode = pKV->find_cstr(pKV, key);
      if (*ppNode == NULL) {
        printf("key:%s not found \n", key);
      } else {
        printf("find key:%s value:%s \n", key, (char *)((*ppNode)->data));
      }

    } else if (strcmp(cmds[0], "transfer") == 0) {
    } else if (strcmp(cmds[0], "state") == 0) {
    } else if (strcmp(cmds[0], "snapshot") == 0) {
    } else if (strcmp(cmds[0], "exit") == 0) {
      exit(0);

    } else if (strcmp(cmds[0], "quit") == 0) {
      exit(0);

    } else if (strcmp(cmds[0], "help") == 0) {
      printHelp();

    } else {
      printf("unknown command: %s \n", cmdBuf);
      printHelp();
    }

    /*
        printf("cmdBuf: [%s] \n", cmdBuf);
        printf("cmdCount : %d \n", cmdCount);
        for (int i = 0; i < MAX_CMD_COUNT; ++i) {
          printf("cmd%d : %s \n", i, cmds[i]);
        }
    */
  }
}

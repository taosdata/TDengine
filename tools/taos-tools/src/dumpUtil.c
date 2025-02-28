/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */


#include <taos.h>
#include "dump.h"
#include "dumpUtil.h"


// malloc new node
SNode *mallocNode(const char *name, int32_t len) {
    // check valid
    if(len >= TSDB_TABLE_NAME_LEN) {
        errorPrint("mallocNode len=%d is over TSDB_TABLE_NAME_LEN=%d \n", len, TSDB_TABLE_NAME_LEN);
        return NULL;
    }

    // malloc
    SNode *node = (SNode *)malloc(sizeof(SNode));
    if (node == NULL) {
        errorPrint("mallocNode memory malloc failed. malloc size=%ld\n", sizeof(SNode));
        return NULL;
    }

    // set
    node->next = NULL;
    memcpy(node->name, name, len);
    node->name[len] = 0;

    // return
    return node;
}

// free nodes
void freeNodes(SNode* head) {
    // check
    if (head == NULL) {
        return ;
    }

    // free
    SNode *next = head;
    while(next) {
        SNode *old = next;
        next = next->next; 
        free(old);
    }
}

// return true to do retry , false no retry , code is error code 
bool canRetry(int32_t code, int8_t type) {
    // rpc error
    if (code >= TSDB_CODE_RPC_BEGIN && code <= TSDB_CODE_RPC_END) {
        return true;
    }

    // single code
    int32_t codes[] = {0x0000ffff};
    for(int32_t i = 0; i< sizeof(codes)/sizeof(int32_t); i++) {
        if (code == codes[i]) {
            return true;
        }
    }

    return false;
}


//
//  ---------------  native  ------------------
//

// connect
TAOS *taosConnect(const char *dbName) {
    int32_t i = 0;
    while (1) {
        TAOS *taos = taos_connect(g_args.host, g_args.user, g_args.password, dbName, g_args.port);
        if (taos) {
            // successful
            if (i > 0) {
                okPrint("Retry %d to connect %s:%d successfully!\n", i, g_args.host, g_args.port);
            }
            return taos;
        }

        // fail
        errorPrint("Failed to connect to server %s, code: 0x%08x, reason: %s! \n", g_args.host, taos_errno(NULL),
                   taos_errstr(NULL));

        if (++i > g_args.retryCount) {
            break;
        }

        // retry agian
        infoPrint("Retry to connect for %d after sleep %dms ...\n", i, g_args.retrySleepMs);
        toolsMsleep(g_args.retrySleepMs);
    }
    return NULL;
}

// query
TAOS_RES *taosQuery(TAOS *taos, const char *sql, int32_t *code) {
    int32_t   i = 0;
    TAOS_RES *res = NULL;
    while (1) {
        res = taos_query(taos, sql);
        *code = taos_errno(res);
        if (*code == 0) {
            // successful
            if (i > 0) {
                okPrint("Retry %d to execute taosQuery %s successfully!\n", i, sql);
            }
            return res;
        }

        // fail
        errorPrint("Failed to execute taosQuery, code: 0x%08x, reason: %s, sql=%s \n", *code, taos_errstr(res), sql);

        // can retry
        if(!canRetry(*code, RETRY_TYPE_QUERY)) {
            infoPrint("%s", "error code not in retry range , give up retry.\n");
            return NULL;
        }

        // check retry count
        if (++i > g_args.retryCount) {
            break;
        }

        // retry agian
        infoPrint("Retry to execute taosQuery for %d after sleep %dms ...\n", i, g_args.retrySleepMs);
        toolsMsleep(g_args.retrySleepMs);
    }

    return NULL;
}

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
#include "pub.h"
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
    //
    // collect params
    //
    char     show[256] = "\0";
    char *   host = NULL;
    uint16_t port = 0;
    char *   user = NULL;
    char *   pwd  = NULL;
    int32_t  code = 0;
    char *   dsnc = NULL;

    // set mode
    if (g_args.dsn) {
        dsnc = strToLowerCopy(g_args.dsn);
        if (dsnc == NULL) {
            return NULL;
        }

        char *cport = NULL;
        char error[512] = "";
        code = parseDsn(dsnc, &host, &cport, &user, &pwd, error);
        if (code) {
            errorPrint("%s dsn=%s\n", error, dsnc);
            free(dsnc);
            return NULL;
        }

        // default ws port
        if (cport == NULL) {
            if (user)
                port = DEFAULT_PORT_WS_CLOUD;
            else
                port = DEFAULT_PORT_WS_LOCAL;
        } else {
            port = atoi(cport);
        }

        // websocket
        memcpy(show, g_args.dsn, 20);
        memcpy(show + 20, "...", 3);
        memcpy(show + 23, g_args.dsn + strlen(g_args.dsn) - 10, 10);

    } else {

        host = g_args.host;
        user = g_args.user;
        pwd  = g_args.password;

        if (g_args.port_inputted) {
            port = g_args.port;
        } else {
            port = g_args.connMode == CONN_MODE_NATIVE ? DEFAULT_PORT_NATIVE : DEFAULT_PORT_WS_LOCAL;
        }

        sprintf(show, "host:%s port:%d ", host, port);
    }    
    
    //
    // connect
    //
    int32_t i = 0;
    TAOS *taos = NULL;
    while (1) {
        taos = taos_connect(host, user, pwd, dbName, port);
        if (taos) {
            // successful
            if (i > 0) {
                okPrint("Retry %d to connect %s:%d successfully!\n", i, host, port);
            }
            break;
        }

        // fail
        errorPrint("Failed to connect to server %s, code: 0x%08x, reason: %s! \n", host, taos_errno(NULL),
                   taos_errstr(NULL));
        if (++i > g_args.retryCount) {
            break;
        }

        // retry agian
        infoPrint("Retry to connect for %d after sleep %dms ...\n", i, g_args.retrySleepMs);
        toolsMsleep(g_args.retrySleepMs);
    }

    if (dsnc) {
        free(dsnc);
    }
    return taos;
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

void engineError(char * module, char * fun, int32_t code) {
    errorPrint("%s %s fun=%s error code:0x%08X \n", TIP_ENGINE_ERR, module, fun, code);
}


//
//  ---------------  DB's table schema change  ------------------
//



//
// -----------------------  hash32 map  --------------------------
//

// BKDR hash algorithm
uint32_t bkdrHash(const char *str) {
    uint32_t seed = 131;
    uint32_t hash = 0;
    while (*str) {
        hash = hash * seed + (*str++);
    }
    return hash;
}

// Initialize the hash table
void hashMapInit(HashMap *map) {
    memset(map->buckets, 0, sizeof(map->buckets));
    pthread_mutex_init(&map->lock, NULL);
}

// Insert a key-value pair
bool hashMapInsert(HashMap *map, const char *key, void *value) {
    pthread_mutex_lock(&map->lock);
    uint32_t hash = bkdr_hash(key) % HASH32_MAP_MAX_BUCKETS;
    HashMapEntry *entry = (HashMapEntry *)malloc(sizeof(HashMapEntry));
    if (entry == NULL) {
        pthread_mutex_unlock(&map->lock);
        return false;
    }
    entry->key = strdup(key);
    entry->value = value;
    entry->next = map->buckets[hash];
    map->buckets[hash] = entry;
    pthread_mutex_unlock(&map->lock);
    return true;
}

// Find the value based on the key
void *hashMapFind(HashMap *map, const char *key) {
    uint32_t hash = bkdr_hash(key) % HASH32_MAP_MAX_BUCKETS;
    HashMapEntry *entry = map->buckets[hash];
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            return entry->value;
        }
        entry = entry->next;
    }
    return NULL;
}

// Destroy the hash table
void hashMapDestroy(HashMap *map) {
    for (int i = 0; i < HASH32_MAP_MAX_BUCKETS; i++) {
        HashMapEntry *entry = map->buckets[i];
        while (entry != NULL) {
            HashMapEntry *next = entry->next;
            free(entry->key);
            free(entry);
            entry = next;
        }
    }
    pthread_mutex_destroy(&map->lock);
} 


//
// -----------------  dbChagne -------------------------
//

// create 
DBChange createDbChange(const char *dbPath) {
    //TOTO
    DBChange * pDbChange = (DBChange *)calloc(1, sizeof(DBChange));
    pDbChange->dbPath = dbPath;

    return pDbChange;
}

// free
void freeDBChange(DBChange *pDbChange) {
    // TODO

    // free stbChange

    
    // free 
    hashMapDestroy(&pDbChange->stbMap);
}


// generate part string
char * genPartStr(ColDes *colDes, int from , int num) {
    // TODO
    return NULL;
}


// return true: schema no changed , false: changed
bool schemaNoChanged(RecordSchema *recordSchema, TableDes *tableDes) {
    //TODO
    return true;
}

// local schema recordSchema cross with server schema tableDes
int32_t localCrossServer(DBChange *pDbChange, StbChange *pStbChange, RecordSchema *recordSchema, TableDes *tableDes) {

    // record old
    int oldc = tableDes->columns;
    int oldt = tableDes->tags;

    int newc = 0; // col num
    int newt = 0; // tag num

    // check schema no change
    if (schemaNoChanged(recordSchema, tableDes)) {
        infoPrint("stb:%s schema no changed. server col:%d tag:%d\n", recordSchema->stbName, oldc, oldt);
        pStbChange->tableDes      = tableDes;
        pStbChange->schemaChanged = false;
        return 0;
    }

    // loop all
    for (int i = 0; i < oldc + oldt; i++) {
        ColDes * colDes = tableDes->cols + i;
        if (i < oldc) {
            // col
            if (findFieldInLocal(recordSchema->cols, recordSchema->num_cols, colDes->field)) {
                moveColDes(tableDes->cols, i, newc);
                ++newc;
            }
        } else {
            // tag
            if (findFieldInLocal(recordSchema->fields, recordSchema->num_fields, colDes->field)) {
                moveColDes(tableDes->cols, i,  newc + newt);
                ++newt;
            }
        }
    }

    // check valid
    if (newc == 0) {
        // col must not zero
        errorPrint("%s() LN%d, new column zero failed! oldc=%d\n", __func__, __LINE__, oldc);
        return -1;
    }
    if (newt == 0) {
        // tag must not zero
        errorPrint("%s() LN%d, new tag zero failed! oldt=%d\n", __func__, __LINE__, oldt);
        return -1;
    }

    // set new
    tableDes->columns = newc;
    tableDes->tags    = newt;

    
    // save tableDes to StbChange
    pStbChange->tableDes = tableDes;
    
    // gen part str
    pStbChange->strCols = genPartStr(tableDes->cols, 0, newc);
    pStbChange->strTags = genPartStr(tableDes->cols, newc, newt);

    pStbChange->schemaChanged = true;
    // show change log
    infoPrint("stb:%s have schema changed. server col:%d tag:%d local col:%d tag:%d\n", 
                recordSchema->stbName, oldc, oldt, newc, newt);

    return 0;
}


// add stb recordSchema to dbChange
int32_t AddStbChanged(DBChange *pDbChange, TAOS *taos, RecordSchema *recordSchema, StbChange **ppStbChange) {
    // TODO

    char *stbName = recordSchema->stbName;
    if (stbName == NULL || stbName[0] == 0) {
        errorPrint("%s() LN:%d, stbName null or empty.", __func__, __LINE__);
        return -1;
    }

    //
    // server
    //
    TableDes *tableDes = (TableDes *)calloc(1, sizeof(TableDes) + sizeof(ColDes) * TSDB_MAX_COLUMNS);
    if (NULL == tableDes) {
        errorPrint("%s() LN%d, mallocDes calloc failed!\n", __func__, __LINE__);
        return -1;
    }

    if (getTableDes(taos, pDbChange->dbName, stbName, tableDes, true) < 0) {
        errorPrint("%s() LN%d getTableDes failed, db:%s stb:%s !\n", __func__, __LINE__, pDbChange->dbName, stbName);
        return -1;
    }

    // init
    StbChange *pStbChange = (StbChange *)calloc(1, sizeof(StbChange));

    //
    // compare local & server and auto calc 
    //
    if (localCrossServer(pDbChange, pStbChange, recordSchema, tableDes)) {
        errorPrint("%s() LN%d localCrossServer failed, db:%s stb:%s !\n", __func__, __LINE__, pDbChange->dbName, stbName);
        free(pStbChange);
        freeTbDes(tableDes, true);
        return -1;
    }
    pStbChange->schemaChanged = true; 
    
    // set out
    if (ppStbChange) {
        *ppStbChange = pStbChange;
    }

    // add to DbChange hashMap
    if (!hashMapInsert(pDbChange->stbMap, pStbChange)) {
        errorPrint("%s() LN%d add hashMap failed, db:%s stb:%s !\n", __func__, __LINE__, pDbChange->dbName, stbName);
        free(pStbChange);
        freeTbDes(tableDes, true);
        return -1;
    }

    // free nothing

    return 0;
}


// find stbChange with stbName
StbChange * findStbChange(DBChange *pDbChange, char *stbName) {
    // TODO
    return NULL;
}
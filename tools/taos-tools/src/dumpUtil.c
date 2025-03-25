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

//
// ------------- util fun ----------------
//

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

// free tbDes
void freeTbDes(TableDes *tableDes, bool self) {
    if (NULL == tableDes) return;

    for (int i = 0; i < (tableDes->columns+tableDes->tags); i++) {
        if (tableDes->cols[i].var_value) {
            free(tableDes->cols[i].var_value);
        }
    }

    if(self) {
        free(tableDes);
    }
}

//
//  ---------------  db interface  ------------------
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
            port = defaultPort(g_args.connMode, g_args.dsn);
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
    // lock map
    pthread_mutex_lock(&map->lock);
    uint32_t hash = bkdrHash(key) % HASH32_MAP_MAX_BUCKETS;
    HashMapEntry *entry = (HashMapEntry *)malloc(sizeof(HashMapEntry));
    if (entry == NULL) {
        pthread_mutex_unlock(&map->lock);
        return false;
    }
    
    // set
    entry->key         = strdup(key);
    entry->value       = value; // StbChange
    entry->next        = map->buckets[hash];
    map->buckets[hash] = entry;
    
    // unlock map
    pthread_mutex_unlock(&map->lock);
    return true;
}

// Find the value based on the key
void *hashMapFind(HashMap *map, const char *key) {
    uint32_t hash = bkdrHash(key) % HASH32_MAP_MAX_BUCKETS;
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
        // loop single linked list
        while (entry != NULL) {
            HashMapEntry *next = entry->next;
            debugPrint("free map entry key=%s\n", entry->key);
            // free entry
            freeStbChange((StbChange *)entry->value);
            free(entry->key);
            free(entry);

            // next
            entry = next;
        }
    }
    pthread_mutex_destroy(&map->lock);
} 


//
// -----------------  dbChagne -------------------------
//

// create 
DBChange *createDbChange(const char *dbPath) {
    //TOTO
    DBChange * pDbChange = (DBChange *)calloc(1, sizeof(DBChange));
    pDbChange->dbPath    = dbPath;

    return pDbChange;
}

// free db
void freeDBChange(DBChange *pDbChange) {
    // free stbChagne map
    hashMapDestroy(&pDbChange->stbMap);

    // free 
    free(pDbChange);
}

// free stb
void freeStbChange(StbChange *stbChange) {
    // free tableDes
    if (stbChange->tableDes) {
        freeTbDes(stbChange->tableDes, true);
        stbChange->tableDes =NULL;
    }

    // free part
    if (stbChange->strCols) {
        free(stbChange->strCols);
        stbChange->strCols = NULL;
    }
    if (stbChange->strTags) {
        free(stbChange->strTags);
        stbChange->strTags = NULL;
    }

    // free
    free(stbChange);
}


// generate part string
char * genPartStr(ColDes *colDes, int from , int num) {
    // TODO
    int32_t size  = 50 + num * (TSDB_COL_NAME_LEN + 2);
    char *partStr = calloc(1, size);
    int32_t pos   = 0;
    for (int32_t i = 0; i < num; i++) {
        pos += sprintf(partStr + pos, 
                i == 0 ? "%s" : ",%s",
                colDes[i].field);
    }

    return partStr;
}


// return true: schema no changed , false: changed
bool schemaNoChanged(RecordSchema *recordSchema, TableDes *tableDesSrv) {
    // local is recordSchema->tableDes
    TableDes * localDes = recordSchema->tableDes;
    char     * stb      = recordSchema->stbName;

    // compare col and tag count
    if (localDes->columns != tableDesSrv->columns) {
        infoPrint("stb:%s columns count changed . local:%d server:%d \n", stb, localDes->columns, tableDesSrv->columns);
        return false;
    }
    if (localDes->tags != tableDesSrv->tags) {
        infoPrint("stb:%s tags count changed . local:%d server:%d \n", stb, localDes->tags, tableDesSrv->tags);
        return false;
    }

    // filed & type
    for (int32_t i = 0; i < localDes->columns + localDes->tags; i++) {
        ColDes * local = &localDes->cols[i];
        ColDes * srv   = &tableDesSrv->cols[i];
        // field name
        if (strcmp(local->field, srv->field) != 0) {
            // field name
            infoPrint("i=%d stb=%s field name changed. local:%s server:%s \n", i, stb, local->field, srv->field);
            return false;    
        }
        // type
        if (local->type != srv->type) {
            infoPrint("i=%d stb=%s field name same but type changed. %s local:%d server:%d \n", i, stb, local->field, local->type, srv->type);
            return false;    
        }
    }

    return true;
}

// find field same in local
bool findFieldInLocal(ColDes *colDes, TableDes * tableDes) {
    for (int32_t i = 0; i < tableDes->columns + tableDes->tags ; i++) {
        if (strcmp(colDes->field,  tableDes->cols[i].field) == 0 &&
                   colDes->type == tableDes->cols[i].type ) {
            debugPrint("%s i=%d found fields:%s type=%d\n", __func__, i, colDes->field, colDes->type);
            return true;
        }
    }

    debugPrint("%s not found fields:%s type=%d\n", __func__, colDes->field, colDes->type);
    return false;
}

void moveColDes(ColDes *colDes, int32_t des, int32_t src) {
    // same no need move
    if (des == src) {
        return ;
    }

    // set
    colDes[des] = colDes[src];
}

// local schema recordSchema cross with server schema tableDes
int32_t localCrossServer(DBChange *pDbChange, StbChange *pStbChange, RecordSchema *recordSchema, TableDes *tableDesSrv) {
    // record old
    int oldc = tableDesSrv->columns;
    int oldt = tableDesSrv->tags;

    int newc = 0; // col num
    int newt = 0; // tag num

    // check schema no change
    if (schemaNoChanged(recordSchema, tableDesSrv)) {
        infoPrint("stb:%s schema no changed. server col:%d tag:%d\n", recordSchema->stbName, oldc, oldt);
        pStbChange->tableDes      = tableDesSrv;
        pStbChange->schemaChanged = false;
        pStbChange->strCols       = NULL;
        pStbChange->strTags       = NULL;
        return 0;
    }

    // loop all
    for (int i = 0; i < oldc + oldt; i++) {
        ColDes * colDes = tableDesSrv->cols + i;
        if (i < oldc) {
            // col
            if (findFieldInLocal(colDes, recordSchema->tableDes)) {
                moveColDes(tableDesSrv->cols, i, newc);
                ++newc;
            }
        } else {
            // tag
            if (findFieldInLocal(colDes, recordSchema->tableDes)) {
                moveColDes(tableDesSrv->cols, i,  newc + newt);
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
    tableDesSrv->columns = newc;
    tableDesSrv->tags    = newt;

    // save tableDes to StbChange
    pStbChange->tableDes = tableDesSrv;
    
    // gen part str
    pStbChange->strCols = genPartStr(tableDesSrv->cols, 0,    newc);
    pStbChange->strTags = genPartStr(tableDesSrv->cols, newc, newt);

    pStbChange->schemaChanged = true;
    // show change log
    infoPrint("stb:%s have schema changed. server col:%d tag:%d local col:%d tag:%d\n", 
                recordSchema->stbName, oldc, oldt, newc, newt);

    return 0;
}


// add stb recordSchema to dbChange
int32_t AddStbChanged(DBChange *pDbChange, TAOS *taos, RecordSchema *recordSchema, StbChange **ppStbChange) {
    // check old json schema
    if (recordSchema->version == 0) {
        debugPrint("%s is old schema json.\n", recordSchema->name);
        return 0;
    }

    char *stbName = recordSchema->stbName;
    if (stbName == NULL || stbName[0] == 0) {
        errorPrint("%s() LN:%d, stbName null or empty.", __func__, __LINE__);
        return -1;
    }

    //
    // server
    //
    TableDes *tableDesSrv = (TableDes *)calloc(1, sizeof(TableDes) + sizeof(ColDes) * TSDB_MAX_COLUMNS);
    if (NULL == tableDesSrv) {
        errorPrint("%s() LN%d, mallocDes calloc failed!\n", __func__, __LINE__);
        return -1;
    }

    // get from server
    if (getTableDes(taos, pDbChange->dbName, stbName, tableDesSrv, false) < 0) {
        errorPrint("%s() LN%d getTableDes failed, db:%s stb:%s !\n", __func__, __LINE__, pDbChange->dbName, stbName);
        return -1;
    }

    // init
    StbChange *pStbChange = (StbChange *)calloc(1, sizeof(StbChange));

    //
    // compare local & server and auto calc 
    //
    if (localCrossServer(pDbChange, pStbChange, recordSchema, tableDesSrv)) {
        errorPrint("%s() LN%d localCrossServer failed, db:%s stb:%s !\n", __func__, __LINE__, pDbChange->dbName, stbName);
        free(pStbChange);
        freeTbDes(tableDesSrv, true);
        return -1;
    }
    
    // set out
    if (ppStbChange) {
        *ppStbChange = pStbChange;
    }

    // add to DbChange hashMap
    if (!hashMapInsert(&pDbChange->stbMap, stbName, pStbChange)) {
        errorPrint("%s() LN%d add hashMap failed, db:%s stb:%s !\n", __func__, __LINE__, pDbChange->dbName, stbName);
        free(pStbChange);
        freeTbDes(tableDesSrv, true);
        return -1;
    }

    // nothing free

    return 0;
}

// find stbChange with stbName
StbChange * findStbChange(DBChange *pDbChange, char *stbName) {
    // check valid
    if (pDbChange == NULL || stbName == NULL || stbName[0] == 0 ) {
        return NULL;
    }

    // find
    return (StbChange *)hashMapFind(&pDbChange->stbMap, stbName);
}
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


#include <jansson.h>
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

    // websocket error
    if (code >= TSDB_CODE_WBS_NETWORK_BEGIN && code <= TSDB_CODE_WBS_NETWORK_END) {
        return true;
    }

    // single code
    int32_t codes[] = {0x0000ffff, TSDB_CODE_VND_STOPPED};
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
// ------------ json util ------------
//

static void print_json_indent(int indent) {
    int i;
    for (i = 0; i < indent; i++) {
        putchar(' ');
    }
}

static void print_json_array(json_t *element, int indent) {
    size_t i;
    size_t size = json_array_size(element);
    print_json_indent(indent);

    printf("JSON Array of %zu element: %s\n", size,
            json_plural(size));
    for (i = 0; i < size; i++) {
        print_json_aux(json_array_get(element, i), indent + 2);
    }
}

static void print_json_string(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON String: \"%s\"\n", json_string_value(element));
}

const char *json_plural(size_t count) {
    return count == 1 ? "" : "s";
}

static void print_json_object(json_t *element, int indent) {
    size_t size;
    const char *key;
    json_t *value;

    print_json_indent(indent);
    size = json_object_size(element);

    printf("JSON Object of %zu pair: %s\n",
            size, json_plural(size));
    json_object_foreach(element, key, value) {
        print_json_indent(indent + 2);
        printf("JSON Key: \"%s\"\n", key);
        print_json_aux(value, indent + 2);
    }
}


void print_json_aux(json_t *element, int indent) {
    switch (json_typeof(element)) {
        case JSON_OBJECT:
            print_json_object(element, indent);
            break;

        case JSON_ARRAY:
            print_json_array(element, indent);
            break;

        case JSON_STRING:
            print_json_string(element, indent);
            break;
/* not used so far
        case JSON_INTEGER:
            print_json_integer(element, indent);
            break;

        case JSON_REAL:
            print_json_real(element, indent);
            break;

        case JSON_TRUE:
            print_json_true(element, indent);
            break;

        case JSON_FALSE:
            print_json_false(element, indent);
            break;

        case JSON_NULL:
            print_json_null(element, indent);
            break;
*/

        default:
            errorPrint("Unrecognized JSON type %d\n", json_typeof(element));
    }
}

void print_json(json_t *root) {
    print_json_aux(root, 0);
}

json_t *load_json(char *jsonbuf) {
    json_t *root = NULL;
    json_error_t error;

    root = json_loads(jsonbuf, 0, &error);
    if (root) {
        return root;
    } else {
        errorPrint("JSON error on line %d: %s\n", error.line, error.text);
        return NULL;
    }
}

//
//  ------------- file operator ----------------
//

// write file
int32_t writeFile(char *filename, char *txt) {
    FILE * fp = fopen(filename, "w+");
    if(fp == NULL) {
        warnPrint("open file failed. file=%s error=%s\n", filename, strerror(errno));
        return -1;
    }

    // write
    if(fprintf(fp, "%s", txt) < 0) {
        errorPrint("write file failed. file=%s error=%s context=%s\n", filename, strerror(errno), txt);
        fclose(fp);
        return -1;
    }

    fclose(fp);
    return 0;
}

long getFileSize(FILE *fp) {
    long cur = ftell(fp);
    // move end
    if (fseek(fp, 0, SEEK_END) != 0) {
        return -1;
    }
    // get
    long size = ftell(fp);
    fseek(fp, cur, SEEK_SET);
    return size;
}

// read file context
char * readFile(char *filename) {
    // open
    FILE * fp = fopen(filename, "r");
    if(fp == NULL) {
        warnPrint("open file failed. file=%s error=%s\n", filename, strerror(errno));
        return NULL;
    }

    // size
    long size = getFileSize(fp);
    if (size <= 0) {
        errorPrint("getFileSize failed size=%ld. file=%s error=%s\n", size, filename, strerror(errno));
        fclose(fp);
        return NULL;
    }

    // calloc
    long bufLen= size + 10;
    char * buf = calloc(bufLen + 10, 1);
    if(buf == NULL) {
        errorPrint("malloc memory size=%ld failed. file=%s error=%s\n", bufLen, filename, strerror(errno));
        fclose(fp);
        return NULL;
    }

    // read
    size_t readLen = fread(buf, 1, size, fp);
    if (readLen != size) {
        errorPrint("read file failed. expect read=%ld real=%ld file=%s error=%s\n", size, readLen, filename, strerror(errno));
        free(buf);
        buf = NULL;
    }

    // succ
    fclose(fp);
    return buf;
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
            infoPrint("debug: taosQuery succeeded. sql=%s \n", sql);
            return res;
        }

        // fail
        errorPrint("Failed to execute taosQuery, code: 0x%08x, reason: %s, sql=%s \n", *code, taos_errstr(res), sql);

        // can retry
        if(!canRetry(*code, RETRY_TYPE_QUERY)) {
            infoPrint("%s", "error code not in retry range, give up retry.\n");
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
    if (str == NULL) {
        return 0;
    }
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
    hashMapInit(&pDbChange->stbMap);

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
    // check valid
    if (colDes == 0 || num == 0) {
        return NULL;
    }

    int32_t size  = 50 + num * (TSDB_COL_NAME_LEN + 2);
    char *partStr = calloc(1, size);
    int32_t pos   = 0;
    for (int32_t i = 0; i < num; i++) {
        pos += sprintf(partStr + pos,
                i == 0 ? "(`%s`" : ",`%s`",
                colDes[from + i].field);
    }
    // end
    strcat(partStr, ")");

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

        // save local col idx
        srv->idx = local->idx;
    }

    return true;
}

// find field same in server
bool findFieldInServer(ColDes *colDes, TableDes * tableDes) {
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

// copy tableDes , cols->var_val not copy
void copyTableDes(TableDes *des, TableDes* src) {
    // whole copy
    int32_t num = src->columns + src->tags;
    memcpy(des, src, sizeof(TableDes) + sizeof(ColDes) * num);
    // src pointer must set null
    for (int32_t i = 0; i < num; i++) {
        des->cols[i].var_value = NULL;
    }
}

// local schema recordSchema cross with server schema tableDes
int32_t localCrossServer(DBChange *pDbChange, StbChange *pStbChange, RecordSchema *recordSchema, TableDes *tableDesSrv) {
    // record old
    TableDes* localDes = recordSchema->tableDes;
    int oldc = localDes->columns;
    int oldt = localDes->tags;

    // local & server cross -> crossDes
    TableDes* crossDes = (TableDes *)calloc(1, sizeof(TableDes) + sizeof(ColDes) * (oldc + oldt));

    // crossDes
    int newc = 0; // col num
    int newt = 0; // tag num

    // check schema no change
    if (schemaNoChanged(recordSchema, tableDesSrv)) {
        infoPrint("stb:%s schema no changed. server col:%d tag:%d\n", recordSchema->stbName, oldc, oldt);
        copyTableDes(crossDes, localDes);
        pStbChange->tableDes      = crossDes;
        pStbChange->schemaChanged = false;
        pStbChange->strCols       = NULL;
        pStbChange->strTags       = NULL;
        return 0;
    }

    // loop all
    for (int i = 0; i < oldc + oldt; i++) {
        ColDes * colDes = localDes->cols + i;
        if (i < oldc) {
            // col
            if (findFieldInServer(colDes, tableDesSrv)) {
                crossDes->cols[newc] = *colDes;
                // copy struct pointer must set NULL
                crossDes->cols[newc].var_value = NULL;
                ++newc;
            }
        } else {
            // tag
            if (findFieldInServer(colDes, tableDesSrv)) {
                crossDes->cols[newc + newt] = *colDes;
                // copy struct pointer must set NULL
                crossDes->cols[newc + newt].var_value = NULL;
                ++newt;
            }
        }
    }

    // check valid
    if (newc == 0) {
        // col must not zero
        errorPrint("backup data schema no same column with server table:%s local col num:%d server col num:%d\n",
            crossDes->name, recordSchema->tableDes->columns, crossDes->columns);
        freeTbDes(crossDes, true);
        return -1;
    }
    if (newt == 0 && oldt > 0) {
        // tag must not zero
        errorPrint("%s() LN%d, new tag zero failed! oldt=%d\n", __func__, __LINE__, oldt);
        freeTbDes(crossDes, true);
        return -1;
    }

    // set new
    crossDes->columns = newc;
    crossDes->tags    = newt;
    strcpy(crossDes->name, localDes->name);

    // save crossDes to StbChange
    pStbChange->tableDes = crossDes;

    // gen part str
    pStbChange->strCols = genPartStr(pStbChange->tableDes->cols, 0,    newc);
    pStbChange->strTags = genPartStr(pStbChange->tableDes->cols, newc, newt);

    pStbChange->schemaChanged = true;
    // show change log
    infoPrint("stb:%s have schema changed. server col:%d tag:%d local col:%d tag:%d\n",
                recordSchema->stbName, oldc, oldt, newc, newt);

    return 0;
}


// add stb recordSchema to dbChange
int32_t AddStbChanged(DBChange *pDbChange, const char* dbName, TAOS *taos, RecordSchema *recordSchema, StbChange **ppStbChange) {
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
    if (getTableDes(taos, dbName, stbName, tableDesSrv, false) < 0) {
        infoPrint("%s() LN%d getTableDes failed, db:%s stb:%s !\n", __func__, __LINE__, dbName, stbName);
        freeTbDes(tableDesSrv, true);
        // normal table here not create table on server
        return 0;
    }

    // init
    StbChange *pStbChange = (StbChange *)calloc(1, sizeof(StbChange));

    //
    // compare local & server and auto calc
    //
    if (localCrossServer(pDbChange, pStbChange, recordSchema, tableDesSrv)) {
        errorPrint("%s() LN%d localCrossServer failed, db:%s stb:%s !\n", __func__, __LINE__, dbName, stbName);
        free(pStbChange);
        freeTbDes(tableDesSrv, true);
        return -1;
    }
    freeTbDes(tableDesSrv, true);
    tableDesSrv = NULL;

    // set out
    if (ppStbChange) {
        *ppStbChange = pStbChange;
    }

    // add to DbChange hashMap
    if (!hashMapInsert(&pDbChange->stbMap, stbName, pStbChange)) {
        errorPrint("%s() LN%d add hashMap failed, db:%s stb:%s !\n", __func__, __LINE__, dbName, stbName);
        if (pStbChange->tableDes) {
            freeTbDes(pStbChange->tableDes, true);
            pStbChange->tableDes = NULL;
        }
        free(pStbChange);
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

static int32_t readStbSchemaCols( json_t *elements, ColDes *cols) {
    const char *key   = NULL;
    json_t     *value = NULL;

    // check valid
    if (JSON_ARRAY != json_typeof(elements)) {
        warnPrint("%s() LN%d, stbSchema have no array\n",
            __func__, __LINE__);
        return  0;
    }

    size_t size = json_array_size(elements);

    for (size_t i = 0; i < size; i++) {
        json_t *element = json_array_get(elements, i);

        // loop read
        ColDes *col = cols + i;
        json_object_foreach(element, key, value) {
            if (0 == strcmp(key, "name")) {
                strncpy(col->field, json_string_value(value), TSDB_COL_NAME_LEN - 1);
            } else if (0 == strcmp(key, "type")) {
                col->type = json_integer_value(value);
            }
        }

        // set idx
        col->idx = i;
    }

    return size;
}

// read stb schema
static int32_t readJsonStbSchema( json_t *element, RecordSchema *recordSchema) {

    const char *key   = NULL;
    json_t     *value = NULL;
    uint32_t   n      = 0;

    // maloc tableDes
    TableDes *tableDes = (TableDes *)calloc(1, sizeof(TableDes) + sizeof(ColDes) * TSDB_MAX_COLUMNS);
    if (tableDes == NULL) {
        errorPrint("%s() LN%d, malloc memory TableDes failed.\n", __func__, __LINE__);
        return -1;
    }

    json_object_foreach(element, key, value) {
        if (0 == strcmp(key, "version")) {
            recordSchema->version = json_integer_value(value);
        } else if (0 == strcmp(key, "name")) {
            tstrncpy(recordSchema->stbName, json_string_value(value), RECORD_NAME_LEN - 1);
        } else if (0 == strcmp(key, "tags")) {
            tableDes->tags = readStbSchemaCols(value, tableDes->cols + n);
            n += tableDes->tags;
        } else if (0 == strcmp(key, "cols")) {
            tableDes->columns = readStbSchemaCols(value, tableDes->cols + n);
            n += tableDes->columns;
        }
    }

    // check valid
    if (tableDes->columns == 0) {
        errorPrint("%s() LN%d, TableDes->columns is zero. stbName=%s\n", __func__, __LINE__, tableDes->name);
        //return -1;
    }

    debugPrint("%s() LN%d, stbName=%s tags=%d columns=%d\n", __func__, __LINE__, tableDes->name, tableDes->tags, tableDes->columns);

    // set
    recordSchema->tableDes = tableDes;
    return 0;

}

// read
int32_t mFileToRecordSchema(char *avroFile, RecordSchema* recordSchema) {
    char mFile[MAX_PATH_LEN];
    strcpy(mFile, avroFile);
    strcat(mFile, MFILE_EXT);

    // read
    char *json = readFile(mFile);
    if (json == NULL) {
        // old no this file
        return 0;
    }

    // parse json
    int32_t ret = -1;
    json_t *json_root = load_json(json);
    if (json_root) {
        if (g_args.verbose_print) {
            print_json(json_root);
        }

        ret = readJsonStbSchema(json_root, recordSchema);
        json_decref(json_root);
    } else {
        errorPrint("json:\n%s\n can't be parsed by jansson file=%s\n", json, mFile);
    }

    // free
    free(json);
    return ret;
}

// found
bool idxInBindTags(int16_t idx, TableDes* tableDes) {
    // check valid
    if (idx < 0 || tableDes == NULL) {
        return false;
    }

    // find in list
    for (int32_t i = tableDes->columns ; i < tableDes->columns + tableDes->tags; i++) {
        if (tableDes->cols[i].idx == idx) {
            return true;
        }
    }

    return false;
}

// found
bool idxInBindCols(int16_t idx, TableDes* tableDes) {
    // check valid
    if (idx < 0 || tableDes == NULL) {
        return false;
    }

    // find in list
    for (int32_t i = 0 ; i < tableDes->columns; i++) {
        if (tableDes->cols[i].idx == idx) {
            return true;
        }
    }

    return false;
}

//
// if avro folder changed, need have new stbChange*
//
StbChange* readFolderStbName(char *folder, DBChange *pDbChange) {
    // compare avro file
    char stbFile[MAX_PATH_LEN] = {0};

    if (pDbChange == NULL) {
        return NULL;
    }

    // combine file
    strcpy(stbFile, folder);
    strcat(stbFile, STBNAME_FILE);

    // folder changed
    char *stbName = readFile(stbFile);
    if(stbName == NULL) {
        debugPrint("read stbname failed. %s\n", stbFile);
        return NULL;
    }

    // find stbChange with stbName
    StbChange *stbChange = hashMapFind(&pDbChange->stbMap, stbName);

    debugPrint("hashmapfind stb:%s stbchange=%p \n", stbName, stbChange);
    free(stbName);
    return stbChange;
}

// reserve json item space 70 bytes
uint32_t getTbDesJsonSize(TableDes *tableDes, bool onlyColumn) {

    //
    // public
    //
    uint32_t size =
        ITEM_SPACE +                    // version 1
        ITEM_SPACE +                    // type: record
        ITEM_SPACE + TSDB_DB_NAME_LEN + // namespace: dbname
        ITEM_SPACE;                     // field : {}


    //
    // fields
    //
    int fieldCnt = tableDes->columns + tableDes->tags + 2; // +2 is tbname and stbname
    if(onlyColumn) {
        fieldCnt -= tableDes->tags;
    }
    size += (TSDB_COL_NAME_LEN + ITEM_SPACE) * fieldCnt;

    return size;
}


// get stb schema size
uint32_t getStbSchemaSize(TableDes *tableDes) {
    //
    // stable schema
    //
    uint32_t size =
        ITEM_SPACE +                      // version 1
        ITEM_SPACE + TSDB_TABLE_NAME_LEN; // stbName

    // stb fields for db schema
    size += (tableDes->columns + tableDes->tags) * (ITEM_SPACE + TSDB_COL_NAME_LEN);

    return size;
}


uint32_t colDesToJson(char *pstr, ColDes * colDes, uint32_t num) {
    uint32_t size = 0;
    for(int32_t i = 0; i < num; i++) {
        if (i > 0) {
            // append splite
            size += sprintf(pstr + size, ",");
        }

        // field
        size += sprintf(pstr + size,
            "{\"name\":\"%s\", \"type\":%d}",
            colDes[i].field, colDes[i].type);
    }
    return size;
}


// covert tableDes to json
char* tableDesToJson(TableDes *tableDes) {
    // calloc
    char *p = calloc(1, getStbSchemaSize(tableDes));
    uint32_t size = 0;
    char *pstr = p;
    // stbName
    size += sprintf(pstr + size,
        "{"
        "\"%s\":%d,"
        "\"%s\":\"%s\",",
        VERSION_KEY, VERSION_VAL,
        STBNAME_KEY, tableDes->name);

    // cols
    size += sprintf(pstr + size,
        "\"cols\": [");
    size += colDesToJson(pstr + size,
        tableDes->cols, tableDes->columns);
    size += sprintf(pstr + size,
        "]");

    // tags
    if (tableDes->tags > 0) {
        size += sprintf(pstr + size,
            ",\"tags\": [");
        size += colDesToJson(pstr + size,
            tableDes->cols + tableDes->columns, tableDes->tags);
        size += sprintf(pstr + size,
            "]");
    }

    // end
    size += sprintf(pstr + size,
        "}");

    return p;
}

// create normal table .m file with meta
int32_t createNTableMFile(char * metaFileName, TableDes* tableDes) {
    char mfile[MAX_PATH_LEN] = {0};
    strcpy(mfile, metaFileName);
    strcat(mfile, MFILE_EXT);

    // create
    int32_t ret = -1;
    char *ntbJson = tableDesToJson(tableDes);
    if (ntbJson) {
        ret = writeFile(mfile, ntbJson);
        if(ret) {
            errorPrint("failed to write normal table mfile:%s\n", mfile);
        }
        free(ntbJson);
    }

    return ret;
}

// check dbPath is normal table folder
bool normalTableFolder(const char* dbPath) {
    if (dbPath == NULL || dbPath[0] == 0) {
        return false;
    }
    size_t len = strlen(dbPath);
    if (len <= strlen(NTABLE_FOLDER)) {
        return false;
    }

    if (strcmp(dbPath + len - sizeof(NTABLE_FOLDER) + 1, NTABLE_FOLDER) == 0) {
        return true;
    }
    return false;
}
#ifndef INC_DUMPUTIL_H_
#define INC_DUMPUTIL_H_

#include <taos.h>
#include <taoserror.h>
#include <toolsdef.h>

//
// ------------  error code range defin ------------
//

// engine can retry code range
#define TSDB_CODE_RPC_BEGIN                     TSDB_CODE_RPC_NETWORK_UNAVAIL
#define TSDB_CODE_RPC_END                       TSDB_CODE_RPC_ASYNC_MODULE_QUIT

// websocket can retry code range
// range1 [0x0001~0x00FF]
#define WEBSOCKET_CODE_BEGIN1      0x0001
#define WEBSOCKET_CODE_END1        0x00FF
// range2 [0x0001~0x00FF]
#define WEBSOCKET_CODE_BEGIN2      0xE000
#define WEBSOCKET_CODE_END2        0xE0FF

//
//   encapsulate the api of calling engine
//
#define RETRY_TYPE_CONNECT 0
#define RETRY_TYPE_QUERY   1
#define RETRY_TYPE_FETCH   2

//come from TDengine util/tdef.h
#define TSDB_TABLE_NAME_LEN           193                                // it is a null-terminated string

//
// ------------- struct define ----------
//

// single link 
typedef struct SNode {
    char name[TSDB_TABLE_NAME_LEN];
    struct SNode *next;
}SNode;

// declare dump.h
struct TableDes;


// return true to do retry , false no retry , code is error code 
bool canRetry(int32_t code, int8_t type);

// single linked list

// malloc new node
SNode *mallocNode(const char* name, int32_t len);

// free nodes
void freeNodes(SNode* head);

static void freeTbDes(TableDes *tableDes, bool self);

//
// ---------------  native ------------------
//


// connect
TAOS *taosConnect(const char *dbName);
// query
TAOS_RES *taosQuery(TAOS *taos, const char *sql, int32_t *code);



//
// ---------------- hash map -----------------
//

// Define the maximum number of buckets
#define HASH32_MAP_MAX_BUCKETS 1024

// Define the key-value pair structure
typedef struct HashMapEntry {
    char *key;
    void *value;
    struct HashMapEntry *next;
} HashMapEntry;

// Define the hash table structure
typedef struct HashMap {
    HashMapEntry *buckets[HASH32_MAP_MAX_BUCKETS];
    pthread_mutex_t lock;
} HashMap;

// Initialize the hash table
HashMap* hashMapCreate();

// Insert a key-value pair
bool hashMapInsert(HashMap *map, const char *key, void *value);

// Find the value based on the key
void *hashMapFind(HashMap *map, const char *key);

// Destroy the hash table
void hashMapDestroy(HashMap *map);


//
// -----------------  dbChange -------------------------
//
struct DBChange;
struct StbChange;
struct RecordSchema;

// create db
DBChange createDbChange(const char *dbPath);
// free db
void freeDBChange(DBChange *pDbChange);
// free stb
void freeStbChange(StbChange *stbChange);




// add stb recordSchema to dbChange
int32_t AddStbChanged(DBChange *pDbChange, TAOS taos, RecordSchema *recordSchema, StbChange **ppStbChange);

// find stbChange with stbName
StbChange * findStbChange(DBChange *pDbChange, char *stbName);



#endif  // INC_DUMPUTIL_H_
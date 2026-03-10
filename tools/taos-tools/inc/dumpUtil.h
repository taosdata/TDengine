#ifndef INC_DUMPUTIL_H_
#define INC_DUMPUTIL_H_

#include <taos.h>
#include <taoserror.h>
#include <toolsdef.h>
#include "dump.h"

//
// ------------  error code range defin ------------
//

// engine can retry code range
#define TSDB_CODE_RPC_BEGIN                     TSDB_CODE_RPC_NETWORK_UNAVAIL
#define TSDB_CODE_RPC_END                       TSDB_CODE_RPC_ASYNC_MODULE_QUIT

// websocket can retry code range
#define TSDB_CODE_WBS_NETWORK_BEGIN             TAOS_DEF_ERROR_CODE(0, 0xE001)
#define TSDB_CODE_WBS_NETWORK_END               TAOS_DEF_ERROR_CODE(0, 0xE0FF)

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

//
// -----------  util fun -------------
//

void print_json(json_t *root);
json_t *load_json(char *jsonbuf);
void print_json_aux(json_t *element, int indent);
const char *json_plural(size_t count);

// return true to do retry , false no retry , code is error code
bool canRetry(int32_t code, int8_t type);

// single linked list

// malloc new node
SNode *mallocNode(const char* name, int32_t len);

// free nodes
void freeNodes(SNode* head);


void freeTbDes(TableDes *tableDes, bool self);

//
// ---------------  native ------------------
//


// connect
TAOS *taosConnect(const char *dbName);
// query
TAOS_RES *taosQuery(TAOS *taos, const char *sql, int32_t *code);


//
//  ------------- file operator ----------------
//

// write file
int32_t writeFile(char *filename, char *txt);
// read file
char * readFile(char *filename);


//
// ---------------- hash map -----------------
//

// hash
uint32_t bkdrHash(const char *str);

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


// create db
DBChange *createDbChange(const char *dbPath);
// free db
void freeDBChange(DBChange *pDbChange);
// free stb
void freeStbChange(StbChange *stbChange);


// add stb recordSchema to dbChange
int32_t AddStbChanged(DBChange *pDbChange, const char* dbName, TAOS *taos, RecordSchema *recordSchema, StbChange **ppStbChange);

// find stbChange with stbName
StbChange * findStbChange(DBChange *pDbChange, char *stbName);

// read mfile schema save to RecordSchema struct
int32_t mFileToRecordSchema(char *avroFile, RecordSchema* recordSchema);

// find cols
bool idxInBindCols(int16_t idx, TableDes* tableDes);
// find tags
bool idxInBindTags(int16_t idx, TableDes* tableDes);


//bool fieldInBindList(char *field, TableDes* tableDes);

StbChange* readFolderStbName(char *folder, DBChange *pDbChange);

// covert tableDes to json
char* tableDesToJson(TableDes *tableDes);

// create normal table .m file with meta
int32_t createNTableMFile(char * metaFileName, TableDes* tableDes);

uint32_t getTbDesJsonSize(TableDes *tableDes, bool onlyColumn);

bool normalTableFolder(const char* dbPath);

#endif  // INC_DUMPUTIL_H_
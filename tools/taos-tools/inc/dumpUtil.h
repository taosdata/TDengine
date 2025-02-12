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


// return true to do retry , false no retry , code is error code 
bool canRetry(int32_t code, int8_t type);

// single linked list

// malloc new node
SNode *mallocNode(const char* name, int32_t len);

// free nodes
void freeNodes(SNode* head);



//
// ---------------  native ------------------
//


// connect
TAOS *taosConnect(const char *dbName);
// query
TAOS_RES *taosQuery(TAOS *taos, const char *sql, int32_t *code);


//
// ---------------  websocket ------------------
//
#ifdef WEBSOCKET
// ws connect
WS_TAOS *wsConnect();
// ws query
WS_RES *wsQuery(WS_TAOS **ws_taos, const char *sql, int32_t *code);
// ws fetch
int32_t wsFetchBlock(WS_RES *rs, const void **pData, int32_t *numOfRows);

#endif


#endif  // INC_DUMPUTIL_H_
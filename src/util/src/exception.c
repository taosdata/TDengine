#include "exception.h"


static _Thread_local SExceptionNode* expList;

void expPushNode( SExceptionNode* node ) {
    node->prev = expList;
    expList = node;
}

int expPopNode() {
    SExceptionNode* node = expList;
    expList = node->prev;
    return node->code;
}

void expThrow( int code ) {
    expList->code = code;
    longjmp( expList->jb, 1 );
}

#include "exception.h"


static _Thread_local SExceptionNode* expList;

void exceptionPushNode( SExceptionNode* node ) {
    node->prev = expList;
    expList = node;
}

int exceptionPopNode() {
    SExceptionNode* node = expList;
    expList = node->prev;
    return node->code;
}

void exceptionThrow( int code ) {
    expList->code = code;
    longjmp( expList->jb, 1 );
}

void deferWrapper_void_ptr( SDeferedOperation* dp ) {
    void (*func)( void* ) = dp->func;
    func( dp->arg );
}

void deferWrapper_int_int( SDeferedOperation* dp ) {
    int (*func)( int ) = dp->func;
    func( (int)(intptr_t)(dp->arg) );
}

void deferWrapper_void_void( SDeferedOperation* dp ) {
    void (*func)() = dp->func;
    func();
}

void deferExecute( SDeferedOperation* operations, unsigned int numOfOperations ) {
    while( numOfOperations > 0 ) {
        --numOfOperations;
        SDeferedOperation* dp = operations + numOfOperations;
        dp->wrapper( dp );
    }
}

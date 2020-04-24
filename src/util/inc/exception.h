/*
 * Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_EXCEPTION_H
#define TDENGINE_EXCEPTION_H

#include <setjmp.h>
#include <stdint.h>
#include <assert.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * exception handling
 */
typedef struct SExceptionNode {
   struct SExceptionNode* prev;
   jmp_buf jb;
   int code;
} SExceptionNode;

void exceptionPushNode( SExceptionNode* node );
int exceptionPopNode();
void exceptionThrow( int code );

#define THROW( x )          exceptionThrow( (x) )
#define CAUGHT_EXCEPTION()  (caught_exception == 1)

#define TRY do { \
    SExceptionNode expNode = { 0 }; \
    exceptionPushNode( &expNode ); \
    int caught_exception = setjmp(expNode.jb); \
    if( caught_exception == 0 )

#define CATCH( code ) int code = exceptionPopNode(); \
    if( caught_exception == 1 )

#define FINALLY( code ) int code = exceptionPopNode();

#define END_TRY } while( 0 );


/*
 * defered operations
 */
typedef struct SDeferedOperation {
    void (*wrapper)( struct SDeferedOperation* dp );
    void* func;
    void* arg;
} SDeferedOperation;

void deferExecute( SDeferedOperation* operations, unsigned int numOfOperations );
void deferWrapper_void_void( SDeferedOperation* dp );
void deferWrapper_void_ptr( SDeferedOperation* dp );
void deferWrapper_int_int( SDeferedOperation* dp );

#define DEFER_INIT( MaxOperations ) unsigned int maxDeferedOperations = MaxOperations, numOfDeferedOperations = 0; \
    SDeferedOperation deferedOperations[MaxOperations]

#define DEFER_PUSH( wrapperFunc, deferedFunc, argument ) do { \
    assert( numOfDeferedOperations < maxDeferedOperations ); \
    SDeferedOperation* dp = deferedOperations + numOfDeferedOperations++; \
    dp->wrapper = wrapperFunc; \
    dp->func = (void*)deferedFunc; \
    dp->arg = (void*)argument; \
} while( 0 )

#define DEFER_POP() do { --numOfDeferedOperations; } while( 0 )

#define DEFER_EXECUTE() do{ \
    deferExecute( deferedOperations, numOfDeferedOperations ); \
    numOfDeferedOperations = 0; \
} while( 0 )

#define DEFER_PUSH_VOID_PTR( func, arg ) DEFER_PUSH( deferWrapper_void_ptr, func, arg )
#define DEFER_PUSH_INT_INT( func, arg ) DEFER_PUSH( deferWrapper_int_int, func, arg )
#define DEFER_PUSH_VOID_VOID( func ) DEFER_PUSH( deferWrapper_void_void, func, 0 )

#define DEFER_PUSH_FREE( arg ) DEFER_PUSH( deferWrapper_void_ptr, free, arg )
#define DEFER_PUSH_CLOSE( arg ) DEFER_PUSH( deferWrapper_int_int, close, arg )

#ifdef __cplusplus
}
#endif

#endif

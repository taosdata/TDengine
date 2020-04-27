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
#include <stdbool.h>
#include <assert.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * cleanup actions
 */
typedef struct SCleanupAction {
    bool failOnly;
    uint8_t wrapper;
    uint16_t reserved;
    void* func;
    union {
        void* Ptr;
        bool Bool;
        char Char;
        int8_t Int8;
        uint8_t Uint8;
        int16_t Int16;
        uint16_t Uint16;
        int Int;
        unsigned int Uint;
        int32_t Int32;
        uint32_t Uint32;
        int64_t Int64;
        uint64_t Uint64;
        float Float;
        double Double;
    } arg1, arg2;
} SCleanupAction;

void cleanupPush_void_ptr_ptr   ( bool failOnly, void* func, void* arg1, void* arg2 );
void cleanupPush_void_ptr_bool  ( bool failOnly, void* func, void* arg1, bool arg2 );
void cleanupPush_void_ptr       ( bool failOnly, void* func, void* arg );
void cleanupPush_int_int        ( bool failOnly, void* func, int arg );
void cleanupPush_void           ( bool failOnly, void* func );

int32_t cleanupGetActionCount();
void cleanupExecute( int32_t anchor, bool failed );

#define CLEANUP_PUSH_VOID_PTR_PTR( failOnly, func, arg1, arg2 )  cleanupPush_void_ptr_ptr( (failOnly), (void*)(func), (void*)(arg1), (void*)(arg2) )
#define CLEANUP_PUSH_VOID_PTR_BOOL( failOnly, func, arg1, arg2 ) cleanupPush_void_ptr_bool( (failOnly), (void*)(func), (void*)(arg1), (bool)(arg2) )
#define CLEANUP_PUSH_VOID_PTR( failOnly, func, arg )             cleanupPush_void_ptr( (failOnly), (void*)(func), (void*)(arg) )
#define CLEANUP_PUSH_INT_INT( failOnly, func, arg )              cleanupPush_void_ptr( (failOnly), (void*)(func), (int)(arg) )
#define CLEANUP_PUSH_VOID( failOnly, func )                      cleanupPush_void( (failOnly), (void*)(func) )
#define CLEANUP_PUSH_FREE( failOnly, arg )                       cleanupPush_void_ptr( (failOnly), free, (void*)(arg) )
#define CLEANUP_PUSH_CLOSE( failOnly, arg )                      cleanupPush_int_int( (failOnly), close, (int)(arg) )

#define CLEANUP_CREATE_ANCHOR()     int32_t cleanupAnchor = cleanupGetActionCount()
#define CLEANUP_EXECUTE( failed )   cleanupExecute( cleanupAnchor, (failed) )

/*
 * exception hander registration
 */
typedef struct SExceptionNode {
   struct SExceptionNode* prev;
   jmp_buf jb;
   int32_t code;
   int32_t maxCleanupAction;
   int32_t numCleanupAction;
   SCleanupAction* cleanupActions;
} SExceptionNode;

void exceptionPushNode( SExceptionNode* node );
int32_t exceptionPopNode();
void exceptionThrow( int code );

#define TRY(maxCleanupActions) do { \
    SExceptionNode exceptionNode = { 0 }; \
    SDeferedOperation cleanupActions[maxCleanupActions > 0 ? maxCleanupActions : 1]; \
    exceptionNode.maxCleanupAction = maxCleanupActions > 0 ? maxDefered : 1; \
    exceptionNode.cleanupActions = cleanupActions; \
    int32_t cleanupAnchor = 0; \
    exceptionPushNode( &exceptionNode ); \
    int caughtException = setjmp( exceptionNode.jb ); \
    if( caughtException == 0 )

#define CATCH( code ) int code = exceptionPopNode(); \
    if( caughtEexception == 1 )

#define FINALLY( code ) int code = exceptionPopNode();

#define END_TRY } while( 0 );

#define THROW( x )          exceptionThrow( (x) )
#define CAUGHT_EXCEPTION()  ((bool)(caughtEexception == 1))

#ifdef __cplusplus
}
#endif

#endif

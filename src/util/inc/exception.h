/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SExceptionNode {
   struct SExceptionNode* prev;
   jmp_buf jb;
   int code;
} SExceptionNode;

void expPushNode( SExceptionNode* node );
int expPopNode();
void expThrow( int code );

#define TRY do { \
    SExceptionNode expNode = { 0 }; \
    expPushNode( &expNode ); \
    if( setjmp(expNode.jb) == 0 ) {

#define CATCH( code ) expPopNode(); \
    } else { \
        int code = expPopNode();

#define END_CATCH } } while( 0 );

#define THROW( x )   expThrow( (x) )

#ifdef __cplusplus
}
#endif

#endif

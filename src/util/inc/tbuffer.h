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

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <setjmp.h>

#ifndef TDENGINE_TBUFFER_H
#define TDENGINE_TBUFFER_H


/*
SBuffer can be used to read or write a buffer, but cannot be used for both
read & write at a same time.
Read example:
   SBuffer rbuf;
   if (tbufBeginOperation(&rbuf) != 0) {
      // handling errors
   }
   tbufInitRead(&rbuf, data, 1024);
   int32_t a = tbufReadInt32(&rbuf);
   // other read functions

Write example:
   SBuffer wbuf;
   if (tbufBeginOperation(&wbuf) != 0) {
      // handling errors
   }
   tbufInitWrite(&wbuf, 1024);
   tbufWriteInt32(&wbuf, 10);
   // other write functions
   size_t size = tbufGetSize(&wbuf);
   char* data = tbufGetBuffer(&wbuf, true);
   tbufUninitWrite(&wbuf);
*/
typedef struct {
  jmp_buf jb;
  char* buf;
  size_t pos;
  size_t size;
} SBuffer;


// common functions can be used in both read & write
#define tbufBeginOperation(buf) setjmp((buf)->jb)
size_t tbufTell(SBuffer* buf);
size_t tbufSeekTo(SBuffer* buf, size_t pos);
size_t tbufSkip(SBuffer* buf, size_t size);


// basic read functions
void tbufInitRead(SBuffer* buf, void* data, size_t size);
char* tbufRead(SBuffer* buf, size_t size);
void tbufReadToBuffer(SBuffer* buf, void* dst, size_t size);
const char* tbufReadString(SBuffer* buf, size_t* len);
size_t tbufReadToString(SBuffer* buf, char* dst, size_t size);


// basic write functions
void tbufInitWrite(SBuffer* buf, size_t size);
void tbufEnsureCapacity(SBuffer* buf, size_t size);
char* tbufGetResult(SBuffer* buf, bool takeOver);
void tbufUninitWrite(SBuffer* buf);
void tbufWrite(SBuffer* buf, const void* data, size_t size);
void tbufWriteAt(SBuffer* buf, size_t pos, const void* data, size_t size);
void tbufWriteStringLen(SBuffer* buf, const char* str, size_t len);
void tbufWriteString(SBuffer* buf, const char* str);


// read & write function for primitive types
#ifndef TBUFFER_DEFINE_OPERATION
#define TBUFFER_DEFINE_OPERATION(type, name) \
  type tbufRead##name(SBuffer* buf); \
  void tbufWrite##name(SBuffer* buf, type data); \
  void tbufWrite##name##At(SBuffer* buf, size_t pos, type data);
#endif

TBUFFER_DEFINE_OPERATION( bool, Bool )
TBUFFER_DEFINE_OPERATION( char, Char )
TBUFFER_DEFINE_OPERATION( int8_t, Int8 )
TBUFFER_DEFINE_OPERATION( uint8_t, Unt8 )
TBUFFER_DEFINE_OPERATION( int16_t, Int16 )
TBUFFER_DEFINE_OPERATION( uint16_t, Uint16 )
TBUFFER_DEFINE_OPERATION( int32_t, Int32 )
TBUFFER_DEFINE_OPERATION( uint32_t, Uint32 )
TBUFFER_DEFINE_OPERATION( int64_t, Int64 )
TBUFFER_DEFINE_OPERATION( uint64_t, Uint64 )
TBUFFER_DEFINE_OPERATION( float, Float )
TBUFFER_DEFINE_OPERATION( double, Double )

#endif
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

#ifndef TDENGINE_TBUFFER_H
#define TDENGINE_TBUFFER_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
// SBuffer can be used to read or write a buffer, but cannot be used for both
// read & write at a same time. Below is an example:
#include <stdio.h>
#include <stdlib.h>
#include "exception.h"
#include "tbuffer.h"

int foo() {
  SBuffer wbuf, rbuf;
  tbufSetup(&wbuf, NULL, false);
  tbufSetup(&rbuf, NULL, false);

  TRY {
    //--------------------- write ------------------------
    tbufBeginWrite(&wbuf);
    // reserve 1024 bytes for the buffer to improve performance
    tbufEnsureCapacity(&wbuf, 1024);
    // write 5 integers to the buffer
    for (int i = 0; i < 5; i++) {
      tbufWriteInt32(&wbuf, i);
    }
    // write a string to the buffer
    tbufWriteString(&wbuf, "this is a string.\n");
    // acquire the result and close the write buffer
    size_t size = tbufTell(&wbuf);
    char*  data = tbufGetData(&wbuf, true);

    //------------------------ read -----------------------
    tbufBeginRead(&rbuf, data, size);
    // read & print out 5 integers
    for (int i = 0; i < 5; i++) {
      printf("%d\n", tbufReadInt32(&rbuf));
    }
    // read & print out a string
    puts(tbufReadString(&rbuf, NULL));
    // try read another integer, this result in an error as there no this integer
    tbufReadInt32(&rbuf);
    printf("you should not see this message.\n");
  } CATCH( code ) {
    printf("exception code is: %d, you will see this message after print out 5 integers and a string.\n", code);
    THROW( code );
  } END_CATCH

  tbufClose(&wbuf, true);
  tbufClose(&rbuf, false);
  return 0;
}

int main(int argc, char** argv) {
    TRY {
        printf("in main: you will see this line\n");
        foo();
        printf("in main: you will not see this line\n");
    } CATCH( code ) {
        printf("foo raise an exception with code %d\n", code);
    } END_CATCH

    return 0;
}
*/

typedef struct {
  void*   (*allocator)(void*, size_t);
  bool    endian;
  char*   data;
  size_t  pos;
  size_t  size;
} SBuffer;

// common functions can be used in both read & write

// tbufSetup setup the buffer, should be called before tbufBeginRead / tbufBeginWrite
// *allocator*, function to allocate memory, will use 'realloc' if NULL
// *endian*, if true, read/write functions of primitive types will do 'ntoh' or 'hton' automatically
void   tbufSetup(SBuffer* buf, void* (*allocator)(void*, size_t), bool endian);
size_t tbufTell(SBuffer* buf);
size_t tbufSeekTo(SBuffer* buf, size_t pos);
void   tbufClose(SBuffer* buf, bool keepData);

// basic read functions
void        tbufBeginRead(SBuffer* buf, void* data, size_t len);
size_t      tbufSkip(SBuffer* buf, size_t size);
char*       tbufRead(SBuffer* buf, size_t size);
void        tbufReadToBuffer(SBuffer* buf, void* dst, size_t size);
const char* tbufReadString(SBuffer* buf, size_t* len);
size_t      tbufReadToString(SBuffer* buf, char* dst, size_t size);
const char* tbufReadBinary(SBuffer* buf, size_t *len);
size_t      tbufReadToBinary(SBuffer* buf, void* dst, size_t size);

// basic write functions
void        tbufBeginWrite(SBuffer* buf);
void        tbufEnsureCapacity(SBuffer* buf, size_t size);
size_t      tbufReserve(SBuffer* buf, size_t size);
char*       tbufGetData(SBuffer* buf, bool takeOver);
void        tbufWrite(SBuffer* buf, const void* data, size_t size);
void        tbufWriteAt(SBuffer* buf, size_t pos, const void* data, size_t size);
void        tbufWriteStringLen(SBuffer* buf, const char* str, size_t len);
void        tbufWriteString(SBuffer* buf, const char* str);
// the prototype of WriteBinary and Write is identical
// the difference is: WriteBinary writes the length of the data to the buffer
// first, then the actual data, which means the reader don't need to know data
// size before read. Write only write the data itself, which means the reader
// need to know data size before read.
void        tbufWriteBinary(SBuffer* buf, const void* data, size_t len);

// read / write functions for primitive types
bool tbufReadBool(SBuffer* buf);
void tbufWriteBool(SBuffer* buf, bool data);
void tbufWriteBoolAt(SBuffer* buf, size_t pos, bool data);

char tbufReadChar(SBuffer* buf);
void tbufWriteChar(SBuffer* buf, char data);
void tbufWriteCharAt(SBuffer* buf, size_t pos, char data);

int8_t tbufReadInt8(SBuffer* buf);
void tbufWriteInt8(SBuffer* buf, int8_t data);
void tbufWriteInt8At(SBuffer* buf, size_t pos, int8_t data);

uint8_t tbufReadUint8(SBuffer* buf);
void tbufWriteUint8(SBuffer* buf, uint8_t data);
void tbufWriteUint8At(SBuffer* buf, size_t pos, uint8_t data);

int16_t tbufReadInt16(SBuffer* buf);
void tbufWriteInt16(SBuffer* buf, int16_t data);
void tbufWriteInt16At(SBuffer* buf, size_t pos, int16_t data);

uint16_t tbufReadUint16(SBuffer* buf);
void tbufWriteUint16(SBuffer* buf, uint16_t data);
void tbufWriteUint16At(SBuffer* buf, size_t pos, uint16_t data);

int32_t tbufReadInt32(SBuffer* buf);
void tbufWriteInt32(SBuffer* buf, int32_t data);
void tbufWriteInt32At(SBuffer* buf, size_t pos, int32_t data);

uint32_t tbufReadUint32(SBuffer* buf);
void tbufWriteUint32(SBuffer* buf, uint32_t data);
void tbufWriteUint32At(SBuffer* buf, size_t pos, uint32_t data);

int64_t tbufReadInt64(SBuffer* buf);
void tbufWriteInt64(SBuffer* buf, int64_t data);
void tbufWriteInt64At(SBuffer* buf, size_t pos, int64_t data);

uint64_t tbufReadUint64(SBuffer* buf);
void tbufWriteUint64(SBuffer* buf, uint64_t data);
void tbufWriteUint64At(SBuffer* buf, size_t pos, uint64_t data);

float tbufReadFloat(SBuffer* buf);
void tbufWriteFloat(SBuffer* buf, float data);
void tbufWriteFloatAt(SBuffer* buf, size_t pos, float data);

double tbufReadDouble(SBuffer* buf);
void tbufWriteDouble(SBuffer* buf, double data);
void tbufWriteDoubleAt(SBuffer* buf, size_t pos, double data);

#ifdef __cplusplus
}
#endif

#endif

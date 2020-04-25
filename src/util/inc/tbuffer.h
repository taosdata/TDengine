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

#include "setjmp.h"
#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
SBuffer can be used to read or write a buffer, but cannot be used for both
read & write at a same time. Below is an example:

int main(int argc, char** argv) {
  //--------------------- write ------------------------
  SBuffer wbuf;
  int32_t code = tbufBeginWrite(&wbuf);
  if (code != 0) {
    // handle errors
    return 0;
  }

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
  tbufClose(&wbuf, true);


  //------------------------ read -----------------------
  SBuffer rbuf;
  code = tbufBeginRead(&rbuf, data, size);
  if (code != 0) {
    printf("you will see this message after print out 5 integers and a string.\n");
    tbufClose(&rbuf, false);
    return 0;
  }

  // read & print out 5 integers
  for (int i = 0; i < 5; i++) {
    printf("%d\n", tbufReadInt32(&rbuf));
  }

  // read & print out a string
  printf(tbufReadString(&rbuf, NULL));

  // try read another integer, this result in an error as there no this integer
  tbufReadInt32(&rbuf);

  printf("you should not see this message.\n");
  tbufClose(&rbuf, false);

  return 0;
}
*/
typedef struct {
  jmp_buf jb;
  char*   data;
  size_t  pos;
  size_t  size;
} SBuffer;

// common functions can be used in both read & write
#define tbufThrowError(buf, code) longjmp((buf)->jb, (code))
size_t tbufTell(SBuffer* buf);
size_t tbufSeekTo(SBuffer* buf, size_t pos);
size_t tbufSkip(SBuffer* buf, size_t size);
void   tbufClose(SBuffer* buf, bool keepData);

// basic read functions
#define tbufBeginRead(buf, _data, len) ((buf)->data = (char*)(_data), ((buf)->pos = 0), ((buf)->size = ((_data) == NULL) ? 0 : (len)), setjmp((buf)->jb))
char*       tbufRead(SBuffer* buf, size_t size);
void        tbufReadToBuffer(SBuffer* buf, void* dst, size_t size);
const char* tbufReadString(SBuffer* buf, size_t* len);
size_t      tbufReadToString(SBuffer* buf, char* dst, size_t size);

// basic write functions
#define tbufBeginWrite(buf) ((buf)->data = NULL, ((buf)->pos = 0), ((buf)->size = 0), setjmp((buf)->jb))
void  tbufEnsureCapacity(SBuffer* buf, size_t size);
char* tbufGetData(SBuffer* buf, bool takeOver);
void  tbufWrite(SBuffer* buf, const void* data, size_t size);
void  tbufWriteAt(SBuffer* buf, size_t pos, const void* data, size_t size);
void  tbufWriteStringLen(SBuffer* buf, const char* str, size_t len);
void  tbufWriteString(SBuffer* buf, const char* str);

// read & write function for primitive types
#ifndef TBUFFER_DEFINE_FUNCTION
#define TBUFFER_DEFINE_FUNCTION(type, name) \
  type tbufRead##name(SBuffer* buf); \
  void tbufWrite##name(SBuffer* buf, type data); \
  void tbufWrite##name##At(SBuffer* buf, size_t pos, type data);
#endif

TBUFFER_DEFINE_FUNCTION(bool, Bool)
TBUFFER_DEFINE_FUNCTION(char, Char)
TBUFFER_DEFINE_FUNCTION(int8_t, Int8)
TBUFFER_DEFINE_FUNCTION(uint8_t, Uint8)
TBUFFER_DEFINE_FUNCTION(int16_t, Int16)
TBUFFER_DEFINE_FUNCTION(uint16_t, Uint16)
TBUFFER_DEFINE_FUNCTION(int32_t, Int32)
TBUFFER_DEFINE_FUNCTION(uint32_t, Uint32)
TBUFFER_DEFINE_FUNCTION(int64_t, Int64)
TBUFFER_DEFINE_FUNCTION(uint64_t, Uint64)
TBUFFER_DEFINE_FUNCTION(float, Float)
TBUFFER_DEFINE_FUNCTION(double, Double)

#ifdef __cplusplus
}
#endif

#endif
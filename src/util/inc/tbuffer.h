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

typedef struct {
  bool endian;
  const char* data;
  size_t pos;
  size_t size;
} SBufferReader;

typedef struct {
  bool endian;
  char* data;
  size_t pos;
  size_t size;
  void* (*allocator)( void*, size_t );
} SBufferWriter;

////////////////////////////////////////////////////////////////////////////////
// common functions & macros for both reader & writer
#define tbufTell( buf ) ((buf)->pos)


////////////////////////////////////////////////////////////////////////////////
// reader functions & macros

// *Endian*, if true, reader functions of primitive types will do 'ntoh' automatically
#define tbufInitReader( Data, Size, Endian ) {.endian = (Endian), .data = (Data), .pos = 0, .size = ((Data) == NULL ? 0 :(Size))}

size_t      tbufSkip( SBufferReader* buf, size_t size );

char*       tbufRead( SBufferReader* buf, size_t size );
void        tbufReadToBuffer( SBufferReader* buf, void* dst, size_t size );
const char* tbufReadString( SBufferReader* buf, size_t* len );
size_t      tbufReadToString( SBufferReader* buf, char* dst, size_t size );
const char* tbufReadBinary( SBufferReader* buf, size_t *len );
size_t      tbufReadToBinary( SBufferReader* buf, void* dst, size_t size );

bool     tbufReadBool( SBufferReader* buf );
char     tbufReadChar( SBufferReader* buf );
int8_t   tbufReadInt8( SBufferReader* buf );
uint8_t  tbufReadUint8( SBufferReader* buf );
int16_t  tbufReadInt16( SBufferReader* buf );
uint16_t tbufReadUint16( SBufferReader* buf );
int32_t  tbufReadInt32( SBufferReader* buf );
uint32_t tbufReadUint32( SBufferReader* buf );
int64_t  tbufReadInt64( SBufferReader* buf );
uint64_t tbufReadUint64( SBufferReader* buf );
float    tbufReadFloat( SBufferReader* buf );
double   tbufReadDouble( SBufferReader* buf );


////////////////////////////////////////////////////////////////////////////////
// writer functions & macros

// *Allocator*, function to allocate memory, will use 'realloc' if NULL
// *Endian*, if true, writer functions of primitive types will do 'hton' automatically
#define tbufInitWriter( Allocator, Endian ) {.endian = (Endian), .data = NULL, .pos = 0, .size = 0, .allocator = ((Allocator) == NULL ? realloc : (Allocator))}
void   tbufCloseWriter( SBufferWriter* buf );

void   tbufEnsureCapacity( SBufferWriter* buf, size_t size );
size_t tbufReserve( SBufferWriter* buf, size_t size );
char*  tbufGetData( SBufferWriter* buf, bool takeOver );

void   tbufWrite( SBufferWriter* buf, const void* data, size_t size );
void   tbufWriteAt( SBufferWriter* buf, size_t pos, const void* data, size_t size );
void   tbufWriteStringLen( SBufferWriter* buf, const char* str, size_t len );
void   tbufWriteString( SBufferWriter* buf, const char* str );
// the prototype of tbufWriteBinary and tbufWrite are identical
// the difference is: tbufWriteBinary writes the length of the data to the buffer
// first, then the actual data, which means the reader don't need to know data
// size before read. Write only write the data itself, which means the reader
// need to know data size before read.
void   tbufWriteBinary( SBufferWriter* buf, const void* data, size_t len );

void tbufWriteBool( SBufferWriter* buf, bool data );
void tbufWriteBoolAt( SBufferWriter* buf, size_t pos, bool data );
void tbufWriteChar( SBufferWriter* buf, char data );
void tbufWriteCharAt( SBufferWriter* buf, size_t pos, char data );
void tbufWriteInt8( SBufferWriter* buf, int8_t data );
void tbufWriteInt8At( SBufferWriter* buf, size_t pos, int8_t data );
void tbufWriteUint8( SBufferWriter* buf, uint8_t data );
void tbufWriteUint8At( SBufferWriter* buf, size_t pos, uint8_t data );
void tbufWriteInt16( SBufferWriter* buf, int16_t data );
void tbufWriteInt16At( SBufferWriter* buf, size_t pos, int16_t data );
void tbufWriteUint16( SBufferWriter* buf, uint16_t data );
void tbufWriteUint16At( SBufferWriter* buf, size_t pos, uint16_t data );
void tbufWriteInt32( SBufferWriter* buf, int32_t data );
void tbufWriteInt32At( SBufferWriter* buf, size_t pos, int32_t data );
void tbufWriteUint32( SBufferWriter* buf, uint32_t data );
void tbufWriteUint32At( SBufferWriter* buf, size_t pos, uint32_t data );
void tbufWriteInt64( SBufferWriter* buf, int64_t data );
void tbufWriteInt64At( SBufferWriter* buf, size_t pos, int64_t data );
void tbufWriteUint64( SBufferWriter* buf, uint64_t data );
void tbufWriteUint64At( SBufferWriter* buf, size_t pos, uint64_t data );
void tbufWriteFloat( SBufferWriter* buf, float data );
void tbufWriteFloatAt( SBufferWriter* buf, size_t pos, float data );
void tbufWriteDouble( SBufferWriter* buf, double data );
void tbufWriteDoubleAt( SBufferWriter* buf, size_t pos, double data );

#ifdef __cplusplus
}
#endif

#endif

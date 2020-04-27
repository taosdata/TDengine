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

#include <stdlib.h>
#include <memory.h>
#include <assert.h>
#include <arpa/inet.h>
#include "tbuffer.h"
#include "exception.h"
#include <taoserror.h>

////////////////////////////////////////////////////////////////////////////////
// reader functions

size_t tbufSkip(SBufferReader* buf, size_t size) {
  if( (buf->pos + size) > buf->size ) {
    THROW( TSDB_CODE_MEMORY_CORRUPTED );
  }
  size_t old = buf->pos;
  buf->pos += size;
  return old;
}

const char* tbufRead( SBufferReader* buf, size_t size ) {
  const char* ret = buf->data + buf->pos;
  tbufSkip( buf, size );
  return ret;
}

void tbufReadToBuffer( SBufferReader* buf, void* dst, size_t size ) {
  assert( dst != NULL );
  // always using memcpy, leave optimization to compiler
  memcpy( dst, tbufRead(buf, size), size );
}

static size_t tbufReadLength( SBufferReader* buf ) {
  // maximum length is 65535, if larger length is required
  // this function and the corresponding write function need to be
  // revised.
  uint16_t l = tbufReadUint16( buf );
  return l;
}

const char* tbufReadString( SBufferReader* buf, size_t* len ) {
  size_t l = tbufReadLength( buf );
  const char* ret = buf->data + buf->pos;
  tbufSkip( buf, l + 1 );
  if( ret[l] != 0 ) {
    THROW( TSDB_CODE_MEMORY_CORRUPTED );
  }
  if( len != NULL ) {
    *len = l;
  }
  return ret;
}

size_t tbufReadToString( SBufferReader* buf, char* dst, size_t size ) {
  assert( dst != NULL );
  size_t len;
  const char* str = tbufReadString( buf, &len );
  if (len >= size) {
    len = size - 1;
  }
  memcpy( dst, str, len );
  dst[len] = 0;
  return len;
}

const char* tbufReadBinary( SBufferReader* buf, size_t *len ) {
  size_t l = tbufReadLength( buf );
  const char* ret = buf->data + buf->pos;
  tbufSkip( buf, l );
  if( len != NULL ) {
    *len = l;
  }
  return ret;
}

size_t tbufReadToBinary( SBufferReader* buf, void* dst, size_t size ) {
  assert( dst != NULL );
  size_t len;
  const char* data = tbufReadBinary( buf, &len );
  if( len >= size ) {
    len = size;
  }
  memcpy( dst, data, len );
  return len;
}

bool tbufReadBool( SBufferReader* buf ) {
  bool ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  return ret;
}

char tbufReadChar( SBufferReader* buf ) {
  char ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  return ret;
}

int8_t tbufReadInt8( SBufferReader* buf ) {
  int8_t ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  return ret;
}

uint8_t tbufReadUint8( SBufferReader* buf ) {
  uint8_t ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  return ret;
}

int16_t tbufReadInt16( SBufferReader* buf ) {
  int16_t ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  if( buf->endian ) {
    return (int16_t)ntohs( ret );
  }
  return ret;
}

uint16_t tbufReadUint16( SBufferReader* buf ) {
  uint16_t ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  if( buf->endian ) {
    return ntohs( ret );
  }
  return ret;
}

int32_t tbufReadInt32( SBufferReader* buf ) {
  int32_t ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  if( buf->endian ) {
    return (int32_t)ntohl( ret );
  }
  return ret;
}

uint32_t tbufReadUint32( SBufferReader* buf ) {
  uint32_t ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  if( buf->endian ) {
    return ntohl( ret );
  }
  return ret;
}

int64_t tbufReadInt64( SBufferReader* buf ) {
  int64_t ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  if( buf->endian ) {
    return (int64_t)htobe64( ret ); // TODO: ntohll
  }
  return ret;
}

uint64_t tbufReadUint64( SBufferReader* buf ) {
  uint64_t ret;
  tbufReadToBuffer( buf, &ret, sizeof(ret) );
  if( buf->endian ) {
    return htobe64( ret ); // TODO: ntohll
  }
  return ret;
}

float tbufReadFloat( SBufferReader* buf ) {
  uint32_t ret = tbufReadUint32( buf );
  return *(float*)( &ret );
}

double tbufReadDouble(SBufferReader* buf) {
  uint64_t ret = tbufReadUint64( buf );
  return *(double*)( &ret );
}

////////////////////////////////////////////////////////////////////////////////
// writer functions

void tbufCloseWriter( SBufferWriter* buf ) {
  (*buf->allocator)( buf->data, 0 );
  buf->data = NULL;
  buf->pos = 0;
  buf->size = 0;
}

void tbufEnsureCapacity( SBufferWriter* buf, size_t size ) {
  size += buf->pos;
  if( size > buf->size ) {
    size_t nsize = size + buf->size;
    char* data = (*buf->allocator)( buf->data, nsize );
    // TODO: the exception should be thrown by the allocator function
    if( data == NULL ) {
      THROW( TSDB_CODE_SERV_OUT_OF_MEMORY );
    }
    buf->data = data;
    buf->size = nsize;
  }
}

size_t tbufReserve( SBufferWriter* buf, size_t size ) {
  tbufEnsureCapacity( buf, size );
  size_t old = buf->pos;
  buf->pos += size;
  return old;
}

char* tbufGetData( SBufferWriter* buf, bool takeOver ) {
  char* ret = buf->data;
  if( takeOver ) {
    buf->pos = 0;
    buf->size = 0;
    buf->data = NULL;
  }
  return ret;
}

void tbufWrite( SBufferWriter* buf, const void* data, size_t size ) {
  assert( data != NULL );
  tbufEnsureCapacity( buf, size );
  memcpy( buf->data + buf->pos, data, size );
  buf->pos += size;
}

void tbufWriteAt( SBufferWriter* buf, size_t pos, const void* data, size_t size ) {
  assert( data != NULL );
  // this function can only be called to fill the gap on previous writes,
  // so 'pos + size <= buf->pos' must be true
  assert( pos + size <= buf->pos );
  memcpy( buf->data + pos, data, size );
}

static void tbufWriteLength( SBufferWriter* buf, size_t len ) {
  // maximum length is 65535, if larger length is required
  // this function and the corresponding read function need to be
  // revised.
  assert( len <= 0xffff );
  tbufWriteUint16( buf, (uint16_t)len );
}

void tbufWriteStringLen( SBufferWriter* buf, const char* str, size_t len ) {
  tbufWriteLength( buf, len );
  tbufWrite( buf, str, len );
  tbufWriteChar( buf, '\0' );
}

void tbufWriteString( SBufferWriter* buf, const char* str ) {
  tbufWriteStringLen( buf, str, strlen(str) );
}

void tbufWriteBinary( SBufferWriter* buf, const void* data, size_t len ) {
  tbufWriteLength( buf, len );
  tbufWrite( buf, data, len );
}

void tbufWriteBool( SBufferWriter* buf, bool data ) {
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteBoolAt( SBufferWriter* buf, size_t pos, bool data ) {
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteChar( SBufferWriter* buf, char data ) {
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteCharAt( SBufferWriter* buf, size_t pos, char data ) {
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteInt8( SBufferWriter* buf, int8_t data ) {
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteInt8At( SBufferWriter* buf, size_t pos, int8_t data ) {
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteUint8( SBufferWriter* buf, uint8_t data ) {
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteUint8At( SBufferWriter* buf, size_t pos, uint8_t data ) {
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteInt16( SBufferWriter* buf, int16_t data ) {
  if( buf->endian ) {
    data = (int16_t)htons( data );
  }
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteInt16At( SBufferWriter* buf, size_t pos, int16_t data ) {
  if( buf->endian ) {
    data = (int16_t)htons( data );
  }
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteUint16( SBufferWriter* buf, uint16_t data ) {
  if( buf->endian ) {
    data = htons( data );
  }
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteUint16At( SBufferWriter* buf, size_t pos, uint16_t data ) {
  if( buf->endian ) {
    data = htons( data );
  }
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteInt32( SBufferWriter* buf, int32_t data ) {
  if( buf->endian ) {
    data = (int32_t)htonl( data );
  }
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteInt32At( SBufferWriter* buf, size_t pos, int32_t data ) {
  if( buf->endian ) {
    data = (int32_t)htonl( data );
  }
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteUint32( SBufferWriter* buf, uint32_t data ) {
  if( buf->endian ) {
    data = htonl( data );
  }
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteUint32At( SBufferWriter* buf, size_t pos, uint32_t data ) {
  if( buf->endian ) {
    data = htonl( data );
  }
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteInt64( SBufferWriter* buf, int64_t data ) {
  if( buf->endian ) {
    data = (int64_t)htobe64( data );
  }
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteInt64At( SBufferWriter* buf, size_t pos, int64_t data ) {
  if( buf->endian ) {
    data = (int64_t)htobe64( data );
  }
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteUint64( SBufferWriter* buf, uint64_t data ) {
  if( buf->endian ) {
    data = htobe64( data );
  }
  tbufWrite( buf, &data, sizeof(data) );
}

void tbufWriteUint64At( SBufferWriter* buf, size_t pos, uint64_t data ) {
  if( buf->endian ) {
    data = htobe64( data );
  }
  tbufWriteAt( buf, pos, &data, sizeof(data) );
}

void tbufWriteFloat( SBufferWriter* buf, float data ) {
  tbufWriteUint32( buf, *(uint32_t*)(&data) );
}

void tbufWriteFloatAt( SBufferWriter* buf, size_t pos, float data ) {
  tbufWriteUint32At( buf, pos, *(uint32_t*)(&data) );
}

void tbufWriteDouble( SBufferWriter* buf, double data ) {
  tbufWriteUint64( buf, *(uint64_t*)(&data) );
}

void tbufWriteDoubleAt( SBufferWriter* buf, size_t pos, double data ) {
  tbufWriteUint64At( buf, pos, *(uint64_t*)(&data) );
}

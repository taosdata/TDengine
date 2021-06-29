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

#include "os.h"
#include "tbuffer.h"
#include "exception.h"
#include "taoserror.h"

////////////////////////////////////////////////////////////////////////////////
// reader functions

size_t tbufSkip(SBufferReader* buf, size_t size) {
  if( (buf->pos + size) > buf->size ) {
    THROW( TSDB_CODE_COM_MEMORY_CORRUPTED );
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

// need not memcpy, just get the value directly
#define DIRECT_READ(_buf, _type)  *((_type*)(tbufRead(_buf, sizeof(_type))))

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
    THROW( TSDB_CODE_COM_MEMORY_CORRUPTED );
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
  return DIRECT_READ(buf, bool);
}

char tbufReadChar( SBufferReader* buf ) {
  return DIRECT_READ(buf, char);
}

int8_t tbufReadInt8( SBufferReader* buf ) {
  return DIRECT_READ(buf, int8_t);
}

uint8_t tbufReadUint8( SBufferReader* buf ) {
  return DIRECT_READ(buf, uint8_t);
}

int16_t tbufReadInt16( SBufferReader* buf ) {
  if (buf->endian)
  {
    return ((int16_t)(ntohs(DIRECT_READ(buf, uint16_t))));
  }
  else
  {
    return DIRECT_READ(buf, int16_t);
  }
}

uint16_t tbufReadUint16( SBufferReader* buf ) {
  return (buf->endian) ? ntohs(DIRECT_READ(buf, uint16_t)) : DIRECT_READ(buf, uint16_t);
}

int32_t tbufReadInt32( SBufferReader* buf ) {
  return (buf->endian) ? ((int32_t)(ntohl(DIRECT_READ(buf, uint32_t)))) : DIRECT_READ(buf, int32_t);
}

uint32_t tbufReadUint32( SBufferReader* buf ) {
  return (buf->endian) ? ntohl(DIRECT_READ(buf, uint32_t)) : DIRECT_READ(buf, uint32_t);
}

int64_t tbufReadInt64( SBufferReader* buf ) {
  return (buf->endian) ? ((int64_t)(htobe64(DIRECT_READ(buf, uint64_t)))) : DIRECT_READ(buf, int64_t);
}

uint64_t tbufReadUint64( SBufferReader* buf ) {
  return (buf->endian) ? htobe64(DIRECT_READ(buf, uint64_t)) : DIRECT_READ(buf, uint64_t);
}

float tbufReadFloat( SBufferReader* buf ) {
  if (buf->endian)
  {
    Un4B _4b;
    _4b.ui = ntohl(DIRECT_READ(buf, uint32_t));
    return _4b.f;
  }
  else
  {
    return DIRECT_READ(buf, float);
  }
}

double tbufReadDouble(SBufferReader* buf) {
  if (buf->endian)
  {
    Un8B _8b;
    _8b.ull = htobe64(DIRECT_READ(buf, uint64_t));
    return _8b.d;
  }
  else
  {
    return DIRECT_READ(buf, double);
  }
}

#undef DIRECT_READ

////////////////////////////////////////////////////////////////////////////////
// writer functions

void tbufCloseWriter( SBufferWriter* buf ) {
  tfree(buf->data);
//  (*buf->allocator)( buf->data, 0 );  // potential memory leak.
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
      THROW( TSDB_CODE_COM_OUT_OF_MEMORY );
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

// need not memcpy, just write the value directly
#if defined(__GNUC__)

#define WRITE_WITHOUT_COPY(_buf, _data, _type)  *((typeof(_data)*)(_buf)) = (_data)

#define DIRECT_WRITE(_buf, _data, _type)  \
  tbufEnsureCapacity((_buf), sizeof(_data));  \
  WRITE_WITHOUT_COPY((_buf)->data + (_buf)->pos, _data, _type);  \
  (_buf)->pos += sizeof(_data);

#define DIRECT_WRITE_AT(_buf, _pos, _data, _type)  \
  assert(((_pos) + sizeof(_data)) <= (_buf)->pos);  \
  WRITE_WITHOUT_COPY((_buf)->data + (_pos), _data, _type);

#else

#define WRITE_WITHOUT_COPY(_buf, _data, _type)  *((_type*)(_buf)) = (_data)

#define DIRECT_WRITE(_buf, _data, _type)  \
  tbufEnsureCapacity((_buf), sizeof(_data));  \
  WRITE_WITHOUT_COPY((_buf)->data + (_buf)->pos, _data, _type);  \
  (_buf)->pos += sizeof(_data);

#define DIRECT_WRITE_AT(_buf, _pos, _data, _type)  \
  assert(((_pos) + sizeof(_data)) <= (_buf)->pos);  \
  WRITE_WITHOUT_COPY((_buf)->data + (_pos), _data, _type);

#endif

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
  DIRECT_WRITE(buf, data, bool)
}

void tbufWriteBoolAt( SBufferWriter* buf, size_t pos, bool data ) {
  DIRECT_WRITE_AT(buf, pos, data, bool)
}

void tbufWriteChar( SBufferWriter* buf, char data ) {
  DIRECT_WRITE(buf, data, char)
}

void tbufWriteCharAt( SBufferWriter* buf, size_t pos, char data ) {
  DIRECT_WRITE_AT(buf, pos, data, char)
}

void tbufWriteInt8( SBufferWriter* buf, int8_t data ) {
  DIRECT_WRITE(buf, data, int8_t)
}

void tbufWriteInt8At( SBufferWriter* buf, size_t pos, int8_t data ) {
  DIRECT_WRITE_AT(buf, pos, data, int8_t)
}

void tbufWriteUint8( SBufferWriter* buf, uint8_t data ) {
  DIRECT_WRITE(buf, data, uint8_t)
}

void tbufWriteUint8At( SBufferWriter* buf, size_t pos, uint8_t data ) {
  DIRECT_WRITE_AT(buf, pos, data, uint8_t)
}

void tbufWriteInt16( SBufferWriter* buf, int16_t data ) {
  DIRECT_WRITE(buf, (buf->endian) ? ((int16_t)(ntohs(data))) : data, int16_t)
}

void tbufWriteInt16At( SBufferWriter* buf, size_t pos, int16_t data ) {
  DIRECT_WRITE_AT(buf, pos, (buf->endian) ? ((int16_t)(ntohs(data))) : data, int16_t)
}

void tbufWriteUint16( SBufferWriter* buf, uint16_t data ) {
  DIRECT_WRITE(buf, (buf->endian) ? ntohs(data) : data, uint16_t)
}

void tbufWriteUint16At( SBufferWriter* buf, size_t pos, uint16_t data ) {
  DIRECT_WRITE_AT(buf, pos, (buf->endian) ? ntohs(data) : data, uint16_t)
}

void tbufWriteInt32( SBufferWriter* buf, int32_t data ) {
  DIRECT_WRITE(buf, (buf->endian) ? ((int32_t)(ntohl(data))) : data, int32_t)
}

void tbufWriteInt32At( SBufferWriter* buf, size_t pos, int32_t data ) {
  DIRECT_WRITE_AT(buf, pos, (buf->endian) ? ((int32_t)(ntohl(data))) : data, int32_t)
}

void tbufWriteUint32( SBufferWriter* buf, uint32_t data ) {
  DIRECT_WRITE(buf, (buf->endian) ? ntohl(data) : data, uint32_t)
}

void tbufWriteUint32At( SBufferWriter* buf, size_t pos, uint32_t data ) {
  DIRECT_WRITE_AT(buf, pos, (buf->endian) ? ntohl(data) : data, uint32_t)
}

void tbufWriteInt64( SBufferWriter* buf, int64_t data ) {
  DIRECT_WRITE(buf, (buf->endian) ? ((int64_t)(htobe64(data))) : data, int64_t)
}

void tbufWriteInt64At( SBufferWriter* buf, size_t pos, int64_t data ) {
  DIRECT_WRITE_AT(buf, pos, (buf->endian) ? ((int64_t)(htobe64(data))) : data, int64_t)
}

void tbufWriteUint64( SBufferWriter* buf, uint64_t data ) {
  DIRECT_WRITE(buf, (buf->endian) ? htobe64(data) : data, uint64_t)
}

void tbufWriteUint64At( SBufferWriter* buf, size_t pos, uint64_t data ) {
  DIRECT_WRITE_AT(buf, pos, (buf->endian) ? htobe64(data) : data, uint64_t)
}

void tbufWriteFloat( SBufferWriter* buf, float data ) {
  if (buf->endian)
  {
    DIRECT_WRITE(buf, ntohl(((Un4B*)(&data))->ui), uint32_t)
  }
  else
  {
    DIRECT_WRITE(buf, data, float)
  }
}

void tbufWriteFloatAt( SBufferWriter* buf, size_t pos, float data ) {
  if (buf->endian)
  {
    DIRECT_WRITE_AT(buf, pos, ntohl(((Un4B*)(&data))->ui), uint32_t)
  }
  else
  {
    DIRECT_WRITE_AT(buf, pos, data, float)
  }
}

void tbufWriteDouble( SBufferWriter* buf, double data ) {
  if (buf->endian)
  {
    DIRECT_WRITE(buf, htobe64(((Un8B*)(&data))->ull), uint64_t)
  }
  else
  {
    DIRECT_WRITE(buf, data, double )
  }
}

void tbufWriteDoubleAt( SBufferWriter* buf, size_t pos, double data ) {
  if (buf->endian)
  {
    DIRECT_WRITE_AT(buf, pos, htobe64(((Un8B*)(&data))->ull), uint64_t)
  }
  else
  {
    DIRECT_WRITE_AT(buf, pos, data, double)
  }
}

#undef DIRECT_WRITE_AT
#undef DIRECT_WRITE
#undef WRITE_WITHOUT_COPY

/** Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

// clang-format off
#include "transSasl.h"
// clang-format on


extern void saslLibInitImpl(); 
extern void saslLibCleanupImpl();
extern int32_t saslConnCreateImpl(SSaslConn** ppConn, int8_t server);
    
extern int32_t saslConnInitImpl(SSaslConn* pConn);

extern void    saslConnCleanupImpl(SSaslConn* pConn);
extern void saslConnSetStateImpl(SSaslConn* pConn, int32_t state);
extern int32_t saslConnEncodeImpl(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen);
extern int32_t saslConnDecodeImpl(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen);
extern int32_t saslConnHandleAuthImpl(SSaslConn* pConn, const char* input, int32_t len);

extern int8_t  saslConnShoudDoAuthImpl(SSaslConn* pConn);
// int32_t saslConnStartAuth(SSaslConn* pConn);

// sasl buffer func
extern int32_t saslBufferInitImpl(SSaslBuffer* buf, int32_t cap);
extern int32_t saslBufferAppendImpl(SSaslBuffer* buf, uint8_t* data, int32_t len);
extern void    saslBufferCleanupImpl(SSaslBuffer* buf);
extern void    saslBufferClearImpl(SSaslBuffer* buf);

void saslLibInit() {
   saslLibInitImpl();   
}

void saslLibCleanup() {
    saslLibCleanupImpl();    
}

int32_t saslConnCreate(SSaslConn** ppConn, int8_t server) {
  return saslConnCreateImpl(ppConn, server);
}

void saslConnSetState(SSaslConn* pConn, int32_t state) {
  saslConnSetStateImpl(pConn, state);
}


int32_t saslConnInit(SSaslConn* pConn) {
  return saslConnInitImpl(pConn);
}

void saslConnCleanup(SSaslConn* pConn) {
  saslConnCleanupImpl(pConn);
}

int32_t saslConnEncode(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen) {
  return saslConnEncodeImpl(pConn, input, len, output, outputLen);
}

int32_t saslConnDecode(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen) {
  return saslConnDecodeImpl(pConn, input, len, output, outputLen);
}

int8_t saslAuthIsInited(SSaslConn* pConn) {
  return saslConnShoudDoAuthImpl(pConn);
}


int32_t saslConnHandleAuth(SSaslConn* pConn, const char* input, int32_t len) {
  return saslConnHandleAuthImpl(pConn, input, len);
}


int32_t saslBufferInit(SSaslBuffer* buf, int32_t cap) {
  return saslBufferInitImpl(buf, cap);  
}

int32_t saslBufferAppend(SSaslBuffer* buf, uint8_t* data, int32_t len) {
  return saslBufferAppendImpl(buf, data, len);
}

void saslBufferCleanup(SSaslBuffer* buf) {
  return saslBufferCleanupImpl(buf);  
}

void saslBufferClear(SSaslBuffer* buf) {
  saslBufferClearImpl(buf);
}

void saslLibInitImpl() {
  return;
}
void saslLibCleanupImpl() {
  return;
}
int32_t saslConnCreateImpl(SSaslConn * *ppConn, int8_t server) {
  return TSDB_CODE_INVALID_CFG; 
}

int32_t saslConnInitImpl(SSaslConn * pConn) {
  return TSDB_CODE_INVALID_CFG; 
}

void saslConnCleanupImpl(SSaslConn * pConn) {
  return;
}

void saslConnSetStateImpl(SSaslConn* pConn, int32_t state) { return; }

int32_t saslConnEncodeImpl(SSaslConn * pConn, const char* input, int32_t len, const char** output,
                           unsigned* outputLen) {
  return TSDB_CODE_INVALID_CFG; 

}
int32_t saslConnDecodeImpl(SSaslConn * pConn, const char* input, int32_t len, const char** output,
                           unsigned* outputLen) {
  return TSDB_CODE_INVALID_CFG; 

}
int32_t saslConnHandleAuthImpl(SSaslConn * pConn, const char* input, int32_t len) {
  return TSDB_CODE_INVALID_CFG; 
}

int8_t saslConnShoudDoAuthImpl(SSaslConn * pConn) {
  return 0;
}

// sasl buffer func
int32_t saslBufferInitImpl(SSaslBuffer * buf, int32_t cap) {
  return TSDB_CODE_INVALID_PARA;
}
int32_t saslBufferAppendImpl(SSaslBuffer* buf, uint8_t* data, int32_t len) {
  return TSDB_CODE_INVALID_PARA;
}
void  saslBufferCleanupImpl(SSaslBuffer* buf) {
  return;
}
void saslBufferClearImpl(SSaslBuffer* buf) {
  return;
}

//#endif

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
#include "transTLS.h"
// clang-format on

#define DEFALUT_SSL_DIR "/etc/ssl/ssls"

extern int32_t transTlsCtxCreateImpl(const SRpcInit* pInit, SSslCtx** ppCtx);
//extern int32_t transTlsCtxCreateNewImpl(const SRpcInit* pInit, SSslCtx** ppCtx);
extern void    transTlsCtxDestroyImpl(SSslCtx* pCtx);

extern int32_t transTlsCtxCreateFromOld(SSslCtx* pOldCtx, int8_t mode, SSslCtx** pNewCtx);

extern int32_t sslInitImpl(SSslCtx* pCtx, STransTLS** ppTLs);
extern void    sslDestroyImpl(STransTLS* pTLs);
extern void    sslSetModeImpl(STransTLS* pTls, int8_t cliMode);
extern int32_t sslConnectImpl(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req);

extern int32_t sslWriteImpl(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req, uv_buf_t* pBuf, int32_t nBuf,
                            void (*cb)(uv_write_t*, int));

extern int32_t sslReadImpl(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread, int8_t cliMode);
extern int8_t  sslIsInitedImpl(STransTLS* pTls);
extern int32_t sslGetCertificateImpl(STransTLS* pTls);

extern int32_t sslBufferInitImpl(SSslBuffer* buf, int32_t cap);
extern void    sslBufferDestroyImpl(SSslBuffer* buf);
extern void    sslBufferClearImpl(SSslBuffer* buf);
extern int32_t sslBufferAppendImpl(SSslBuffer* buf, uint8_t* data, int32_t len);
extern int32_t sslBufferReallocImpl(SSslBuffer* buf, int32_t newCap, uv_buf_t* uvbuf);
extern int32_t sslBufferGetAvailableImpl(SSslBuffer* buf, int32_t* available);

extern void sslBufferRefImpl(SSslBuffer* buf);
extern void sslBufferUnrefImpl(SSslBuffer* buf);



int32_t transTlsCxtCreate(const SRpcInit* pInit, SSslCtx** ppCtx) { return transTlsCtxCreateImpl(pInit, ppCtx); }

void transTlsCtxDestroy(SSslCtx* pCtx) { transTlsCtxDestroyImpl(pCtx); }

int32_t sslInit(SSslCtx* pCtx, STransTLS** ppTLs) { return sslInitImpl(pCtx, ppTLs); }
void    sslDestroy(STransTLS* pTLs) { sslDestroyImpl(pTLs); }

void sslSetMode(STransTLS* pTls, int8_t cliMode) { sslSetModeImpl(pTls, cliMode); }

int32_t sslConnect(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req) { return sslConnectImpl(pTls, stream, req); }

int32_t sslWrite(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req, uv_buf_t* pBuf, int32_t nBuf,
                 void (*cb)(uv_write_t*, int)) {
  return sslWriteImpl(pTls, stream, req, pBuf, nBuf, cb);
}

int32_t sslRead(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread, int8_t cliMode) {
  return sslReadImpl(pTls, pBuf, nread, cliMode);
}

int32_t sslGetCertificate(STransTLS* pTls, char buf[], int32_t len) {
  int32_t code = sslGetCertificateImpl(pTls);
  if (code == 0) {
    tstrncpy(buf, pTls->certDn, len);
  }

  return code;
}

int8_t sslIsInited(STransTLS* pTls) { return sslIsInitedImpl(pTls); }

int32_t sslBufferInit(SSslBuffer* buf, int32_t cap) { return sslBufferInitImpl(buf, cap); }
void    sslBufferDestroy(SSslBuffer* buf) { return sslBufferDestroyImpl(buf); }
void    sslBufferClear(SSslBuffer* buf) { return sslBufferClearImpl(buf); }

int32_t sslBufferAppend(SSslBuffer* buf, uint8_t* data, int32_t len) { return sslBufferAppendImpl(buf, data, len); }
int32_t sslBufferRealloc(SSslBuffer* buf, int32_t newCap, uv_buf_t* uvbuf) {
  return sslBufferReallocImpl(buf, newCap, uvbuf);
}
int32_t sslBufferGetAvailable(SSslBuffer* buf, int32_t* available) { return sslBufferGetAvailableImpl(buf, available); }

void sslBufferRef(SSslBuffer* buf) { sslBufferRefImpl(buf); }
void sslBufferUnref(SSslBuffer* buf) { sslBufferUnrefImpl(buf);}

int32_t transTlsCxtMgtInit(STlsCxtMgt** pMgt) {
  int32_t code = 0;

  STlsCxtMgt *p = taosMemoryCalloc(1, sizeof(STlsCxtMgt));
  if (p == NULL) {
    return terrno;
  }

  (void)taosThreadRwlockInit(&p->lock, NULL);

  *pMgt = p;
  return code;
}

int32_t transTlsCxtMgtGet(STlsCxtMgt* pMgt, SSslCtx** ppCtx) {
  int32_t code = 0;

  (void)taosThreadRwlockRdlock(&pMgt->lock);
  *ppCtx = pMgt->pTlsCtx;
  (void)taosThreadRwlockUnlock(&pMgt->lock);

  return code;
}

int32_t transTlsCxtMgtUpdate(STlsCxtMgt* pMgt) {
  int32_t code = 0;

  (void)taosThreadRwlockWrlock(&pMgt->lock);
  SSslCtx* pOldCtx = pMgt->pTlsCtx; 
  pMgt->pTlsCtx = pMgt->pNewTlsCtx;
  pMgt->pNewTlsCtx = NULL;
  (void)taosThreadRwlockUnlock(&pMgt->lock);

  transTlsCxtUnref(pOldCtx);
  transTlsCxtRef(pMgt->pTlsCtx);

  return code;
}

int32_t transTlsCxtMgtAppend(STlsCxtMgt* pMgt, SSslCtx* pNewCtx) {
  int32_t code = 0;

  (void)taosThreadRwlockWrlock(&pMgt->lock);
  pMgt->pTlsCtx = pNewCtx;
  (void)taosThreadRwlockUnlock(&pMgt->lock);

  transTlsCxtRef(pNewCtx);
  return code;
}


void transTlsCxtMgtDestroy(STlsCxtMgt* pMgt) {
  (void)taosThreadRwlockDestroy(&pMgt->lock);
  transTlsCxtUnref(pMgt->pTlsCtx);
  taosMemoryFree(pMgt);
  return;
}

int32_t transTlsCxtMgtCreateNewCxt(STlsCxtMgt* pMgt, int8_t cliMode) {
  int32_t code = 0;
  SSslCtx *pOldCxt = NULL;
  if (!(transShouldDoReloadTlsConfig(pMgt))) {
    return TSDB_CODE_INVALID_CFG;
  }

  code = transTlsCxtMgtGet(pMgt, &pOldCxt); ;
  if (code != 0) {
    return code;
  }

  if (pOldCxt == NULL) {
    return TSDB_CODE_INVALID_CFG;
  }

  transTlsCxtRef(pOldCxt);

  (void)taosThreadRwlockWrlock(&pMgt->lock);
  code = transTlsCtxCreateFromOld(pOldCxt, cliMode, &pMgt->pNewTlsCtx);
  (void)taosThreadRwlockUnlock(&pMgt->lock);

  transTlsCxtUnref(pOldCxt);
  return code;
}


void transTlsCxtRef(SSslCtx* pCtx) {
  if (pCtx == NULL) {
    return;
  }
  int32_t count = atomic_fetch_add_32(&pCtx->refCount, 1);
  tInfo("ref tls context %p, current refCount:%d", pCtx, count + 1);
}
void transTlsCxtUnref(SSslCtx* pCtx) {
  if (pCtx == NULL) {
    return;
  }

  if (atomic_sub_fetch_32(&pCtx->refCount, 1) == 0) {
    tInfo("ref tls context %p, current refCount:%d, try to destroy", pCtx, atomic_load_32(&pCtx->refCount));
    transTlsCtxDestroy(pCtx);
  } else {
    tInfo("ref tls context %p, current refCount:%d", pCtx, atomic_load_32(&pCtx->refCount));
  }
}


int8_t transDoReloadTlsConfig(STlsCxtMgt *pMgt, int32_t numOfThreads) {
  STlsCxtMgt *p = pMgt;
  if (atomic_load_8(&p->tlsLoading)) {
    return 0;
  }
  if (atomic_add_fetch_32(&p->loadTlsCount, 1) >= numOfThreads) {
    (void)transTlsCxtMgtUpdate(p);

    atomic_store_32(&p->loadTlsCount, 0); 
    atomic_store_8(&p->tlsLoading, 0);  
    return 1;
  }
  return 0;
}

int8_t transShouldDoReloadTlsConfig(STlsCxtMgt *pMgt) {
  STlsCxtMgt *p = pMgt;
  int8_t ready = atomic_load_8(&p->tlsLoading) ? 0 : 1;
  if (ready) {
    tInfo("transport instance is ready to reload tls config");
  } else {
    tInfo("transport instance is not ready to reload tls config");
  }
  return ready;
}

#if !defined(TD_ENTERPRISE)

int32_t sslGetCertificateImpl(STransTLS* pTls) { return TSDB_CODE_INVALID_CFG; }

int32_t transTlsCtxCreateFromOld(SSslCtx* pOldCtx, int8_t mode, SSslCtx** pNewCtx) { return TSDB_CODE_INVALID_CFG; }

int32_t transTlsCtxCreateImpl(const SRpcInit* pInit, SSslCtx** ppCtx) { return TSDB_CODE_INVALID_CFG; }

//int32_t transTlsCtxCreateNewImpl(const SRpcInit* pInit, SSslCtx** ppCtx) { return TSDB_CODE_INVALID_CFG; } 

void    transTlsCtxDestroyImpl(SSslCtx* pCtx) { return; }

int32_t sslInitImpl(SSslCtx* pCtx, STransTLS** ppTLs) { return TSDB_CODE_INVALID_CFG; }

void sslDestroyImpl(STransTLS* pTLs) { return; }

void sslSetModeImpl(STransTLS* pTls, int8_t cliMode) { return; }

int32_t sslConnectImpl(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req) { return TSDB_CODE_INVALID_CFG; }

int32_t sslWriteImpl(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req, uv_buf_t* pBuf, int32_t nBuf,
                     void (*cb)(uv_write_t*, int)) {
  return TSDB_CODE_INVALID_CFG;
}

int32_t sslReadImpl(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread, int8_t cliMode) { return TSDB_CODE_INVALID_CFG; }
int8_t  sslIsInitedImpl(STransTLS* pTls) { return 0; }

int32_t sslBufferInitImpl(SSslBuffer* buf, int32_t cap) { return TSDB_CODE_INVALID_CFG; }
void    sslBufferDestroyImpl(SSslBuffer* buf) { return; }
void    sslBufferClearImpl(SSslBuffer* buf) { return; }
int32_t sslBufferAppendImpl(SSslBuffer* buf, uint8_t* data, int32_t len) { return TSDB_CODE_INVALID_CFG; }
int32_t sslBufferReallocImpl(SSslBuffer* buf, int32_t newCap, uv_buf_t* uvbuf) { return TSDB_CODE_INVALID_CFG; }
int32_t sslBufferGetAvailableImpl(SSslBuffer* buf, int32_t* available) { return TSDB_CODE_INVALID_CFG; }

void sslBufferRefImpl(SSslBuffer* buf) { return; }
void sslBufferUnrefImpl(SSslBuffer* buf) { return; }

#endif
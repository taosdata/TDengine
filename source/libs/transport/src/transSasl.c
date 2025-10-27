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

extern void saslServerInitImpl();
extern void saslServerCleanupImpl();

extern void saslClientStartImpl();
extern void saslServerStartImpl();
extern void saslClientStepImpl();
extern void saslServerStepImpl();

#define SASL_MECHANISM_SCRAM_SHA256 "SCRAM-SHA-256"

enum { STATE_HANDSHAKE = 0, STATE_SALA_AUTH, STATE_READY, STATE_CLOSING } SASL_STATE;

int32_t saslConnCreate(SSaslConn** ppConn) {
  int32_t code = 0;

  SSaslConn* pConn = (SSaslConn*)taosMemCalloc(1, sizeof(SSaslConn));
  if (pConn == NULL) {
    tError("saslConnCreate failed to alloc memory");
    return terrno;
  }
  memset(pConn, 0, sizeof(SSaslConn));
  pConn->state = STATE_HANDSHAKE;

  *ppConn = pConn;
  return code;
}

void saslConnSetState(SSaslConn* pConn, int32_t state) {
  if (pConn == NULL) {
    return;
  }
  pConn->state = state;
}

static int saslCallBackFn(SSaslConn* conn, int id, const char** result, unsigned* len, void* context) {
  if (id == SASL_CB_USER) {
    *result = taosStrdup("tdengineUser");
    if (len) *len = (unsigned)strlen(*result);
    return SASL_OK;
  } else if (id == SASL_CB_PASS) {
    *result = taosStrdup("tdenginePass");
    if (len) *len = (unsigned)strlen(*result);
    return SASL_OK;
  } else {
    return SASL_FAIL;
  }
  return SASL_FAIL;
}
int32_t saslConnInit(SSaslConn** pConn, int8_t isServer) {
  int32_t code = 0;
  int32_t lino = 0;
  int     result;

  sasl_callback_t callbacks[] = {
      {SASL_CB_USER, (int (*)())saslCallBackFn, NULL},
      {SASL_CB_PASS, (int (*)())saslCallBackFn, NULL},
      {SASL_CB_LIST_END, NULL, NULL},
  };

  code = saslConnCreate(pConn);
  TAOS_CHECK_GOTO(code, &lino, _error);

  if (isServer) {
    result = sasl_server_new("tdengine", NULL, NULL, NULL, NULL, callbacks, 0, &(*pConn)->conn);
    if (result != SASL_OK) {
      tError("sasl_server_new failed: %s", sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }
  } else {
    result = sasl_client_new("tdengine", NULL, NULL, NULL, callbacks, 0, &(*pConn)->conn);
    if (result != SASL_OK) {
      tError("sasl_client_new failed: %s", sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }
  }
  (*pConn)->completed = 0;

_error:
  if (code != 0) {
    if (pConn) {
      saslConnCleanup(*pConn);
      *pConn = NULL;
    }
  }
  return code;
}

void saslConnCleanup(SSaslConn* pConn) {
  if (pConn == NULL || pConn->conn == NULL) {
    return;
  }

  if (pConn->conn != NULL) {
    sasl_dispose(&pConn->conn);
    pConn->conn = NULL;
  }

  if (pConn->authUser != NULL) {
    taosMemFreeClear(pConn->authUser);
    pConn->authUser = NULL;
  }

  taosMemFree(pConn);
}

int32_t saslConnEncode(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen) {
  int32_t code = 0;
  int     result = 0;

  const char* outBuf = NULL;
  unsigned    outBufLen = 0;
  if (pConn == NULL || pConn->conn == NULL) {
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

  result = sasl_encode(pConn->conn, input, len, (const char**)outBuf, &outBufLen);
  if (result != SASL_OK) {
    tError("sasl_encode64 failed: %s", sasl_errstring(result, NULL, NULL));
    code = TSDB_CODE_THIRDPARTY_ERROR;
  } else {
    *output = taosMemoryMalloc(outBufLen);
    if (*output == NULL) {
      tError("saslEncode failed to alloc memory");
      return terrno;
    }

    memcpy((void*)*output, outBuf, outBufLen);
    *outputLen = outBufLen;
  }
  return code;
}

int32_t saslConnDecode(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen) {
  int32_t code = 0;
  int     result = 0;

  if (pConn == NULL || pConn->conn == NULL) {
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

  result = sasl_decode(pConn->conn, input, len, (const char**)output, outputLen);
  if (result != SASL_OK) {
    tError("sasl_decode64 failed: %s", sasl_errstring(result, NULL, NULL));
    code = TSDB_CODE_THIRDPARTY_ERROR;
  }
  return code;
}

int32_t saslConnHandleAuth(SSaslConn* pConn, const char* input, int32_t len) {
  int32_t     code = 0;

  if (pConn == NULL || pConn->conn == NULL) {
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

  const char* mechanism = SASL_MECHANISM_SCRAM_SHA256;

  const char* clientOut = NULL;
  unsigned    clientOutLen = 0;

  int result = sasl_server_start(pConn->conn, mechanism, input, len, &clientOut, &clientOutLen);
  while (result == SASL_CONTINUE) {
    const char* serverOut = NULL;
    unsigned    serverOutLen = 0;
    result = sasl_server_step(pConn->conn, input, len, &clientOut, &serverOutLen);
  }

  if (result == SASL_OK) {
    pConn->completed = 1;

    result = sasl_getprop(pConn->conn, SASL_USERNAME, (const void**)&pConn->authUser);
    if (result != SASL_OK) {
      tError("sasl_getprop SASL_USERNAME failed: %s", sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
    }

  } else {
    tError("sasl_server_step failed: %s", sasl_errstring(result, NULL, NULL));
    code = TSDB_CODE_THIRDPARTY_ERROR;
  }

  return code;
}

// int32_t transTlsCtxCreate(const SRpcInit* pInit, SSslCtx** ppCtx) { return transTlsCtxCreateImpl(pInit, ppCtx); }

// void transTlsCtxDestroy(SSslCtx* pCtx) { transTlsCtxDestroyImpl(pCtx); }

// int32_t sslInit(SSslCtx* pCtx, STransTLS** ppTLs) { return sslInitImpl(pCtx, ppTLs); }
// void    sslDestroy(STransTLS* pTLs) { sslDestroyImpl(pTLs); }

// void sslSetMode(STransTLS* pTls, int8_t cliMode) { sslSetModeImpl(pTls, cliMode); }

// int32_t sslConnect(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req) { return sslConnectImpl(pTls, stream, req);
// }

// int32_t sslWrite(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req, uv_buf_t* pBuf, int32_t nBuf,
//                  void (*cb)(uv_write_t*, int)) {
//   return sslWriteImpl(pTls, stream, req, pBuf, nBuf, cb);
// }

// int32_t sslRead(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread, int8_t cliMode) {
//   return sslReadImpl(pTls, pBuf, nread, cliMode);
// }

// int8_t sslIsInited(STransTLS* pTls) { return sslIsInitedImpl(pTls); }

// int32_t sslBufferInit(SSslBuffer* buf, int32_t cap) { return sslBufferInitImpl(buf, cap); }
// void    sslBufferDestroy(SSslBuffer* buf) { return sslBufferDestroyImpl(buf); }
// void    sslBufferClear(SSslBuffer* buf) { return sslBufferClearImpl(buf); }

// int32_t sslBufferAppend(SSslBuffer* buf, uint8_t* data, int32_t len) { return sslBufferAppendImpl(buf, data, len); }
// int32_t sslBufferRealloc(SSslBuffer* buf, int32_t newCap, uv_buf_t* uvbuf) {
//   return sslBufferReallocImpl(buf, newCap, uvbuf);
// }
// int32_t sslBufferGetAvailable(SSslBuffer* buf, int32_t* available) { return sslBufferGetAvailableImpl(buf,
// available); }

// void sslBufferRef(SSslBuffer* buf) { sslBufferRefImpl(buf); }
// void sslBufferUnref(SSslBuffer* buf) { sslBufferUnrefImpl(buf); }

// #if !defined(TD_ENTERPRISE)

// int32_t transTlsCtxCreateImpl(const SRpcInit* pInit, SSslCtx** ppCtx) { return TSDB_CODE_INVALID_CFG; }
// void    transTlsCtxDestroyImpl(SSslCtx* pCtx) { return; }

// int32_t sslInitImpl(SSslCtx* pCtx, STransTLS** ppTLs) { return TSDB_CODE_INVALID_CFG; }

// void sslDestroyImpl(STransTLS* pTLs) { return; }

// void sslSetModeImpl(STransTLS* pTls, int8_t cliMode) { return; }

// int32_t sslConnectImpl(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req) { return TSDB_CODE_INVALID_CFG; }

// int32_t sslWriteImpl(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req, uv_buf_t* pBuf, int32_t nBuf,
//                      void (*cb)(uv_write_t*, int)) {
//   return TSDB_CODE_INVALID_CFG;
// }

// int32_t sslReadImpl(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread, int8_t cliMode) { return
// TSDB_CODE_INVALID_CFG; } int8_t  sslIsInitedImpl(STransTLS* pTls) { return 0; }

// int32_t sslBufferInitImpl(SSslBuffer* buf, int32_t cap) { return TSDB_CODE_INVALID_CFG; }
// void    sslBufferDestroyImpl(SSslBuffer* buf) { return; }
// void    sslBufferClearImpl(SSslBuffer* buf) { return; }
// int32_t sslBufferAppendImpl(SSslBuffer* buf, uint8_t* data, int32_t len) { return TSDB_CODE_INVALID_CFG; }
// int32_t sslBufferReallocImpl(SSslBuffer* buf, int32_t newCap, uv_buf_t* uvbuf) { return TSDB_CODE_INVALID_CFG; }
// int32_t sslBufferGetAvailableImpl(SSslBuffer* buf, int32_t* available) { return TSDB_CODE_INVALID_CFG; }

// void sslBufferRefImpl(SSslBuffer* buf) { return; }
// void sslBufferUnrefImpl(SSslBuffer* buf) { return; }

// #endif

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
#include "transComm.h"
#include <openssl/err.h>
#include <openssl/ssl.h>
#include "transLog.h"
// clang-format on

#define DEFALUT_SSL_DIR "/etc/ssl/ssls"

static int32_t sslDoConn(STransTLS* pTls);

static int32_t sslHandleError(STransTLS* pTls, int ret);

static int32_t sslWriteToBIO(STransTLS* pTls, int32_t nread);

static void destroySSLCtx(SSL_CTX* ctx);

SSL_CTX* initSSLCtx(const char* certPath, const char* keyPath, const char* caPath, int8_t cliMode) {
  int32_t lino = 0;
  int32_t code = 0;
  int     ret = 1;

  SSL_load_error_strings();
  SSL_library_init();
  OpenSSL_add_all_algorithms();

  const SSL_METHOD* sslMode = cliMode ? TLS_client_method() : TLS_server_method();

  SSL_CTX* ctx = SSL_CTX_new(sslMode);
  if (ctx == NULL) {
    tError("failed to create ssl ctx");
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &lino, _error);
  }
  SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
  SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION);

  if (cliMode) {
    ret = SSL_CTX_load_verify_locations(ctx, caPath, NULL);
    if (ret == 1) {
      ret = SSL_CTX_use_certificate_chain_file(ctx, certPath);
    }
    if (ret == 1) {
      ret = SSL_CTX_use_PrivateKey_file(ctx, keyPath, SSL_FILETYPE_PEM);
    }

  } else {
    ret = SSL_CTX_load_verify_locations(ctx, caPath, NULL);
    if (ret == 1) {
      ret = SSL_CTX_use_certificate_chain_file(ctx, certPath);
    }
    if (ret == 1) {
      ret = SSL_CTX_use_PrivateKey_file(ctx, keyPath, SSL_FILETYPE_PEM);
    }
  }

_error:

  if (ret != 1) {
    unsigned long err;
    while ((err = ERR_get_error()) != 0) {
      char buf[256] = {0};
      ERR_error_string_n(err, buf, sizeof(buf));
      tError("failed to init ssl ctx since:%s", buf);
    }

    code = TSDB_CODE_THIRDPARTY_ERROR;
  }
  if (code != 0) {
    tError("failed to init ssl ctx since %s", tstrerror(code));
    destroySSLCtx(ctx);
    ctx = NULL;
  }
  return ctx;
}
void destroySSLCtx(SSL_CTX* ctx) {
  if (ctx) SSL_CTX_free(ctx);
}

int32_t sslHandleError(STransTLS* pTls, int ret) {
  int32_t code = 0;
  int err = SSL_get_error(pTls->ssl, ret);
  switch (err) {
    case SSL_ERROR_WANT_READ:
      tDebug("conn %p SSL should read ", pTls->pConn);
      // read
      break;
    case SSL_ERROR_WANT_WRITE:
      tDebug("conn %p SSL should write", pTls->pConn);
      break;
    case SSL_ERROR_SSL:
      tError("conn %p SSL error: %s", pTls->pConn, ERR_reason_error_string(ERR_get_error()));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      break;
    default:
      tError("conn %p Unknown SSL error: %s", pTls->pConn, ERR_reason_error_string(ERR_get_error()));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      break;
  }
  return code;
}

int32_t transTlsCtxCreate(const char* certPath, const char* keyPath, const char* caPath, int8_t cliMode,
                          SSslCtx** ppCtx) {
  int32_t code = 0;
  int32_t lino = 0;

  SSslCtx* pCtx = (SSslCtx*)taosMemCalloc(1, sizeof(SSslCtx));
  if (pCtx == NULL) {
    tError("Failed to allocate memory for TLS context");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pCtx->certfile = taosStrdupi(caPath);
  pCtx->keyfile = taosStrdupi(keyPath);
  pCtx->cafile = taosStrdupi(caPath);
  if (pCtx->certfile == NULL || pCtx->keyfile == NULL || pCtx->cafile == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    tError("Failed to duplicate TLS context file paths since %s", tstrerror(code));
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  pCtx->sslCtx = initSSLCtx(pCtx->certfile, pCtx->keyfile, pCtx->cafile, cliMode);
  if (pCtx->sslCtx == NULL) {
    tError("Failed to initialize SSL context");
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &lino, _error);
  }

  *ppCtx = pCtx;
_error:
  if (code != 0) {
    transTlsCtxDestroy(pCtx);
  }

  return code;
}

void transTlsCtxDestroy(SSslCtx* pCtx) {
  if (pCtx) {
    destroySSLCtx(pCtx->sslCtx);
    pCtx->sslCtx = NULL;

    taosMemoryFree(pCtx->certfile);
    taosMemoryFree(pCtx->keyfile);
    taosMemoryFree(pCtx->cafile);

    taosMemFree(pCtx);
  }
}

int32_t sslInit(SSslCtx* pCtx, STransTLS** ppTLs) {
  int32_t code = 0;
  int32_t lino = 0;
  if (pCtx == NULL) {
    tError("SSL context is not initialized");
    return TSDB_CODE_INVALID_CFG;
  }

  STransTLS* pTls = (STransTLS*)taosMemCalloc(1, sizeof(STransTLS));
  if (pTls == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _error);
  }

  pTls->ssl = SSL_new(pCtx->sslCtx);
  if (pTls->ssl == NULL) {
    tError("Failed to create new SSL_new");
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &lino, _error);
  }

  pTls->readBio = BIO_new(BIO_s_mem());
  pTls->writeBio = BIO_new(BIO_s_mem());
  if (pTls->readBio == NULL || pTls->writeBio == NULL) {
    tError("Failed to create read/write BIO buffer");
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &lino, _error);
  }

  // BIO_set_nbio(pTls->readBio, 1);
  // BIO_set_nbio(pTls->writeBio, 1);

  SSL_set_bio(pTls->ssl, pTls->readBio, pTls->writeBio);

  code = sslBufferInit(&pTls->readBuf, 4096);
  TAOS_CHECK_GOTO(code, &lino, _error);

  code = sslBufferInit(&pTls->sendBuf, 4096);
  TAOS_CHECK_GOTO(code, &lino, _error);

  *ppTLs = pTls;

_error:
  if (code != 0) {
    sslDestroy(pTls);
  }
  return code;
}

void sslSetMode(STransTLS* pTls, int8_t cliMode) {
  if (cliMode) {
    SSL_set_connect_state(pTls->ssl);  // client mode
  } else {
    SSL_set_accept_state(pTls->ssl);  // server mode
  }
}

static int32_t sslInitConn(STransTLS* pTls) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pTls == NULL || pTls->ssl == NULL) {
    tError("SSL is not initialized");
    return TSDB_CODE_INVALID_CFG;
  }

  int ret = SSL_do_handshake(pTls->ssl);

  if (ret <= 0) {
    int err = SSL_get_error(pTls->ssl, ret);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
      if (err == SSL_ERROR_WANT_READ) {
        tDebug("conn %p read more data to complete ssl", pTls->pConn);
      } else {
        tDebug("conn %p write more data to complete ssl", pTls->pConn);
      }
      // Handehake is in progress, continue later
      return TSDB_CODE_SUCCESS;
    } else {
      tError("SSL handshake failed: %s", ERR_reason_error_string(ERR_get_error()));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }
  }

_error:
  if (code != 0) {
    tError("SSL handshake failed since %s", tstrerror(code));
    return code;
  }

  return code;
}
int32_t sslConnect(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req) {
  int32_t code = 0;
  int32_t lino = 0;

  sslSetMode(pTls, 1);

  code = sslInitConn(pTls);
  TAOS_CHECK_GOTO(code, NULL, _error);

  code = sslDoConn(pTls);
  TAOS_CHECK_GOTO(code, NULL, _error);

_error:
  if (code != 0) {
    tError("SSL connect failed since %s", tstrerror(code));
    return code;
  }
  return code;
}

static int32_t sslFlushBioToSocket(STransTLS* pTls) {
  if (pTls == NULL || pTls->ssl == NULL) {
    tError("SSL is not initialized");
    return TSDB_CODE_INVALID_CFG;
  }

  char buf[4096];
  int  n;
  while ((n = BIO_read(pTls->writeBio, buf, sizeof(buf))) > 0) {
    uv_buf_t b = uv_buf_init(buf, n);

    uv_write_t* req = taosMemCalloc(1, sizeof(uv_write_t));
    if (req == NULL) {
      return terrno;
    }

    req->data = pTls->pConn;
    int status = uv_write(req, (uv_stream_t*)pTls->pStream, &b, 1, pTls->writeCb);
    if (status != 0) {
      tError("Failed to write SSL data: %s", uv_err_name(status));
      return TSDB_CODE_THIRDPARTY_ERROR;
    } else {
      tDebug("conn %p write %d bytes to socket", pTls->pConn, n);
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t sslDoConn(STransTLS* pTls) { return sslFlushBioToSocket(pTls); }

int32_t sslWrite(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req, uv_buf_t* pBuf, int32_t nBuf,
                 void (*cb)(uv_write_t*, int)) {
  int32_t lino = 0;
  int32_t code = 0;
  int32_t nread = 0, total = 0;
  for (int i = 0; i < nBuf; i++) {
    int n = SSL_write(pTls->ssl, pBuf[i].base, pBuf[i].len);
    if (n <= 0) {
      code = sslHandleError(pTls, n);
      TAOS_CHECK_GOTO(code, &lino, _error);
    }
  }

  int32_t start = SSL_BUFFER_LEN(&pTls->sendBuf);

  uint8_t buf[4096] = {0};
  while ((nread = BIO_read(pTls->writeBio, buf, sizeof(buf))) > 0) {
    code = sslBufferAppend(&pTls->sendBuf, buf, nread);
    TAOS_CHECK_GOTO(code, &lino, _error);
    total += nread;
  }
  if (total <= 0) {
    tDebug("conn %p not read data from bio", pTls->pConn);
    return code;
  }

  sslBufferRef(&pTls->sendBuf);

  uv_buf_t b = uv_buf_init((char*)(SSL_BUFFER_OFFSET_DATA(&(pTls->sendBuf), start)), total);
  int32_t status = uv_write(req, stream, &b, 1, cb);
  if (status == 0) {
    tDebug("conn %p write %d bytes to socket", pTls->pConn, total);
  } else {
    sslBufferUnref(&pTls->sendBuf);
    tError("Failed to write SSL data: %s", uv_err_name(status));
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &lino, _error);
  }

_error:
  if (code != 0) {
    tError("SSL write error since %s", tstrerror(code));
  }
  return code;
}

// netcore --> BIO ---> SSL ---> user

static int32_t sslDoConnOrRead(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread) {
  int32_t code = 0;
  int32_t lino = 0;

  int     ret = SSL_do_handshake(pTls->ssl);
  if (ret == 1) {
    tDebug("SSL handshake completed successfully");
  } else {
    code = sslHandleError(pTls, ret);
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  code = sslFlushBioToSocket(pTls);
_error:
  return code;
}

int8_t sslIsInited(STransTLS* pTls) {
  if (pTls == NULL || pTls->ssl == NULL) {
    tError("SSL is not initialized");
    return 0;
  }
  return SSL_is_init_finished(pTls->ssl) ? 1 : 0;
}

static int32_t sslWriteToBIO(STransTLS* pTls, int32_t nread) {
  int32_t     code = 0;
  SSslBuffer* sslBuf = &pTls->readBuf;

  sslBuf->len += nread;
  tDebug("conn %p write %d bytes to bio,ssl buf len %d", pTls->pConn, nread, sslBuf->len);
  int32_t nwrite = BIO_write(pTls->readBio, sslBuf->buf, sslBuf->len);

  tDebug("conn %p read %d bytes from socket", pTls->pConn, nread);

  sslBuf->len = 0;
  return code;
}

int32_t sslRead(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread, int8_t cliMode) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SSslBuffer* sslBuf = &pTls->readBuf;
  int32_t     nwrite = 0;

  code = sslWriteToBIO(pTls, nread);
  if (code != 0) {
    tError("conn %p failed to write data to SSL BIO since %s", pTls->pConn, tstrerror(code));
    return code;
  }

  if (!SSL_is_init_finished(pTls->ssl)) {
    code = sslDoConnOrRead(pTls, pBuf, nread);
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  char buf[4096] = {0};
  while ((nwrite = SSL_read(pTls->ssl, buf, sizeof(buf))) > 0) {
    code = transConnBufferAppend(pBuf, buf, nwrite);
    if (code != 0) {
      tError("failed to append decrypted data to conn buffer since %s", tstrerror(code));
      return code;
    }
  }

  if (nwrite < 0) {
    code = sslHandleError(pTls, nwrite);
  }
_error:
  return code;
}

void sslDestroy(STransTLS* pTls) {
  if (pTls) {
    SSL_free(pTls->ssl);

    sslBufferDestroy(&pTls->readBuf);
    sslBufferDestroy(&pTls->sendBuf);

    taosMemoryFree(pTls);
  }
}

int32_t sslBufferInit(SSslBuffer* buf, int32_t cap) {
  if (buf == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  buf->cap = cap;
  buf->len = 0;
  buf->buf = taosMemoryCalloc(1, cap);
  if (buf->buf == NULL) {
    return terrno;
  }

  buf->ref = 0;
  return 0;
}

void sslBufferClear(SSslBuffer* buf) {
  buf->len = 0;
  memset(buf->buf, 0, buf->cap);
}

int32_t sslBufferDestroy(SSslBuffer* buf) {
  if (buf == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  taosMemoryFree(buf->buf);
  buf->buf = NULL;
  buf->cap = 0;
  buf->len = 0;
  return 0;
}

int32_t sslBufferAppend(SSslBuffer* buf, uint8_t* data, int32_t len) {
  int32_t code = 0;
  if (buf == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (buf->len + len > buf->cap) {
    int32_t newCap = buf->cap * 2;
    while (newCap < buf->len + len) {
      newCap *= 2;
    }

    uint8_t* newBuf = taosMemoryRealloc(buf->buf, newCap);
    if (newBuf == NULL) {
      return terrno;
    }

    buf->buf = newBuf;
    buf->cap = newCap;
  }
  memcpy(buf->buf + buf->len, data, len);

  buf->len += len;
  return code;
}

int32_t sslBufferGetAvailable(SSslBuffer* buf, int32_t* available) {
  if (buf == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *available = buf->cap - buf->len;
  return 0;
}

int32_t sslBufferRealloc(SSslBuffer* buf, int32_t newCap, uv_buf_t* uvbuf) {
  int32_t code = 0;
  int32_t lino = 0;
  if (buf == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (buf->len <= buf->cap / 2) {
    goto _error;  // no need to realloc
  } else {
    newCap = buf->cap * 2;
  }

  uint8_t* newBuf = taosMemoryRealloc(buf->buf, newCap);
  if (newBuf == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _error);
  }

  buf->buf = newBuf;
  buf->cap = newCap;

_error:
  if (code != 0) {
    tError("failed to realloc ssl buffer since %s", tstrerror(code));
    return code;
  }

  uvbuf->base = (char*)buf->buf + buf->len;
  uvbuf->len = buf->cap - buf->len;

  uDebug("alloc recv buffer, base:%p, len:%d", uvbuf->base, (int)(uvbuf->len));
  return 0;
}

void sslBufferRef(SSslBuffer* buf) {
  if (buf == NULL) {
  }
  buf->ref++;
}

void sslBufferUnref(SSslBuffer* buf) {
  if (buf->ref > 0) {
    buf->ref--;
  }
  if (buf->ref == 0) {
    sslBufferClear(buf);
  }
}

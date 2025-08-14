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

#define DEFALUT_SSL_DIR "/etc/ssl/"
static int32_t sslReadDecryptedData(STransTLS* pTls, char* data, size_t ndata);
static int32_t sslWriteEncyptedData(STransTLS* pTls, const char* data, size_t ndata);

static int32_t sslDoWriteHandshake(STransTLS* pTls);
static void destroySSLCtx(SSL_CTX* ctx);

SSL_CTX* initSSLCtx(const char* cert_path, const char* key_path, const char* ca_path) {
  SSL_load_error_strings();
  SSL_library_init();
  OpenSSL_add_all_algorithms();
  int32_t code = 0;

  SSL_CTX* ctx = SSL_CTX_new(TLS_method());
  SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);

  X509_STORE* store = X509_STORE_new();

  if (X509_STORE_load_locations(store, NULL, DEFALUT_SSL_DIR) <= 0) {
    code = TSDB_CODE_THIRDPARTY_ERROR;
    tError("failed to load CA file from %s since %s", ca_path, tstrerror(code));
  }

  // if (SSL_CTX_use_certificate_file(ctx, cert_path, SSL_FILETYPE_PEM) <= 0) {
  //   tError("failed to load certificate file: %s", cert_path);
  //   SSL_CTX_free(ctx);
  //   return NULL;
  // }

  // if (SSL_CTX_use_PrivateKey_file(ctx, key_path, SSL_FILETYPE_PEM) <= 0) {
  //   tError("failed to load private key file: %s", key_path);
  //   SSL_CTX_free(ctx);
  //   return NULL;
  // }

  // if (SSL_CTX_check_private_key(ctx) <= 0) {
  //   tError("private key does not match the public certificate");
  //   SSL_CTX_free(ctx);
  //   return NULL;
  // }

  //     if (SSL_CTX_load_verify_locations(ctx, ca_path, NULL) <= 0) {
  //   tError("failed to load CA file: %s", ca_path);
  //   SSL_CTX_free(ctx);
  //   return NULL;
  // }
_error:
  if (code != 0) {
    tError("failed to init ssl ctx since %s", tstrerror(code));
    destroySSLCtx(ctx);
    return NULL;
  }
  return ctx;
}
void destroySSLCtx(SSL_CTX* ctx) {
  if (ctx) SSL_CTX_free(ctx);
}

void handleSSLError(SSL* ssl, int ret) {
  int err = SSL_get_error(ssl, ret);
  switch (err) {
    case SSL_ERROR_WANT_READ:
      tInfo("SSL should read");
      // read
      break;
    case SSL_ERROR_WANT_WRITE:
      tInfo("SSL should write%s");
      break;
    case SSL_ERROR_SSL:
      tError("SSL error: %s", ERR_reason_error_string(ERR_get_error()));
      break;
    default:
      tError("Unknown SSL error: %s", ERR_reason_error_string(ERR_get_error()));
      break;
  }
}

int32_t transTlsCtxCreate(const char* certPath, const char* keyPath, const char* caPath, SSslCtx** ppCtx) {
  int32_t code = 0;
  int32_t lino = 0;

  SSslCtx* pCtx = (SSslCtx*)taosMemCalloc(1, sizeof(SSslCtx));
  if (pCtx == NULL) {
    tError("Failed to allocate memory for TLS context");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pCtx->certfile = taosStrdupi("test");
  pCtx->keyfile = taosStrdupi("test");
  pCtx->cafile = taosStrdupi("tes");
  if (pCtx->certfile == NULL || pCtx->keyfile == NULL || pCtx->cafile == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    tError("Failed to duplicate TLS context file paths since %s", tstrerror(code));
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  pCtx->ssl_ctx = initSSLCtx(certPath, keyPath, caPath);
  if (pCtx->ssl_ctx == NULL) {
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
    destroySSLCtx(pCtx->ssl_ctx);
    pCtx->ssl_ctx = NULL;

    taosMemoryFree(pCtx->certfile);
    taosMemoryFree(pCtx->keyfile);
    taosMemoryFree(pCtx->cafile);

    taosMemFree(pCtx);
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
  return 0;
}

int32_t sslBufferClear(SSslBuffer* buf) {
  if (buf == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  buf->len = 0;
  memset(buf->buf, 0, buf->cap);
  return 0;
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
  return 0;
}

int32_t initSSL(SSslCtx* pCtx, STransTLS** ppTLs) {
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

  pTls->ssl = SSL_new(pCtx->ssl_ctx);
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

  SSL_set_bio(pTls->ssl, pTls->readBio, pTls->writeBio);

  code = sslBufferInit(&pTls->readBuf, 4096);
  TAOS_CHECK_GOTO(code, &lino, _error);

  *ppTLs = pTls;

_error:
  if (code != 0) {
    destroySSL(pTls);
  }
  return code;
}

void setSSLMode(STransTLS* pTls, int8_t cliMode) {
  if (cliMode) {
    SSL_set_connect_state(pTls->ssl);  // client mode
  } else {
    SSL_set_accept_state(pTls->ssl);  // server mode
  }
}

static int32_t sslDoHandShake(STransTLS* pTls) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pTls == NULL || pTls->ssl == NULL) {
    tError("SSL is not initialized");
    return TSDB_CODE_INVALID_CFG;
  }

  int ret = SSL_connect(pTls->ssl);

  if (ret <= 0) {
    int err = SSL_get_error(pTls->ssl, ret);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
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
int32_t sslDoConnect(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req) {
  int32_t code = 0;
  int32_t lino = 0;

  setSSLMode(pTls, 1);

  code = sslDoHandShake(pTls);
  TAOS_CHECK_GOTO(code, NULL, _error);

  code = sslDoWriteHandshake(pTls);
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
      return status;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t sslDoWriteHandshake(STransTLS* pTls) {
  int32_t code = 0;
  if (pTls == NULL || pTls->ssl == NULL) {
    tError("SSL is not initialized");
    return TSDB_CODE_INVALID_CFG;
  }

  char buf[1024] = {0};
  int  n = 0;

  while ((n = BIO_read(pTls->writeBio, buf, sizeof(buf))) > 0) {
    uv_buf_t    b = uv_buf_init(buf, n);
    uv_write_t* req = taosMemoryCalloc(1, sizeof(uv_write_t));
    if (req == NULL) {
      return terrno;
    }

    req->data = pTls->pConn;
    code = uv_write(req, pTls->pStream, &b, 1, pTls->writeCb);
    if (code != 0) {
      tError("Failed to write SSL handshake data: %s", uv_err_name(code));
      return code;
    }
  }

  return code;
}

int32_t sslDoWrite(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req, uv_buf_t* pBuf, int32_t nBuf,
                   void (*cb)(uv_write_t*, int)) {
  int32_t code = 0;
  int32_t nread = 0;
  for (int i = 0; i < nBuf; i++) {
    int n = SSL_write(pTls->ssl, pBuf[i].base, pBuf[i].len);
    if (n <= 0) {
      handleSSLError(pTls->ssl, n);
      code = TSDB_CODE_THIRDPARTY_ERROR;
      break;
    }
  }

  if (code != 0) {
    tError("SSL write error since %s", tstrerror(code));
    return code;
  }

  char buf[4096] = {0};
  while ((nread = BIO_read(pTls->writeBio, buf, sizeof(buf))) > 0) {
    uv_buf_t b = uv_buf_init(buf, nread);
    code = uv_write(req, stream, &b, 1, cb);
  }
  return code;
}

// netcore --> BIO ---> SSL ---> user

static int32_t sslDoHandsShakeOrRead(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread, int8_t* waitRead) {
  int32_t code = 0;
  int     ret = SSL_connect(pTls->ssl);
  if (ret == 1) {
    tDebug("SSL handshake completed successfully");
    sslFlushBioToSocket(pTls);
    return TSDB_CODE_SUCCESS;
  }

  int err = SSL_get_error(pTls->ssl, ret);
  if (err == SSL_ERROR_WANT_READ) {
    *waitRead = 1;            // 等待对端
    return TSDB_CODE_SUCCESS; /* 等待对端 */
  } else if (err == SSL_ERROR_WANT_WRITE) {
    code = sslFlushBioToSocket(pTls);
    if (code != 0) {
      tError("Failed to flush SSL write BIO");
      return code;
    }
  } else {
    tError("SSL handshake failed: %s", ERR_reason_error_string(ERR_get_error()));
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  return code;
}

int8_t sslIsInited(STransTLS* pTls) {
  if (pTls == NULL || pTls->ssl == NULL) {
    tError("SSL is not initialized");
    return 0;
  }
  return SSL_is_init_finished(pTls->ssl) ? 1 : 0;
}
int32_t sslDoRead(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread, int8_t cliMode) {
  int32_t     code = 0;
  int8_t      waitRead = 0;  // 等待对端
  SSslBuffer* sslBuf = &pTls->readBuf;

  sslBuf->len += nread;
  int32_t nwrite = BIO_write(pTls->readBio, sslBuf->buf, sslBuf->len);
  sslBuf->len = 0;

  if (cliMode && !SSL_is_init_finished(pTls->ssl)) {
    code = sslDoHandsShakeOrRead(pTls, pBuf, nread, &waitRead);
    if (code != 0) {
      tError("SSL handshake failed since %s", tstrerror(code));
      return code;
    }
    if (!waitRead) {
      return TSDB_CODE_SUCCESS;  // 等待对端
    }
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
    int err = SSL_get_error(pTls->ssl, nwrite);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
      return TSDB_CODE_SUCCESS;  // 等待对端
    } else {
      tError("SSL read failed: %s", ERR_reason_error_string(ERR_get_error()));
      return TSDB_CODE_THIRDPARTY_ERROR;
    }
  }
  return code;
}

void destroySSL(STransTLS* pTls) {
  if (pTls) {
    SSL_free(pTls->ssl);

    sslBufferDestroy(&pTls->readBuf);
    taosMemoryFree(pTls);
  }
}
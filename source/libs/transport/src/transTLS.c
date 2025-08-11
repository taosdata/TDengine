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

static int32_t sslReadDecryptedData(STransTLS* pTls, char* data, size_t ndata);
static int32_t sslWriteEncyptedData(STransTLS* pTls, const char* data, size_t ndata);

SSL_CTX* initSSLCtx(const char* cert_path, const char* key_path, const char* ca_path) {
  SSL_CTX* ctx = SSL_CTX_new(TLS_method());
  SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);

  if (SSL_CTX_use_certificate_file(ctx, cert_path, SSL_FILETYPE_PEM) <= 0) {
    tError("failed to load certificate file: %s", cert_path);
    SSL_CTX_free(ctx);
    return NULL;
  }

  if (SSL_CTX_use_PrivateKey_file(ctx, key_path, SSL_FILETYPE_PEM) <= 0) {
    tError("failed to load private key file: %s", key_path);
    SSL_CTX_free(ctx);
    return NULL;
  }

  if (SSL_CTX_check_private_key(ctx) <= 0) {
    tError("private key does not match the public certificate");
    SSL_CTX_free(ctx);
    return NULL;
  }

  if (SSL_CTX_load_verify_locations(ctx, ca_path, NULL) <= 0) {
    tError("failed to load CA file: %s", ca_path);
    SSL_CTX_free(ctx);
    return NULL;
  }
  return ctx;
}
void destroySSLCtx(SSL_CTX* ctx) {
  if (ctx) {
    SSL_CTX_free(ctx);
  }
}

void handleSSLError(SSL* ssl, int ret) {
  int err = SSL_get_error(ssl, ret);
  switch (err) {
    case SSL_ERROR_WANT_READ:
      // read
      break;
    case SSL_ERROR_WANT_WRITE:
      // write
      break;
    case SSL_ERROR_SSL:
      tError("SSL error: %s", ERR_reason_error_string(ERR_get_error()));
      break;
    default:
      tError("Unknown SSL error: %s", ERR_reason_error_string(ERR_get_error()));
      break;
  }
}

int32_t transTlsCtxCreate(const char* certPath, const char* keyPath, const char* caPath, STransTLSCtx** ppCtx) {
  int32_t code = 0;
  int32_t lino = 0;

  STransTLSCtx* pCtx = (STransTLSCtx*)taosMemCalloc(1, sizeof(STransTLSCtx));
  if (pCtx == NULL) {
    tError("Failed to allocate memory for TLS context");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pCtx->certfile = taosStrdupi(certPath);
  pCtx->keyfile = taosStrdupi(keyPath);
  pCtx->cafile = taosStrdupi(caPath);
  if (pCtx->certfile == NULL || pCtx->keyfile == NULL || pCtx->cafile == NULL) {
    code = terrno;
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

void transTlsCtxDestroy(STransTLSCtx* pCtx) {
  if (pCtx) {
    destroySSLCtx(pCtx->ssl_ctx);
    pCtx->ssl_ctx = NULL;

    taosMemoryFree(pCtx->certfile);
    taosMemoryFree(pCtx->keyfile);
    taosMemoryFree(pCtx->cafile);

    taosMemFree(pCtx);
  }
}

int32_t initSSL(STransTLSCtx* pCtx, STransTLS** ppTLs) {
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

  *ppTLs = pTls;

_error:
  if (code != 0) {
    destroySSL(pTls);
  }
  return code;
}

void setSSLMode(STransTLS* pTls) {
  SSL_set_accept_state(pTls->ssl);  // server mode
}
int32_t sslReadDecryptedData(STransTLS* pTls, char* data, size_t ndata);

void sslOnRead(STransTLS* pTls, size_t nread, const uv_buf_t* buf) {
  int32_t code = 0;
  int32_t nwrite = 0;
  if (nread > 0) {
    nwrite = BIO_write(pTls->readBio, buf->base, nread);

    if (!SSL_is_init_finished(pTls->ssl)) {
      int ret = SSL_connect(pTls->ssl);
      if (ret != 1) {
        int err = SSL_get_error(pTls->ssl, ret);
        if (err == SSL_ERROR_WANT_READ) {
          code = sslWriteEncyptedData(pTls, buf->base, nread);
        } else {
          tError("SSL connect error: %s", ERR_reason_error_string(ERR_get_error()));
          code = TSDB_CODE_THIRDPARTY_ERROR;
        }
      } else {
        SSL_write(pTls->ssl, buf->base, nread);
      }
    } else {
      code = sslReadDecryptedData(pTls, NULL, 0);
    }
  } else if (nread < 0) {
    tError("SSL read error: %s", ERR_reason_error_string(ERR_get_error()));
    return;
  }
  return;
}

int32_t sslReadDecryptedData(STransTLS* pTls, char* data, size_t ndata) {
  if (pTls == NULL || pTls->ssl == NULL) {
    tError("SSL is not initialized");
    return TSDB_CODE_INVALID_CFG;
  }

  int n = SSL_read(pTls->ssl, data, ndata);
  if (n <= 0) {
    handleSSLError(pTls->ssl, n);
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  return n;  // Return number of bytes read
}
int32_t sslWriteEncyptedData(STransTLS* pTls, const char* data, size_t ndata) {
  char    buf[4096] = {0};
  int32_t code = 0;
  int32_t total = 0;
  while (1) {
    int n = BIO_read(pTls->writeBio, (void*)data, ndata);
    if (n <= 0) {
      break;
    }
    total += n;
  }

  if (total > 0) {
    // uv_write
  }
  return code;
}
void sslOnWrite(STransTLS* pTls, const char* data, size_t ndata) {
  if (pTls == NULL || pTls->ssl == NULL) {
    tError("SSL is not initialized");
    return;
  }
  SSL_write(pTls->ssl, data, ndata);
}

void sslOnNewConn(STransTLS* pTls) { return; }

void sslOnConn(STransTLS* pTls) {
  
}

void destroySSL(STransTLS* pTLs) {
  if (pTLs && pTLs->pTlsCtx) {
    SSL_free(pTLs->ssl);
  }
}

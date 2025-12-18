/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
#if defined(TD_ENTERPRISE) && defined(LINUX)
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/provider.h>
#endif
#include "crypt.h"
#include "sm4.h"


int32_t Builtin_CBC_DecryptImpl(SCryptOpts *opts) {
  int32_t newLen = 0;
  int32_t count = 0;
  terrno = 0;

  while (count < opts->len) {
    if (SM4_CBC_Decrypt(opts->key, 16, opts->key, 16, opts->source + count, opts->unitLen, opts->result + count,
                        &newLen) == 0)
      count += newLen;
    else {
      terrno = TSDB_CODE_UTIL_CRYPT_INVALID_PARA;
      break;
    }
  }

  return count;
}

int32_t Builtin_CBC_EncryptImpl(SCryptOpts *opts) {
  int32_t newLen = 0;
  int32_t count = 0;
  terrno = 0;

  while (count < opts->len) {
    if (SM4_CBC_Encrypt(opts->key, 16, opts->key, 16, opts->source + count, opts->unitLen, opts->result + count,
                        &newLen) == 0)
      count += newLen;
    else {
      terrno = TSDB_CODE_UTIL_CRYPT_INVALID_PARA;
      break;
    }
  }

  return count;
}

#if defined(TD_ENTERPRISE) && defined(LINUX)
int32_t CBC_EncryptImpl(SCryptOpts *opts) {
  int outlen = -1;
  terrno = 0;

  if (opts == NULL || opts->pOsslAlgrName == NULL || strlen(opts->pOsslAlgrName) == 0) {
    terrno = TSDB_CODE_UTIL_CRYPT_INVALID_PARA;
    return outlen;
  }

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (ctx == NULL) {
    terrno = TSDB_CODE_UTIL_CRYPT_FAIL_NEW_CXT;
    uError("No context");
    return outlen;
  }

  EVP_CIPHER *cipher = EVP_CIPHER_fetch(NULL, opts->pOsslAlgrName, NULL);
  if (cipher == NULL) {
    uError("No evp! in encrypt %s", opts->pOsslAlgrName);
    ERR_print_errors_fp(stderr);
    terrno = TSDB_CODE_UTIL_CIPHER_NOT_EXIST;
    goto out;
  }

  if (!EVP_EncryptInit(ctx, cipher, (unsigned char *)opts->key, (unsigned char *)opts->key)) {
    uError("Failed EVP_EncryptInit!");
    terrno = TSDB_CODE_UTIL_CRYPT_FAIL_INIT;
    goto out;
  }

  if (0 == opts->len % opts->unitLen) {
    EVP_CIPHER_CTX_set_padding(ctx, 0);
  }

  if (!EVP_EncryptUpdate(ctx, opts->result, &outlen, opts->source, opts->len)) {
    uError("Failed EVP_EncryptUpdate!");
    terrno = TSDB_CODE_UTIL_CRYPT_FAIL_EXEC;
    goto out;
  }
  uTrace("source len:%d, result len:%d", opts->len, outlen);

  int tmplen = 0;
  if (!EVP_EncryptFinal(ctx, &opts->result[outlen], &tmplen)) {
    uError("Failed EVP_EncryptFinal!");
    terrno = TSDB_CODE_UTIL_CRYPT_FAIL_EXEC;
    goto out;
  }
  uTrace("tmplen:%d", tmplen);
  outlen += tmplen;

out:
  EVP_CIPHER_free(cipher);
  EVP_CIPHER_CTX_free(ctx);

  return outlen;
}

int32_t CBC_DecryptImpl(SCryptOpts *opts) {
  int outlen = -1;
  terrno = 0;

  if (opts == NULL || opts->pOsslAlgrName == NULL || strlen(opts->pOsslAlgrName) == 0) {
    terrno = TSDB_CODE_UTIL_CRYPT_INVALID_PARA;
    return outlen;
  }

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (ctx == NULL) {
    terrno = TSDB_CODE_UTIL_CRYPT_FAIL_NEW_CXT;
    uError("No context");
    return outlen;
  }

  EVP_CIPHER *cipher = EVP_CIPHER_fetch(NULL, opts->pOsslAlgrName, NULL);
  if (cipher == NULL) {
    uError("No evp! in decrypt %s", opts->pOsslAlgrName);
    ERR_print_errors_fp(stderr);
    terrno = TSDB_CODE_UTIL_CIPHER_NOT_EXIST;
    goto out;
  }

  if (!EVP_DecryptInit(ctx, cipher, (unsigned char *)opts->key, (unsigned char *)opts->key)) {
    uError("Failed EVP_DecryptInit!");
    terrno = TSDB_CODE_UTIL_CRYPT_FAIL_INIT;
    goto out;
  }

  if (0 == opts->len % opts->unitLen) {
    EVP_CIPHER_CTX_set_padding(ctx, 0);
  }

  if (!EVP_DecryptUpdate(ctx, opts->result, &outlen, opts->source, opts->len)) {
    uError("Failed EVP_DecryptUpdate!");
    terrno = TSDB_CODE_UTIL_CRYPT_FAIL_EXEC;
    goto out;
  }
  uTrace("source len:%d, result len:%d", opts->len, outlen);

  int tmplen = 0;
  if (!EVP_DecryptFinal(ctx, &opts->result[outlen], &tmplen)) {
    uError("Failed EVP_DecryptFinal!");
    terrno = TSDB_CODE_UTIL_CRYPT_FAIL_EXEC;
    goto out;
  }
  uTrace("tmplen:%d", tmplen);
  outlen += tmplen;

out:
  EVP_CIPHER_free(cipher);
  EVP_CIPHER_CTX_free(ctx);

  return outlen;
}

OSSL_PROVIDER *tsProvCustomized = NULL;
OSSL_PROVIDER *tsProvDefault = NULL;

int32_t cryptLoadProviders() {
  uInfo("load encrypt ext from %s", tsEncryptExtDir);

  tsProvCustomized = OSSL_PROVIDER_load(NULL, tsEncryptExtDir);
  if (tsProvCustomized == NULL) {
    uError("failed to load provider:%s", tsEncryptExtDir);
    return TSDB_CODE_DNODE_FAIL_LOAD_ENCRYPT_PROV;
  }

  uInfo("load encrypt ext %p", tsProvCustomized);

  tsProvDefault = OSSL_PROVIDER_load(NULL, "default");
  if (tsProvDefault == NULL) {
    uError("failed to load default provider");
    OSSL_PROVIDER_unload(tsProvCustomized);
    tsProvCustomized = NULL;
    return TSDB_CODE_DNODE_FAIL_LOAD_ENCRYPT_PROV;
  }

  uInfo("load encrypt default ext %p", tsProvDefault);

  return 0;
}

void cryptUnloadProviders() {
  if (tsProvCustomized != NULL) OSSL_PROVIDER_unload(tsProvCustomized);
  if (tsProvDefault != NULL) OSSL_PROVIDER_unload(tsProvDefault);
}
#endif
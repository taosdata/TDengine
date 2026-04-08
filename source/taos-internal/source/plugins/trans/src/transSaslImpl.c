
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



#if defined(TD_ENTERPRISE) && defined(LINUX)

#define SASL_HOST "localhost"

// #define SASL_CMD_INIT      "SASL_EXTERNAL_INIT"
// #define SASL_CMD_AUTH_OK   "SASL_AUTH_OK"
// #define SASL_CMD_AUTH_FIAL "SASL_AUTH_FAIL"

#define SASL_MECHANISM_SCRAM_SHA256 "SCRAM-SHA-256"

enum { STATE_HANDSHAKE = 0, STATE_SALA_AUTH, STATE_READY, STATE_CLOSING } SASL_STATE;

enum { SASL_STATUS_INIT = 0, SASL_STATUS_AUTHING, SASL_STATUS_AUTHED, SASL_STATUS_ERROR } SASL_STATUS_T;

typedef int32_t (*authDoFunc)(SSaslConn* p, const char* input, int32_t len);

int32_t authInitFp(SSaslConn* p, const char* input, int32_t len);

int32_t authConnFp(SSaslConn* p, const char* input, int32_t len);

int32_t authDoingFp(SSaslConn* p, const char* input, int32_t len);

int32_t authDoneFp(SSaslConn* p, const char* input, int32_t len);

const char *authStateStr[] = {"INIT", "AUTHING", "AUTHED", "ERROR"};
authDoFunc auFunc[] = {authInitFp, authDoingFp, authDoneFp};

static int32_t saslConnListMech(SSaslConn* pConn, const char* tgt);

static int saslCallBackFn(void *context, int id, const char** result, unsigned* len); 

static int serverVerifyCallBackFn(sasl_conn_t *conn,
                          void *context,
                          const char *requested_user,
                          unsigned rlen,
                          const char *auth_identity,
                          unsigned alen,
                          const char *realm,
                          unsigned prop_maxbuf,
                          const char **out_user,
                          unsigned *out_ulen,
                          struct propctx *propctx) {
    
    tInfo("SASL verify callback: auth_identity=%s, requested_user=%s", auth_identity, requested_user);
    
    // 对于 EXTERNAL 机制，auth_identity 应该来自 TLS 证书
    if (auth_identity && alen > 0) {
        *out_user = auth_identity;
        *out_ulen = alen;
        tInfo("SASL EXTERNAL authentication accepted for: %s", auth_identity);
        return SASL_OK;
    }
    
    tWarn("SASL EXTERNAL authentication failed: no identity provided");
    return SASL_BADAUTH;
}
int canonUserCallBackFn(sasl_conn_t *conn,
                       void *context,
                       const char *in,
                       unsigned inlen,
                       unsigned flags,
                       const char *user_realm,
                       char *out,
                       unsigned out_max,
                       unsigned *out_len) {
    
    tInfo("Canonicalizing username: '%.*s'\n", inlen, in);
    tInfo("User realm: %s\n", user_realm ? user_realm : "NULL");
    
    // 检查输出缓冲区是否足够大
    if (inlen >= out_max) {
        tError("Output buffer too small\n");
        return SASL_BUFOVER;
    }
    
    const char *start = in;
    const char *end = in + inlen - 1;
    
    // 跳过前导空格
    while (start <= end && isspace(*start)) {
        start++;
    }
    
    // 跳过尾随空格
    while (end >= start && isspace(*end)) {
        end--;
    }
    
    unsigned actual_len = end - start + 1;
    
    if (actual_len >= out_max) {
        return SASL_BUFOVER;
    }
    
    // 转换为小写
    for (unsigned i = 0; i < actual_len; i++) {
        out[i] = tolower(start[i]);
    }
    
    *out_len = actual_len;
    out[actual_len] = '\0';
    
    tInfo("Canonicalized username: '%s'\n", out);
    
    return SASL_OK;
}



static int saslLogCallBack(void *context, int level, const char *message) {
  tInfo("SASL log level %d: %s", level, message);
  return SASL_OK;
}

sasl_callback_t callbacks[] = {
      {SASL_CB_LOG, (int (*)())saslLogCallBack, NULL},
      {SASL_CB_USER, (int (*)())saslCallBackFn, NULL},
      {SASL_CB_PASS, (int (*)())saslCallBackFn, NULL},
      {SASL_CB_AUTHNAME, (int (*)())saslCallBackFn, NULL},
      {SASL_CB_GETREALM, (int (*)())saslCallBackFn, NULL},
      {SASL_CB_CANON_USER, NULL, NULL},
      {SASL_CB_LIST_END, NULL, NULL}
};

void saslLibInitImpl() {
  int rc = sasl_client_init(callbacks);
  if (rc != SASL_OK) {
    tError("sasl_client_init failed: %s", sasl_errstring(rc, NULL, NULL));
    return;
  }

  rc = sasl_server_init(NULL, "tdengine");
  if (rc != SASL_OK) {
    tError("sasl_server_init failed: %s", sasl_errstring(rc, NULL, NULL));
    return;
  }
}

void saslLibCleanupImpl() { sasl_done(); }


int32_t saslConnCreateImpl(SSaslConn** ppConn, int8_t server) {
  int32_t code = 0;
  int32_t lino = 0;

  SSaslConn* pConn = (SSaslConn*)taosMemCalloc(1, sizeof(SSaslConn));
  if (pConn == NULL) {
    tError("saslConnCreate failed to alloc memory");
    return terrno;
  }
  memset(pConn, 0, sizeof(SSaslConn));
  pConn->state = SASL_STATUS_INIT;
  pConn->isAuthed = 0;

  pConn->server = server;

  code = saslConnInit(pConn);
  TAOS_CHECK_GOTO(code, &lino, _error);

  *ppConn = pConn;

_error:
  if (code != 0) {
    tError("saslConnCreate failed, code:%d", code);
    if (pConn != NULL) {
      saslConnCleanup(pConn);
    }
    *ppConn = NULL;
  }
  return code;
}
    
void saslConnSetStateImpl(SSaslConn* pConn, int32_t state) {
  if (pConn == NULL) {
    return;
  }
  pConn->state = state;
}
static int saslOptCallBackFn(sasl_conn_t *conn,
                          const char *pluginName,
                          const char *option,
                          const char **result,
                          unsigned *len) {
  tInfo("saslOptCallBackFn is supported");
  return SASL_OK;
}
static int saslCallBackFn(void* context, int id, const char** result, unsigned* len) {
  tInfo("callback is supported");
  if (id == SASL_CB_USER) {
    tInfo("callback user is supported");
   // *result = taosStrdup("tdengineUser");
    // if (len) *len = (unsigned)strlen(*result);
    return SASL_OK;
  } else if (id == SASL_CB_PASS) {
    tInfo("callback pass is supported");
    *result = taosStrdup("tdenginePass");
    if (len) *len = (unsigned)strlen(*result);
    return SASL_OK;
  } else if (id == SASL_CB_LANGUAGE){
    tInfo("callback language is supported");
    return SASL_OK;
  } else if (id == SASL_CB_AUTHNAME){ 
    tInfo("callback authname is supported");
    return SASL_OK;
  } else if (id == SASL_CB_GETREALM){
    tInfo("callback getrealm is supported");
    return SASL_OK;
  } else if (id == SASL_CB_GETOPT){
    tInfo("callback getopt is supported");
    // static const char* dummy_opt = "dummy";
    // *result = taosStrdup(dummy_opt);
    return SASL_OK;
  } else {
    tInfo("callback not supported, id:%d", id);
    return SASL_FAIL;
  }
  return SASL_FAIL;
}
int32_t saslConnInitImpl(SSaslConn* pConn) {
  int32_t code = 0;
  int32_t lino = 0;
  int     result;


  code = saslBufferInit(&pConn->in, 1024);
  TAOS_CHECK_GOTO(code, &lino, _error);

  code = saslBufferInit(&pConn->out, 1024);
  TAOS_CHECK_GOTO(code, &lino, _error);

  code = saslBufferInit(&pConn->authInfo, 1024);
  TAOS_CHECK_GOTO(code, &lino, _error);

  if (pConn->server) {
    result = sasl_server_new("tdengine", SASL_HOST, NULL, NULL, NULL, NULL, 0, &pConn->conn);
    if (result != SASL_OK) {
      tError("sasl_server_new failed: %s", sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }
  } else {
    result = sasl_client_new("tdengine", SASL_HOST, NULL, NULL, NULL, 0, &pConn->conn);
    if (result != SASL_OK) {
      tError("sasl_client_new failed: %s", sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }
  }
  pConn->completed = 0;
  pConn->isAuthed = 0;

_error:
  if (code != 0) {
    tError("saslConnInit failed, code:%d", code);
  }
  return code;

}

void  saslConnCleanupImpl(SSaslConn* pConn){
  if (pConn == NULL) {
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

  saslBufferCleanup(&pConn->in);
  saslBufferCleanup(&pConn->out);
  saslBufferCleanup(&pConn->authInfo);

  taosMemFree(pConn);

}

int32_t saslConnEncodeImpl(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen) {
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
int32_t saslConnDecodeImpl(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen) {
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

int8_t  saslConnShoudDoAuthImpl(SSaslConn* pConn) {
  if (pConn == NULL) {
    return 1;
  }
  return pConn->isAuthed ? 1 : 0;
}

static int32_t saslConnListMech(SSaslConn* pConn, const char* tgt) {
  int32_t code = 0;
  int     result = 0;
  int8_t  found = 0;

  if (pConn == NULL || pConn->conn == NULL) {
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  uint32_t    len = 0;
  int32_t     count = 0;
  const char* mechList = NULL;
  result = sasl_listmech(pConn->conn, NULL, NULL, "", "", &mechList, &len, &count);
  if (result != SASL_OK) {
    tError("conn %p sasl_listmech failed: %s", pConn->pUvConn, sasl_errstring(result, NULL, NULL));
    code = TSDB_CODE_THIRDPARTY_ERROR;
  } else {
    tInfo("conn %p get Supported SASL mechanisms: %s", pConn->pUvConn, mechList);
  }

  if (mechList != NULL) {
    tInfo("conn %p Supported SASL mechanisms: %s", pConn->pUvConn, mechList);
    if (strstr(mechList, tgt) != NULL) {
      tInfo("conn %p Found target SASL mechanism: %s", pConn->pUvConn, tgt);
      found = 1;
    }
  }

  if (!found) {
    tError("conn %p Target SASL mechanism %s not supported", pConn->pUvConn, tgt);
    code = TSDB_CODE_THIRDPARTY_ERROR;
  }

  return code;
}
int32_t saslConnSetAuthIdImpl(SSaslConn* pConn, char* authId) {
  int32_t code = 0;
  if (pConn->authId[0] == '\0') {
    strncpy(pConn->authId, authId, sizeof(pConn->authId) - 1);
    pConn->authId[sizeof(pConn->authId) - 1] = '\0';
  } else {
    tInfo("sasl authId is already set to %s", pConn->authId);
  }

  return code;
}

int32_t saslConnHandleAuthImpl(SSaslConn* pConn, const char* input, int32_t len) {
  int32_t code = 0;

  if (pConn == NULL || pConn->conn == NULL) {
    return TSDB_CODE_THIRDPARTY_ERROR;
  }

  if (pConn->state >= SASL_STATUS_INIT && pConn->state < SASL_STATUS_ERROR) {
    tInfo("conn %p sasl in state: %s", pConn->pUvConn, authStateStr[pConn->state]);
    code = auFunc[pConn->state](pConn, input, len);

    tInfo("conn %p sasl out state: %s", pConn->pUvConn, authStateStr[pConn->state]);

    code = 0; 
  } else {
    code = TSDB_CODE_THIRDPARTY_ERROR;
    code = 0;
  }
  return code;
}

static int32_t authConnCheck(SSaslConn* p) {
  int32_t code = 0;

  if (p == NULL || p->conn == NULL) {
    return TSDB_CODE_THIRDPARTY_ERROR;
  }
  return code;
}

int32_t authInitFp(SSaslConn* p, const char* input, int32_t len) {
  int32_t code = 0;
  int32_t lino = 0;

  if (p->state != SASL_STATUS_INIT) {
    p->state = SASL_STATUS_ERROR;
    code = TSDB_CODE_THIRDPARTY_ERROR;
    return code;
  }
  int result = 0;

  const char* mechlist = "EXTERNAL";
  const char* in = NULL;
  uint32_t    inlen = 0;
  const char* out = NULL;
  uint32_t    outlen = 0;

  //result = sasl_setprop(p->conn, SASL_LOG_LEVEL, (const void*)SASL_LOG_NONE); 

  if (!p->server) {
    // sasl_ssf_t ssf = 256; 
    // result = sasl_setprop(p->conn, SASL_SSF_EXTERNAL, &ssf);
    // if (result != SASL_OK) {
    //   tError("sasl_setprop SASL_AUTH_EXTERNAL failed: %s", sasl_errstring(result, NULL, NULL));
    //   code = TSDB_CODE_THIRDPARTY_ERROR;
    //   TAOS_CHECK_GOTO(code, &lino, _error);
    // }



    code = saslConnListMech(p, mechlist);
    code = 0;

    result = sasl_setprop(p->conn, SASL_AUTH_EXTERNAL, p->authId);
    if (result != SASL_OK) {
      tError("conn %p sasl_setprop SASL_AUTH_EXTERNAL failed: %s", p->pUvConn, sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }

    const char *extIdentify = NULL;
    result = sasl_getprop(p->conn, SASL_AUTH_EXTERNAL, (const void**)&extIdentify);
    if (result != SASL_OK) {
      tInfo("conn %p sasl_getprop SASL_AUTH_EXTERNAL failed: %s", p->pUvConn, sasl_errstring(result, NULL, NULL));
    } else {
      tInfo("conn %p sasl_getprop SASL_AUTH_EXTERNAL: %s", p->pUvConn, extIdentify);
    }

    code = saslConnListMech(p, mechlist);
    code = 0;
    //TAOS_CHECK_GOTO(code, &lino, _error);
    result = sasl_client_start(p->conn, mechlist, NULL, &out, &outlen, NULL);
    if (result != SASL_OK && result != SASL_CONTINUE) {
      tError("conn %p sasl_client_start failed: %s", p->pUvConn, sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }

    code = saslBufferAppend(&p->out, (uint8_t*)out, outlen);
    TAOS_CHECK_GOTO(code, &lino, _error);

  } else {
    // sasl_ssf_t ssf = 256; 
    // result = sasl_setprop(p->conn, SASL_SSF_EXTERNAL, &ssf);
    // if (result != SASL_OK) {
    //   tError("sasl_setprop SASL_AUTH_EXTERNAL failed: %s", sasl_errstring(result, NULL, NULL));
    //   code = TSDB_CODE_THIRDPARTY_ERROR;
    //   TAOS_CHECK_GOTO(code, &lino, _error);
    // }

    result = sasl_setprop(p->conn, SASL_AUTH_EXTERNAL, "localhost");
    if (result != SASL_OK) {
      tError("conn %p sasl_setprop SASL_AUTH_EXTERNAL failed: %s", p->pUvConn, sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      TAOS_CHECK_GOTO(code, &lino, _error);
    }

    // code = saslConnListMech(p, mechlist);
    // TAOS_CHECK_GOTO(code, &lino, _error);

    result = sasl_server_start(p->conn, mechlist, in, inlen, &out, &outlen);
    if (result != SASL_OK && result != SASL_CONTINUE) {
      tError("sasl_server_start failed: %s", sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      TAOS_CHECK_GOTO(code, &lino, _error);
    } else {
      tInfo("conn %p sasl_server_start success", p->pUvConn);
    }

    code = saslBufferAppend(&p->out, (uint8_t*)out, outlen);
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  p->state = p->state + 1;

_error:
  if (code != 0) {
    tError("conn %p authInitFp failed, code:%d", p->pUvConn, code);
    p->state = SASL_STATUS_ERROR;
  }

  return code;
}

int32_t authDoingFp(SSaslConn* p, const char* input, int32_t len) {
  int32_t code = 0;
  if (p->state != SASL_STATUS_AUTHING) {
    p->state = SASL_STATUS_ERROR;
    code = TSDB_CODE_THIRDPARTY_ERROR;
    return code;
  }
  const char* cliOut = NULL;
  unsigned    cliOutLen = 0;

  int result = 0;
  if (p->server) {
    code = saslConnListMech(p, "EXTERNAL");
    if (code != 0) {
      return code;
    }

    const char *extIdentify = NULL;
    result = sasl_getprop(p->conn, SASL_AUTH_EXTERNAL, (const void**)&extIdentify);
    if (result != SASL_OK) {
      tInfo("svr sasl_getprop SASL_AUTH_EXTERNAL failed: %s", sasl_errstring(result, NULL, NULL));
    } else {
      tInfo("svr sasl_getprop SASL_AUTH_EXTERNAL: %s", extIdentify);
    }

    result = sasl_getprop(p->conn, SASL_MECHNAME, (const void**)&extIdentify);
    if (result != SASL_OK) {
      tInfo("svr sasl_getprop cli SASL_MECHNAME failed: %s", sasl_errstring(result, NULL, NULL));
    } else {
      tInfo("svr sasl_getprop cli SASL_MECHNAME: %s", extIdentify);
    }
    
    result = sasl_setprop(p->conn, SASL_AUTH_EXTERNAL, "localhost");
    if (result != SASL_OK) {
      tError("sasl_setprop SASL_AUTH_EXTERNAL failed: %s", sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
      return code;
      //TAOS_CHECK_GOTO(code, &lino, _error);
    }


    result = sasl_server_step(p->conn, input, len, &cliOut, &cliOutLen);
    if (result == SASL_OK) {
      p->completed = 1;
      p->isAuthed = 1;

      result = sasl_getprop(p->conn, SASL_USERNAME, (const void**)&p->authUser);
      if (result != SASL_OK) {
        tError("sasl_getprop SASL_USERNAME failed: %s", sasl_errstring(result, NULL, NULL));
        code = TSDB_CODE_THIRDPARTY_ERROR;
      }

      code = saslBufferAppend(&p->out, (uint8_t*)cliOut, cliOutLen);

    } else if (result == SASL_CONTINUE) {
      tInfo("sasl server continue to auth, sasl conn %p, conn %p", p, p->conn);
      code = saslBufferAppend(&p->authInfo, (uint8_t*)cliOut, (int32_t)cliOutLen);
      if (code != 0) {
        tError("saslConnHandleAuth failed to append auth info, code:%d", code);
        return code;
      }
    } else {
      tError("sasl_server_step failed: %s", sasl_errstring(result, NULL, NULL));
      code = TSDB_CODE_THIRDPARTY_ERROR;
    }
  } else {
    int result = sasl_client_step(p->conn, input, len, NULL, &cliOut, &cliOutLen);
    if (result == SASL_OK) {
      p->completed = 1;
      p->isAuthed = 1;
      tInfo("sasl client auth success, sasl conn %p, conn %p", p, p->conn);
    } else if (result == SASL_CONTINUE) {
      tInfo("sasl client continue to auth, sasl conn %p, conn %p", p, p->conn);
    } else {
      const char *detail = sasl_errdetail(p->conn);
      tError("sasl_client_step failed: %s, detail:%s", sasl_errstring(result, NULL, NULL), detail ? detail : "");
      code = TSDB_CODE_THIRDPARTY_ERROR;
    }
  }

  p->state = p->state + 1;

  p->isAuthed = 1; 

  return code;
}

int32_t authDoneFp(SSaslConn* p, const char* input, int32_t len) {
  int32_t code = 0;

  if (p->state != SASL_STATUS_AUTHED) {
    p->state = SASL_STATUS_ERROR;
  }
  p->isAuthed = 1;

  return code;
}

// sasl buffer func
int32_t saslBufferInitImpl(SSaslBuffer* buf, int32_t cap) {
  int32_t code = 0;
  buf->buf = (uint8_t*)taosMemCalloc(1, cap);

  if (buf->buf == NULL) {
    tError("saslBufferInit failed to alloc memory");
    return terrno;
  }

  buf->cap = cap;
  buf->len = 0;
  buf->invalid = 0;

  return 0;

}
int32_t saslBufferAppendImpl(SSaslBuffer* buf, uint8_t* data, int32_t len) {
  int32_t code = 0;

  if (buf->len + len > buf->cap) {
    while (buf->len + len > buf->cap) {
      buf->cap *= 2;
    }

    uint8_t* newBuf = (uint8_t*)taosMemCalloc(1, buf->cap);
    if (newBuf == NULL) {
      tError("saslBufferAppend failed to alloc memory");
      return terrno;
    }
    memcpy(newBuf, buf->buf, buf->len);

    taosMemFree(buf->buf);
    buf->buf = newBuf;
  }
  memcpy(buf->buf + buf->len, data, len);
  buf->len += len;

  return code;

}
void  saslBufferCleanupImpl(SSaslBuffer* buf) {
  if (buf->buf != NULL) {
    taosMemFree(buf->buf);
    buf->buf = NULL;
  }
  buf->cap = 0;
  buf->len = 0;
  buf->invalid = 0;

}
void saslBufferClearImpl(SSaslBuffer* buf) {
  buf->len = 0;
  buf->invalid = 0; 
}

#else 
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
void saslConnSetStateImpl(SSaslConn * pConn, int32_t state) {
  return TSDB_CODE_INVALID_CFG; 

}
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

#endif

// clang-format on
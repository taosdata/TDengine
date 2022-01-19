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
#ifdef USE_UV

#include "transComm.h"

int rpcAuthenticateMsg(void* pMsg, int msgLen, void* pAuth, void* pKey) {
  T_MD5_CTX context;
  int       ret = -1;

  tMD5Init(&context);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Update(&context, (uint8_t*)pMsg, msgLen);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Final(&context);

  if (memcmp(context.digest, pAuth, sizeof(context.digest)) == 0) ret = 0;

  return ret;
}
void rpcBuildAuthHead(void* pMsg, int msgLen, void* pAuth, void* pKey) {
  T_MD5_CTX context;

  tMD5Init(&context);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Update(&context, (uint8_t*)pMsg, msgLen);
  tMD5Update(&context, (uint8_t*)pKey, TSDB_PASSWORD_LEN);
  tMD5Final(&context);

  memcpy(pAuth, context.digest, sizeof(context.digest));
}

int32_t rpcCompressRpcMsg(char* pCont, int32_t contLen) {
  SRpcHead* pHead = rpcHeadFromCont(pCont);
  int32_t   finalLen = 0;
  int       overhead = sizeof(SRpcComp);

  if (!NEEDTO_COMPRESSS_MSG(contLen)) {
    return contLen;
  }

  char* buf = malloc(contLen + overhead + 8);  // 8 extra bytes
  if (buf == NULL) {
    tError("failed to allocate memory for rpc msg compression, contLen:%d", contLen);
    return contLen;
  }

  int32_t compLen = LZ4_compress_default(pCont, buf, contLen, contLen + overhead);
  tDebug("compress rpc msg, before:%d, after:%d, overhead:%d", contLen, compLen, overhead);

  /*
   * only the compressed size is less than the value of contLen - overhead, the compression is applied
   * The first four bytes is set to 0, the second four bytes are utilized to keep the original length of message
   */
  if (compLen > 0 && compLen < contLen - overhead) {
    SRpcComp* pComp = (SRpcComp*)pCont;
    pComp->reserved = 0;
    pComp->contLen = htonl(contLen);
    memcpy(pCont + overhead, buf, compLen);

    pHead->comp = 1;
    tDebug("compress rpc msg, before:%d, after:%d", contLen, compLen);
    finalLen = compLen + overhead;
  } else {
    finalLen = contLen;
  }

  free(buf);
  return finalLen;
}

SRpcHead* rpcDecompressRpcMsg(SRpcHead* pHead) {
  int       overhead = sizeof(SRpcComp);
  SRpcHead* pNewHead = NULL;
  uint8_t*  pCont = pHead->content;
  SRpcComp* pComp = (SRpcComp*)pHead->content;

  if (pHead->comp) {
    // decompress the content
    assert(pComp->reserved == 0);
    int contLen = htonl(pComp->contLen);

    // prepare the temporary buffer to decompress message
    char* temp = (char*)malloc(contLen + RPC_MSG_OVERHEAD);
    pNewHead = (SRpcHead*)(temp + sizeof(SRpcReqContext));  // reserve SRpcReqContext

    if (pNewHead) {
      int compLen = rpcContLenFromMsg(pHead->msgLen) - overhead;
      int origLen = LZ4_decompress_safe((char*)(pCont + overhead), (char*)pNewHead->content, compLen, contLen);
      assert(origLen == contLen);

      memcpy(pNewHead, pHead, sizeof(SRpcHead));
      pNewHead->msgLen = rpcMsgLenFromCont(origLen);
      /// rpcFreeMsg(pHead);  // free the compressed message buffer
      pHead = pNewHead;
      tTrace("decomp malloc mem:%p", temp);
    } else {
      tError("failed to allocate memory to decompress msg, contLen:%d", contLen);
    }
  }

  return pHead;
}

#endif

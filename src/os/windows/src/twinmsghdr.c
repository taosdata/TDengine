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

#include <winsock2.h>

void taosFreeMsgHdr(void *hdr) {
    WSAMSG *msgHdr = (WSAMSG *)hdr;
    free(msgHdr->lpBuffers);
}

int taosMsgHdrSize(void *hdr) {
    WSAMSG *msgHdr = (WSAMSG *)hdr;
    return msgHdr->dwBufferCount;
}

void taosSendMsgHdr(void *hdr, int fd) {
    WSAMSG *msgHdr = (WSAMSG *)hdr;
    DWORD len;

    WSASendMsg(fd, msgHdr, 0, &len, 0, 0);
    msgHdr->dwBufferCount = 0;
}

void taosInitMsgHdr(void **hdr, void *dest, int maxPkts) {
    WSAMSG *msgHdr = (WSAMSG *)malloc(sizeof(WSAMSG));
    memset(msgHdr, 0, sizeof(WSAMSG));
    *hdr = msgHdr;

    // see ws2def.h
    // the size of LPSOCKADDR and sockaddr_in * is same, so it's safe
    msgHdr->name = (LPSOCKADDR)dest;
    msgHdr->namelen = sizeof(struct sockaddr_in);
    int size = sizeof(WSABUF) * maxPkts;
    msgHdr->lpBuffers = (LPWSABUF)malloc(size);
    memset(msgHdr->lpBuffers, 0, size);
    msgHdr->dwBufferCount = 0;
}

void taosSetMsgHdrData(void *hdr, char *data, int dataLen) {
    WSAMSG *msgHdr = (WSAMSG *)hdr;
    msgHdr->lpBuffers[msgHdr->dwBufferCount].buf = data;
    msgHdr->lpBuffers[msgHdr->dwBufferCount].len = dataLen;
    msgHdr->dwBufferCount++;
}


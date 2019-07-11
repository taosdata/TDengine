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

#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

void taosFreeMsgHdr(void *hdr) {
  struct msghdr *msgHdr = (struct msghdr *)hdr;
  free(msgHdr->msg_iov);
}

int taosMsgHdrSize(void *hdr) {
  struct msghdr *msgHdr = (struct msghdr *)hdr;
  return (int)msgHdr->msg_iovlen;
}

void taosSendMsgHdr(void *hdr, int fd) {
  struct msghdr *msgHdr = (struct msghdr *)hdr;
  sendmsg(fd, msgHdr, 0);
  msgHdr->msg_iovlen = 0;
}

void taosInitMsgHdr(void **hdr, void *dest, int maxPkts) {
  struct msghdr *msgHdr = (struct msghdr *)malloc(sizeof(struct msghdr));
  memset(msgHdr, 0, sizeof(struct msghdr));
  *hdr = msgHdr;
  struct sockaddr_in *destAdd = (struct sockaddr_in *)dest;

  msgHdr->msg_name = destAdd;
  msgHdr->msg_namelen = sizeof(struct sockaddr_in);
  int size = (int)sizeof(struct iovec) * maxPkts;
  msgHdr->msg_iov = (struct iovec *)malloc((size_t)size);
  memset(msgHdr->msg_iov, 0, (size_t)size);
}

void taosSetMsgHdrData(void *hdr, char *data, int dataLen) {
  struct msghdr *msgHdr = (struct msghdr *)hdr;
  msgHdr->msg_iov[msgHdr->msg_iovlen].iov_base = data;
  msgHdr->msg_iov[msgHdr->msg_iovlen].iov_len = (size_t)dataLen;
  msgHdr->msg_iovlen++;
}

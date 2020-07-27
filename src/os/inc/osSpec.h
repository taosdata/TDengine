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

#ifndef TDENGINE_OS_SPEC_H
#define TDENGINE_OS_SPEC_H

#ifdef __cplusplus
extern "C" {
#endif

ssize_t taosTReadImp(int fd, void *buf, size_t count);
ssize_t taosTWriteImp(int fd, void *buf, size_t count);
ssize_t taosTSendFileImp(int dfd, int sfd, off_t *offset, size_t size);

#ifdef TAOS_RANDOM_NETWORK_FAIL
  ssize_t taosSendRandomFail(int sockfd, const void *buf, size_t len, int flags);
  ssize_t taosSendToRandomFail(int sockfd, const void *buf, size_t len, int flags, const struct sockaddr *dest_addr, socklen_t addrlen);
  ssize_t taosReadSocketRandomFail(int fd, void *buf, size_t count);
  ssize_t taosWriteSocketRandomFail(int fd, const void *buf, size_t count);

  #define taosSend(sockfd, buf, len, flags) taosSendRandomFail(sockfd, buf, len, flags)
  #define taosSendto(sockfd, buf, len, flags, dest_addr, addrlen) taosSendToRandomFail(sockfd, buf, len, flags, dest_addr, addrlen)
  #define taosReadSocket(fd, buf, len) taosReadSocketRandomFail(fd, buf, len)
  #define taosWriteSocket(fd, buf, len) taosWriteSocketRandomFail(fd, buf, len)
#else
  #ifndef TAOS_OS_FUNC_SOCKET
    #define taosSend(sockfd, buf, len, flags) send(sockfd, buf, len, flags)
    #define taosSendto(sockfd, buf, len, flags, dest_addr, addrlen) sendto(sockfd, buf, len, flags, dest_addr, addrlen)
    #define taosReadSocket(fd, buf, len) read(fd, buf, len)
    #define taosWriteSocket(fd, buf, len) write(fd, buf, len)
  #endif
#endif

#ifdef TAOS_RANDOM_FILE_FAIL
  void taosSetRandomFileFailFactor(int factor);
  void taosSetRandomFileFailOutput(const char *path);
  ssize_t taosReadFileRandomFail(int fd, void *buf, size_t count, const char *file, uint32_t line);
  ssize_t taosWriteFileRandomFail(int fd, void *buf, size_t count, const char *file, uint32_t line);
  off_t taosLSeekRandomFail(int fd, off_t offset, int whence, const char *file, uint32_t line);

  #define taosTRead(fd, buf, count) taosReadFileRandomFail(fd, buf, count, __FILE__, __LINE__)
  #define taosTWrite(fd, buf, count) taosWriteFileRandomFail(fd, buf, count, __FILE__, __LINE__)
  #define taosLSeek(fd, offset, whence) taosLSeekRandomFail(fd, offset, whence, __FILE__, __LINE__)
  #define taosTSendFile(dfd, sfd, offset, size) taosTSendFileImp(dfd, sfd, offset, size);
#else
  #ifndef TAOS_OS_FUNC_FILE
    #define taosTRead(fd, buf, count) taosTReadImp(fd, buf, count)
    #define taosTWrite(fd, buf, count) taosTWriteImp(fd, buf, count)
    #define taosLSeek(fd, offset, whence) lseek(fd, offset, whence)
    #define taosTSendFile(dfd, sfd, offset, size) taosTSendFileImp(dfd, sfd, offset, size)
  #endif
#endif


#ifdef __cplusplus
}
#endif

#endif

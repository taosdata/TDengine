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

#ifndef TDENGINE_OS_FILE_H
#define TDENGINE_OS_FILE_H

#ifdef __cplusplus
extern "C" {
#endif

ssize_t taosTReadImp(int fd, void *buf, size_t count);
ssize_t taosTWriteImp(int fd, void *buf, size_t count);

ssize_t taosTSendFileImp(int dfd, int sfd, off_t *offset, size_t size);
int     taosFSendFileImp(FILE* out_file, FILE* in_file, int64_t* offset, int32_t count);

#ifndef TAOS_OS_FUNC_FILE_SENDIFLE
  #define taosTSendFile(dfd, sfd, offset, size) taosTSendFileImp(dfd, sfd, offset, size)
  #define taosFSendFile(outfile, infile, offset, count) taosTSendFileImp(fileno(outfile), fileno(infile), offset, size)
#endif

#define taosTRead(fd, buf, count) taosTReadImp(fd, buf, count)
#define taosTWrite(fd, buf, count) taosTWriteImp(fd, buf, count)
#define taosLSeek(fd, offset, whence) lseek(fd, offset, whence)

#ifdef TAOS_RANDOM_FILE_FAIL
  void taosSetRandomFileFailFactor(int factor);
  void taosSetRandomFileFailOutput(const char *path);
  #ifdef TAOS_RANDOM_FILE_FAIL_TEST
    ssize_t taosReadFileRandomFail(int fd, void *buf, size_t count, const char *file, uint32_t line);
    ssize_t taosWriteFileRandomFail(int fd, void *buf, size_t count, const char *file, uint32_t line);
    off_t taosLSeekRandomFail(int fd, off_t offset, int whence, const char *file, uint32_t line);
    #undef taosTRead
    #undef taosTWrite
    #undef taosLSeek
    #define taosTRead(fd, buf, count) taosReadFileRandomFail(fd, buf, count, __FILE__, __LINE__)
    #define taosTWrite(fd, buf, count) taosWriteFileRandomFail(fd, buf, count, __FILE__, __LINE__)
    #define taosLSeek(fd, offset, whence) taosLSeekRandomFail(fd, offset, whence, __FILE__, __LINE__)
  #endif  
#endif 

int32_t taosFileRename(char *fullPath, char *suffix, char delimiter, char **dstPath);

// TAOS_OS_FUNC_FILE_GETTMPFILEPATH
void taosGetTmpfilePath(const char *fileNamePrefix, char *dstPath);

#ifdef TAOS_OS_FUNC_FILE_ISDIR
  #define S_ISDIR(m) (((m) & 0170000) == (0040000))  
#endif

#ifdef TAOS_OS_FUNC_FILE_ISREG
  #define S_ISREG(m) !(S_ISDIR(m))
#endif

#ifdef TAOS_OS_FUNC_FILE_ISLNK
  #define S_ISLNK(m) 0
#endif

#ifndef TAOS_OS_FUNC_FILE_FTRUNCATE
  #define taosFtruncate ftruncate
#endif

#ifdef __cplusplus
}
#endif

#endif

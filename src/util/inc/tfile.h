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

#ifndef TDENGINE_TFILE_H
#define TDENGINE_TFILE_H

#ifdef TAOS_RANDOM_FILE_FAIL

ssize_t taos_tread(int fd, void *buf, size_t count);
ssize_t taos_twrite(int fd, void *buf, size_t count);
off_t taos_lseek(int fd, off_t offset, int whence);

#define tread(fd, buf, count)  taos_tread(fd, buf, count)
#define twrite(fd, buf, count)  taos_twrite(fd, buf, count)
#define lseek(fd, offset, whence)  taos_lseek(fd, offset, whence)

#endif  // TAOS_RANDOM_FILE_FAIL

#endif  // TDENGINE_TFILE_H

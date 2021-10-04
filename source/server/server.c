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
#include "os.h"
#include "tulog.h"
#include "trpc.h"
#include "dnode.h"

int main(int argc, char const *argv[]) {
  struct Dnode *dnode = dnodeCreateInstance();
  if (dnode == NULL) {
    uInfo("Failed to start TDengine, please check the log at:%s", tsLogDir);
    exit(EXIT_FAILURE);
  }

  uInfo("Started TDengine service successfully.");

  // if (tsem_wait(&exitSem) != 0) {
  //   syslog(LOG_ERR, "failed to wait exit semphore: %s", strerror(errno));
  // }

  dnodeDropInstance(dnode);
  
  uInfo("TDengine is shut down!");
  return 0;
}

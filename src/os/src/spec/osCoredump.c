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
#include "tconfig.h"
#include "tglobal.h"
#include "tulog.h"
#include "tutil.h"
#include "tsystem.h"
#include <sys/sysctl.h>

#ifndef TAOS_OS_FUNC_CORE

int _sysctl(struct __sysctl_args *args );

void taosSetCoreDump() {
  if (0 == tsEnableCoreFile) {
    return;
  }
  
  // 1. set ulimit -c unlimited
  struct rlimit rlim;
  struct rlimit rlim_new;
  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
    uInfo("the old unlimited para: rlim_cur=%" PRIu64 ", rlim_max=%" PRIu64, rlim.rlim_cur, rlim.rlim_max);
    rlim_new.rlim_cur = RLIM_INFINITY;
    rlim_new.rlim_max = RLIM_INFINITY;
    if (setrlimit(RLIMIT_CORE, &rlim_new) != 0) {
      uInfo("set unlimited fail, error: %s", strerror(errno));
      rlim_new.rlim_cur = rlim.rlim_max;
      rlim_new.rlim_max = rlim.rlim_max;
      (void)setrlimit(RLIMIT_CORE, &rlim_new);
    }
  }

  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
    uInfo("the new unlimited para: rlim_cur=%" PRIu64 ", rlim_max=%" PRIu64, rlim.rlim_cur, rlim.rlim_max);
  }

#ifndef _TD_ARM_
  // 2. set the path for saving core file
  struct __sysctl_args args;
  int     old_usespid = 0;
  size_t  old_len     = 0;
  int     new_usespid = 1;
  size_t  new_len     = sizeof(new_usespid);
  
  int name[] = {CTL_KERN, KERN_CORE_USES_PID};
  
  memset(&args, 0, sizeof(struct __sysctl_args));
  args.name    = name;
  args.nlen    = sizeof(name)/sizeof(name[0]);
  args.oldval  = &old_usespid;
  args.oldlenp = &old_len;
  args.newval  = &new_usespid;
  args.newlen  = new_len;
  
  old_len = sizeof(old_usespid);
  
  if (syscall(SYS__sysctl, &args) == -1) {
      uInfo("_sysctl(kern_core_uses_pid) set fail: %s", strerror(errno));
  }
  
  uInfo("The old core_uses_pid[%" PRIu64 "]: %d", old_len, old_usespid);


  old_usespid = 0;
  old_len     = 0;
  memset(&args, 0, sizeof(struct __sysctl_args));
  args.name    = name;
  args.nlen    = sizeof(name)/sizeof(name[0]);
  args.oldval  = &old_usespid;
  args.oldlenp = &old_len;
  
  old_len = sizeof(old_usespid);
  
  if (syscall(SYS__sysctl, &args) == -1) {
      uInfo("_sysctl(kern_core_uses_pid) get fail: %s", strerror(errno));
  }
  
  uInfo("The new core_uses_pid[%" PRIu64 "]: %d", old_len, old_usespid);
#endif

}

#endif
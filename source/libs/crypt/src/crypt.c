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
#include "crypt.h"

extern int32_t CBC_DecryptImpl(SCryptOpts *opts);
extern int32_t CBC_EncryptImpl(SCryptOpts *opts);

int32_t CBC_Encrypt(SCryptOpts *opts) { 
  return CBC_EncryptImpl(opts); 
}
int32_t CBC_Decrypt(SCryptOpts *opts) { 
  return CBC_DecryptImpl(opts); 
}

#if !defined(TD_ENTERPRISE) && !defined(TD_ASTRA)
int32_t CBC_EncryptImpl(SCryptOpts *opts) { 
  memcpy(opts->result, opts->source, opts->len);
  return opts->len; 
}
int32_t CBC_DecryptImpl(SCryptOpts *opts) { 
  memcpy(opts->result, opts->source, opts->len);
  return opts->len; 
}
#endif
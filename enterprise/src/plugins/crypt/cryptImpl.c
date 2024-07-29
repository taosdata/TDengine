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
#include "sm4.h"

int32_t CBC_DecryptImpl(SCryptOpts *opts){
    int	NewLen = 0;

    int32_t count = 0;
    while(count < opts->len)
    {
        (void)SM4_CBC_Decrypt(opts->key, 16, opts->key, 16, opts->source + count, opts->unitLen, opts->result + count, &NewLen);
        count += NewLen;
    }
    return count;
}

int32_t CBC_EncryptImpl(SCryptOpts *opts){
    int	NewLen = 0;

    int32_t count = 0;
    while(count < opts->len)
    {
        (void)SM4_CBC_Encrypt(opts->key, 16, opts->key, 16, opts->source + count, opts->unitLen, opts->result + count, &NewLen);
        count += NewLen;
    }
    return count;
}
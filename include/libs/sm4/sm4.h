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

#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <time.h>
#ifndef _SM4_H_
#define _SM4_H_

#ifdef __cplusplus
extern "C" {
#endif

int SM4_ECB_Encrypt( unsigned char *pKey, 
                      unsigned int KeyLen, 
                      unsigned char *pInData,
                      unsigned int inDataLen,
                      unsigned char *pOutData, 
                      unsigned int *pOutDataLen);

int SM4_ECB_Decrypt(  unsigned char *pKey, 
                      unsigned int KeyLen, 
                      unsigned char *pInData, 
                      unsigned int inDataLen,
                      unsigned char *pOutData,
                      unsigned int *pOutDataLen);

int SM4_CBC_Encrypt( unsigned char *pKey, 
                     unsigned int KeyLen,
                     unsigned char *pIV, 
                     unsigned int ivLen,
                     unsigned char *pInData, 
                     unsigned int inDataLen,
                     unsigned char *pOutData, 
                     unsigned int *pOutDataLen);

int SM4_CBC_Decrypt(unsigned char *pKey, 
                    unsigned int KeyLen, 
                    unsigned char *pIV, 
                    unsigned int ivLen,
                    unsigned char *pInData,
                    unsigned int inDataLen,
                    unsigned char *pOutData, 
                    unsigned int *pOutDataLen);
#ifdef __cplusplus
}
#endif

#endif // _SM4_H_
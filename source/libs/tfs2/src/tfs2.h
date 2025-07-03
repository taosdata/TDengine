/*
 * Copyright (c) 2023 Hongze Cheng <hzcheng@umich.edu>.
 * All rights reserved.
 *
 * This code is the intellectual property of Hongze Cheng.
 * Any reproduction or distribution, in whole or in part,
 * without the express written permission of Hongze Cheng is
 * strictly prohibited.
 */

#ifndef __TD_LIBS_TFS2_H_
#define __TD_LIBS_TFS2_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STfs2      STfs2;
typedef struct STfsConfig STfsConfig;
typedef struct STfsFileObj STfsFileObj;

struct STfsFileObj {
// TODO
};

int32_t tfsOpen(STfsConfig *pCfg);
void    tfsClose();

int32_t tfsImportFile(/* TODO: some thing*/);

// File Object
const char *tfsFileGetName(const STfsFileObj *pFile);
int32_t tfsNewFile(STfsFileObj *pFile);
int32_t tfsLinkFile(STfsFileObj *pFile);
int32_t tfsUnlinkFile(STfsFileObj *pFile);
int32_t tfsFileSize(STfsFileObj *pFile);
int32_t tfsFileAppend(STfsFileObj *pFile, const void *data, size_t size);

#ifdef __cplusplus
}
#endif

#endif /*__TD_LIBS_TFS2_H_*/
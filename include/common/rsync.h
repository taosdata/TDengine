//
// Created by mingming wanng on 2023/11/2.
//

#ifndef TDENGINE_RSYNC_H
#define TDENGINE_RSYNC_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tarray.h"

void stopRsync();
void startRsync();
int  uploadRsync(const char* id, const char* path);
int  downloadRsync(const char* id, const char* path);
int  deleteRsync(const char* id);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_RSYNC_H

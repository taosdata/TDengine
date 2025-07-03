/*
 * Copyright (c) 2023 Hongze Cheng <hzcheng@umich.edu>.
 * All rights reserved.
 *
 * This code is the intellectual property of Hongze Cheng.
 * Any reproduction or distribution, in whole or in part,
 * without the express written permission of Hongze Cheng is
 * strictly prohibited.
 */

#include "tfsDisk.h"

typedef enum {
  TFS_DISK_TYPE_LOCAL = 0,  // Local disk
  TFS_DISK_TYPE_HDFS = 1,   // HDFS disk
  TFS_DISK_TYPE_S3,         // S3 disk
} ETfsDiskType;

typedef struct {
  uint64_t id_;
} SDiskID;

struct STfsDisk {
  // TODO
};

struct STfsDiskMgr {
  // TODO
};
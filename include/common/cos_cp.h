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

#ifndef _TD_COMMON_COS_CP_H_
#define _TD_COMMON_COS_CP_H_

#include "os.h"
#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  COS_CP_TYPE_UPLOAD,   // upload
  COS_CP_TYPE_DOWNLOAD  // download
} ECpType;

typedef struct {
  int32_t  index;      // the index of part, start from 0
  int64_t  offset;     // the offset point of part
  int64_t  size;       // the size of part
  int      completed;  // COS_TRUE completed, COS_FALSE uncompleted
  char     etag[128];  // the etag of part, for upload
  uint64_t crc64;
} SCheckpointPart;

typedef struct {
  ECpType   cp_type;  // 0 upload, 1 download
  char      md5[64];  // the md5 of checkout content
  TdFilePtr thefile;  // the handle of checkpoint file

  char    file_path[TSDB_FILENAME_LEN];  // local file path
  int64_t file_size;                     // local file size, for upload
  int32_t file_last_modified;            // local file last modified time, for upload
  char    file_md5[64];                  // md5 of the local file content, for upload, reserved

  char    object_name[128];          // object name
  int64_t object_size;               // object size, for download
  char    object_last_modified[64];  // object last modified time, for download
  char    object_etag[128];          // object etag, for download

  char upload_id[128];  // upload id

  int              part_num;   // the total number of parts
  int64_t          part_size;  // the part size, byte
  SCheckpointPart* parts;      // the parts of local or object, from 0
} SCheckpoint;

int32_t cos_cp_open(char const* cp_path, SCheckpoint* checkpoint);
void    cos_cp_close(TdFilePtr fd);
void    cos_cp_remove(char const* filepath);

int32_t cos_cp_load(char const* filepath, SCheckpoint* checkpoint);
int32_t cos_cp_dump(SCheckpoint* checkpoint);
void    cos_cp_get_undo_parts(SCheckpoint* checkpoint, int* part_num, SCheckpointPart* parts, int64_t* consume_bytes);
void    cos_cp_update(SCheckpoint* checkpoint, int32_t part_index, char const* etag, uint64_t crc64);
void    cos_cp_build_upload(SCheckpoint* checkpoint, char const* filepath, int64_t size, int32_t mtime,
                            char const* upload_id, int64_t part_size);
bool    cos_cp_is_valid_upload(SCheckpoint* checkpoint, int64_t size, int32_t mtime);

void cos_cp_build_download(SCheckpoint* checkpoint, char const* filepath, char const* object_name, int64_t object_size,
                           char const* object_lmtime, char const* object_etag, int64_t part_size);
bool cos_cp_is_valid_download(SCheckpoint* checkpoint, char const* object_name, int64_t object_size,
                              char const* object_lmtime, char const* object_etag);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_COS_CP_H_*/

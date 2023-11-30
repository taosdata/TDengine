#define ALLOW_FORBID_FUNC

#include "cos_cp.h"

void      cos_cp_get_path(char const* filepath, char* cp_path) {}
TdFilePtr cos_cp_open(char const* cp_path, SCheckpoint* checkpoint) { return NULL; }
void      cos_cp_close(TdFilePtr fd) {}
void      cos_cp_remove(char const* filepath) {}
bool      cos_cp_exist(char const* filepath) { return true; }

int32_t cos_cp_load(char const* filepath, SCheckpoint* checkpoint) { return 0; }
int32_t cos_cp_dump(SCheckpoint* checkpoint) { return 0; }
void    cos_cp_get_parts(SCheckpoint* checkpoint, int* part_num, SCheckpointPart* parts, int64_t consume_bytes) {}
void    cos_cp_update(SCheckpoint* checkpoint, int32_t part_index, char const* etag, uint64_t crc64) {}
void    cos_cp_build_upload(SCheckpoint* checkpoint, char const* filepath, int64_t size, int32_t mtime,
                            char const* upload_id, int64_t part_size) {}
bool    cos_cp_is_valid_upload(SCheckpoint* checkpoint, int64_t size, int32_t mtime) { return true; }

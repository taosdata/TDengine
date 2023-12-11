#define ALLOW_FORBID_FUNC

#include "cos_cp.h"
#include "cJSON.h"
#include "tutil.h"

int32_t cos_cp_open(char const* cp_path, SCheckpoint* checkpoint) {
  int32_t code = 0;

  TdFilePtr fd = taosOpenFile(cp_path, TD_FILE_WRITE | TD_FILE_CREATE /* | TD_FILE_TRUNC*/ | TD_FILE_WRITE_THROUGH);
  if (!fd) {
    code = TAOS_SYSTEM_ERROR(errno);
    uError("ERROR: %s Failed to open %s", __func__, cp_path);
    return code;
  }

  checkpoint->thefile = fd;

  return code;
}

void cos_cp_close(TdFilePtr fd) { taosCloseFile(&fd); }
void cos_cp_remove(char const* filepath) { taosRemoveFile(filepath); }

static int32_t cos_cp_parse_body(char* cp_body, SCheckpoint* cp) {
  int32_t      code = 0;
  cJSON const* item2 = NULL;

  cJSON* json = cJSON_Parse(cp_body);
  if (NULL == json) {
    code = TSDB_CODE_FILE_CORRUPTED;
    uError("ERROR: %s Failed to parse json", __func__);
    goto _exit;
  }

  cJSON const* item = cJSON_GetObjectItem(json, "ver");
  if (!cJSON_IsNumber(item) || item->valuedouble != 1) {
    code = TSDB_CODE_FILE_CORRUPTED;
    uError("ERROR: %s Failed to parse json ver: %f", __func__, item->valuedouble);
    goto _exit;
  }

  item = cJSON_GetObjectItem(json, "type");
  if (!cJSON_IsNumber(item)) {
    code = TSDB_CODE_FILE_CORRUPTED;
    uError("ERROR: %s Failed to parse json", __func__);
    goto _exit;
  }
  cp->cp_type = item->valuedouble;

  item = cJSON_GetObjectItem(json, "md5");
  if (cJSON_IsString(item)) {
    memcpy(cp->md5, item->valuestring, strlen(item->valuestring));
  }

  item = cJSON_GetObjectItem(json, "upload_id");
  if (cJSON_IsString(item)) {
    strncpy(cp->upload_id, item->valuestring, 128);
  }

  item2 = cJSON_GetObjectItem(json, "file");
  if (cJSON_IsObject(item2)) {
    item = cJSON_GetObjectItem(item2, "size");
    if (cJSON_IsNumber(item)) {
      cp->file_size = item->valuedouble;
    }

    item = cJSON_GetObjectItem(item2, "lastmodified");
    if (cJSON_IsNumber(item)) {
      cp->file_last_modified = item->valuedouble;
    }

    item = cJSON_GetObjectItem(item2, "path");
    if (cJSON_IsString(item)) {
      strncpy(cp->file_path, item->valuestring, TSDB_FILENAME_LEN);
    }

    item = cJSON_GetObjectItem(item2, "file_md5");
    if (cJSON_IsString(item)) {
      strncpy(cp->file_md5, item->valuestring, 64);
    }
  }

  item2 = cJSON_GetObjectItem(json, "object");
  if (cJSON_IsObject(item2)) {
    item = cJSON_GetObjectItem(item2, "object_size");
    if (cJSON_IsNumber(item)) {
      cp->object_size = item->valuedouble;
    }

    item = cJSON_GetObjectItem(item2, "object_name");
    if (cJSON_IsString(item)) {
      strncpy(cp->object_name, item->valuestring, 128);
    }

    item = cJSON_GetObjectItem(item2, "object_last_modified");
    if (cJSON_IsString(item)) {
      strncpy(cp->object_last_modified, item->valuestring, 64);
    }

    item = cJSON_GetObjectItem(item2, "object_etag");
    if (cJSON_IsString(item)) {
      strncpy(cp->object_etag, item->valuestring, 128);
    }
  }

  item2 = cJSON_GetObjectItem(json, "cpparts");
  if (cJSON_IsObject(item2)) {
    item = cJSON_GetObjectItem(item2, "number");
    if (cJSON_IsNumber(item)) {
      cp->part_num = item->valuedouble;
    }

    item = cJSON_GetObjectItem(item2, "size");
    if (cJSON_IsNumber(item)) {
      cp->part_size = item->valuedouble;
    }

    item2 = cJSON_GetObjectItem(item2, "parts");
    if (cJSON_IsArray(item2) && cp->part_num > 0) {
      cJSON_ArrayForEach(item, item2) {
        cJSON const* item3 = cJSON_GetObjectItem(item, "index");
        int32_t      index = 0;
        if (cJSON_IsNumber(item3)) {
          index = item3->valuedouble;
          cp->parts[index].index = index;
        }

        item3 = cJSON_GetObjectItem(item, "offset");
        if (cJSON_IsNumber(item3)) {
          cp->parts[index].offset = item3->valuedouble;
        }

        item3 = cJSON_GetObjectItem(item, "size");
        if (cJSON_IsNumber(item3)) {
          cp->parts[index].size = item3->valuedouble;
        }

        item3 = cJSON_GetObjectItem(item, "completed");
        if (cJSON_IsNumber(item3)) {
          cp->parts[index].completed = item3->valuedouble;
        }

        item3 = cJSON_GetObjectItem(item, "crc64");
        if (cJSON_IsNumber(item3)) {
          cp->parts[index].crc64 = item3->valuedouble;
        }

        item3 = cJSON_GetObjectItem(item, "etag");
        if (cJSON_IsString(item3)) {
          strncpy(cp->parts[index].etag, item3->valuestring, 128);
        }
      }
    }
  }

_exit:
  if (json) cJSON_Delete(json);
  if (cp_body) taosMemoryFree(cp_body);

  return code;
}

int32_t cos_cp_load(char const* filepath, SCheckpoint* checkpoint) {
  int32_t code = 0;

  TdFilePtr fd = taosOpenFile(filepath, TD_FILE_READ);
  if (!fd) {
    code = TAOS_SYSTEM_ERROR(errno);
    uError("ERROR: %s Failed to open %s", __func__, filepath);
    goto _exit;
  }

  int64_t size = -1;
  code = taosStatFile(filepath, &size, NULL, NULL);
  if (code) {
    uError("ERROR: %s Failed to stat %s", __func__, filepath);
    goto _exit;
  }

  char* cp_body = taosMemoryMalloc(size + 1);

  int64_t n = taosReadFile(fd, cp_body, size);
  if (n < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    uError("ERROR: %s Failed to read %s", __func__, filepath);
    goto _exit;
  } else if (n != size) {
    code = TSDB_CODE_FILE_CORRUPTED;
    uError("ERROR: %s Failed to read %s %" PRId64 "/%" PRId64, __func__, filepath, n, size);
    goto _exit;
  }
  taosCloseFile(&fd);
  cp_body[size] = '\0';

  return cos_cp_parse_body(cp_body, checkpoint);

_exit:
  if (fd) {
    taosCloseFile(&fd);
  }
  if (cp_body) {
    taosMemoryFree(cp_body);
  }

  return code;
}

static int32_t cos_cp_save_json(cJSON const* json, SCheckpoint* checkpoint) {
  int32_t code = 0;

  char* data = cJSON_PrintUnformatted(json);
  if (NULL == data) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TdFilePtr fp = checkpoint->thefile;
  if (taosFtruncateFile(fp, 0) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }
  if (taosLSeekFile(fp, 0, SEEK_SET) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }
  if (taosWriteFile(fp, data, strlen(data)) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

  if (taosFsyncFile(fp) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    goto _exit;
  }

_exit:
  taosMemoryFree(data);
  return code;
}

int32_t cos_cp_dump(SCheckpoint* cp) {
  int32_t code = 0;
  int32_t lino = 0;

  cJSON* ojson = NULL;
  cJSON* json = cJSON_CreateObject();
  if (!json) return TSDB_CODE_OUT_OF_MEMORY;

  if (NULL == cJSON_AddNumberToObject(json, "ver", 1)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (NULL == cJSON_AddNumberToObject(json, "type", cp->cp_type)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (NULL == cJSON_AddStringToObject(json, "md5", cp->md5)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (NULL == cJSON_AddStringToObject(json, "upload_id", cp->upload_id)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (COS_CP_TYPE_UPLOAD == cp->cp_type) {
    ojson = cJSON_AddObjectToObject(json, "file");
    if (!ojson) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(ojson, "size", cp->file_size)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(ojson, "lastmodified", cp->file_last_modified)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "path", cp->file_path)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "file_md5", cp->file_md5)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else if (COS_CP_TYPE_DOWNLOAD == cp->cp_type) {
    ojson = cJSON_AddObjectToObject(json, "object");
    if (!ojson) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(ojson, "object_size", cp->object_size)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "object_name", cp->object_name)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "object_last_modified", cp->object_last_modified)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "object_etag", cp->object_etag)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  ojson = cJSON_AddObjectToObject(json, "cpparts");
  if (!ojson) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (NULL == cJSON_AddNumberToObject(ojson, "number", cp->part_num)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if (NULL == cJSON_AddNumberToObject(ojson, "size", cp->part_size)) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  cJSON* ajson = cJSON_AddArrayToObject(ojson, "parts");
  if (!ajson) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  for (int i = 0; i < cp->part_num; ++i) {
    cJSON* item = cJSON_CreateObject();
    if (!item) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    cJSON_AddItemToArray(ajson, item);

    if (NULL == cJSON_AddNumberToObject(item, "index", cp->parts[i].index)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(item, "offset", cp->parts[i].offset)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(item, "size", cp->parts[i].size)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(item, "completed", cp->parts[i].completed)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(item, "crc64", cp->parts[i].crc64)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(item, "etag", cp->parts[i].etag)) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = cos_cp_save_json(json, cp);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  cJSON_Delete(json);
  return code;
}

void cos_cp_get_undo_parts(SCheckpoint* checkpoint, int* part_num, SCheckpointPart* parts, int64_t* consume_bytes) {}

void cos_cp_update(SCheckpoint* checkpoint, int32_t part_index, char const* etag, uint64_t crc64) {
  checkpoint->parts[part_index].completed = 1;
  strncpy(checkpoint->parts[part_index].etag, etag, 128);
  checkpoint->parts[part_index].crc64 = crc64;
}

void cos_cp_build_upload(SCheckpoint* checkpoint, char const* filepath, int64_t size, int32_t mtime,
                         char const* upload_id, int64_t part_size) {
  int i = 0;

  checkpoint->cp_type = COS_CP_TYPE_UPLOAD;
  strncpy(checkpoint->file_path, filepath, TSDB_FILENAME_LEN);

  checkpoint->file_size = size;
  checkpoint->file_last_modified = mtime;
  strncpy(checkpoint->upload_id, upload_id, 128);

  checkpoint->part_size = part_size;
  for (; i * part_size < size; i++) {
    checkpoint->parts[i].index = i;
    checkpoint->parts[i].offset = i * part_size;
    checkpoint->parts[i].size = TMIN(part_size, (size - i * part_size));
    checkpoint->parts[i].completed = 0;
    checkpoint->parts[i].etag[0] = '\0';
  }
  checkpoint->part_num = i;
}

static bool cos_cp_verify_md5(SCheckpoint* cp) { return true; }

bool cos_cp_is_valid_upload(SCheckpoint* checkpoint, int64_t size, int32_t mtime) {
  if (cos_cp_verify_md5(checkpoint) && checkpoint->file_size == size && checkpoint->file_last_modified == mtime) {
    return true;
  }

  return false;
}

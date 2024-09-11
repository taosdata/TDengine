#define ALLOW_FORBID_FUNC

#include "cos_cp.h"
#include "cJSON.h"
#include "tutil.h"

int32_t cos_cp_open(char const* cp_path, SCheckpoint* checkpoint) {
  int32_t code = 0;

  TdFilePtr fd = taosOpenFile(cp_path, TD_FILE_WRITE | TD_FILE_CREATE /* | TD_FILE_TRUNC*/ | TD_FILE_WRITE_THROUGH);
  if (!fd) {
    uError("%s Failed to open %s", __func__, cp_path);
    TAOS_CHECK_RETURN(terrno);
  }

  checkpoint->thefile = fd;

  TAOS_RETURN(code);
}

int32_t cos_cp_close(TdFilePtr fd) { return taosCloseFile(&fd); }
int32_t cos_cp_remove(char const* filepath) { return taosRemoveFile(filepath); }

static int32_t cos_cp_parse_body(char* cp_body, SCheckpoint* cp) {
  int32_t      code = 0, lino = 0;
  cJSON const* item2 = NULL;

  cJSON* json = cJSON_Parse(cp_body);
  if (NULL == json) {
    TAOS_CHECK_GOTO(TSDB_CODE_FILE_CORRUPTED, &lino, _exit);
  }

  cJSON const* item = cJSON_GetObjectItem(json, "ver");
  if (!cJSON_IsNumber(item) || item->valuedouble != 1) {
    TAOS_CHECK_GOTO(TSDB_CODE_FILE_CORRUPTED, &lino, _exit);
  }

  item = cJSON_GetObjectItem(json, "type");
  if (!cJSON_IsNumber(item)) {
    TAOS_CHECK_GOTO(TSDB_CODE_FILE_CORRUPTED, &lino, _exit);
  }
  cp->cp_type = item->valuedouble;

  item = cJSON_GetObjectItem(json, "md5");
  if (cJSON_IsString(item)) {
    (void)memcpy(cp->md5, item->valuestring, strlen(item->valuestring));
  }

  item = cJSON_GetObjectItem(json, "upload_id");
  if (cJSON_IsString(item)) {
    (void)strncpy(cp->upload_id, item->valuestring, 128);
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
      (void)strncpy(cp->file_path, item->valuestring, TSDB_FILENAME_LEN);
    }

    item = cJSON_GetObjectItem(item2, "file_md5");
    if (cJSON_IsString(item)) {
      (void)strncpy(cp->file_md5, item->valuestring, 64);
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
      (void)strncpy(cp->object_name, item->valuestring, 128);
    }

    item = cJSON_GetObjectItem(item2, "object_last_modified");
    if (cJSON_IsString(item)) {
      (void)strncpy(cp->object_last_modified, item->valuestring, 64);
    }

    item = cJSON_GetObjectItem(item2, "object_etag");
    if (cJSON_IsString(item)) {
      (void)strncpy(cp->object_etag, item->valuestring, 128);
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
          (void)strncpy(cp->parts[index].etag, item3->valuestring, 128);
        }
      }
    }
  }

_exit:
  if (code) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (json) cJSON_Delete(json);
  if (cp_body) taosMemoryFree(cp_body);
  TAOS_RETURN(code);
}

int32_t cos_cp_load(char const* filepath, SCheckpoint* checkpoint) {
  int32_t code = 0, lino = 0;
  char*   cp_body = NULL;

  TdFilePtr fd = taosOpenFile(filepath, TD_FILE_READ);
  if (!fd) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  int64_t size = -1;
  TAOS_CHECK_GOTO(taosStatFile(filepath, &size, NULL, NULL), &lino, _exit);

  cp_body = taosMemoryMalloc(size + 1);
  if (!cp_body) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  int64_t n = taosReadFile(fd, cp_body, size);
  if (n < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  } else if (n != size) {
    TAOS_CHECK_GOTO(TSDB_CODE_FILE_CORRUPTED, &lino, _exit);
  }
  (void)taosCloseFile(&fd);
  cp_body[size] = '\0';

  return cos_cp_parse_body(cp_body, checkpoint);

_exit:
  if (code) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (fd) {
    (void)taosCloseFile(&fd);
  }
  if (cp_body) {
    taosMemoryFree(cp_body);
  }
  TAOS_RETURN(code);
}

static int32_t cos_cp_save_json(cJSON const* json, SCheckpoint* checkpoint) {
  int32_t code = 0, lino = 0;

  char* data = cJSON_PrintUnformatted(json);
  if (!data) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  TdFilePtr fp = checkpoint->thefile;
  if (taosFtruncateFile(fp, 0) < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }
  if (taosLSeekFile(fp, 0, SEEK_SET) < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }
  if (taosWriteFile(fp, data, strlen(data)) < 0) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  if (taosFsyncFile(fp) < 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &lino, _exit);
  }

_exit:
  if (code) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosMemoryFree(data);
  TAOS_RETURN(code);
}

int32_t cos_cp_dump(SCheckpoint* cp) {
  int32_t code = 0, lino = 0;

  cJSON* ojson = NULL;
  cJSON* json = cJSON_CreateObject();
  if (!json) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (NULL == cJSON_AddNumberToObject(json, "ver", 1)) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  if (NULL == cJSON_AddNumberToObject(json, "type", cp->cp_type)) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  if (NULL == cJSON_AddStringToObject(json, "md5", cp->md5)) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  if (NULL == cJSON_AddStringToObject(json, "upload_id", cp->upload_id)) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  if (COS_CP_TYPE_UPLOAD == cp->cp_type) {
    ojson = cJSON_AddObjectToObject(json, "file");
    if (!ojson) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(ojson, "size", cp->file_size)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(ojson, "lastmodified", cp->file_last_modified)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "path", cp->file_path)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "file_md5", cp->file_md5)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
  } else if (COS_CP_TYPE_DOWNLOAD == cp->cp_type) {
    ojson = cJSON_AddObjectToObject(json, "object");
    if (!ojson) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(ojson, "object_size", cp->object_size)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "object_name", cp->object_name)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "object_last_modified", cp->object_last_modified)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(ojson, "object_etag", cp->object_etag)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
  }

  ojson = cJSON_AddObjectToObject(json, "cpparts");
  if (!ojson) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }
  if (NULL == cJSON_AddNumberToObject(ojson, "number", cp->part_num)) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }
  if (NULL == cJSON_AddNumberToObject(ojson, "size", cp->part_size)) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  cJSON* ajson = cJSON_AddArrayToObject(ojson, "parts");
  if (!ajson) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }
  for (int i = 0; i < cp->part_num; ++i) {
    cJSON* item = cJSON_CreateObject();
    if (!item) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    (void)cJSON_AddItemToArray(ajson, item);

    if (NULL == cJSON_AddNumberToObject(item, "index", cp->parts[i].index)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(item, "offset", cp->parts[i].offset)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(item, "size", cp->parts[i].size)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(item, "completed", cp->parts[i].completed)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddNumberToObject(item, "crc64", cp->parts[i].crc64)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
    if (NULL == cJSON_AddStringToObject(item, "etag", cp->parts[i].etag)) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
    }
  }

  code = cos_cp_save_json(json, cp);
  TAOS_CHECK_GOTO(code, &lino, _exit);

_exit:
  if (code) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  cJSON_Delete(json);
  TAOS_RETURN(code);
}

void cos_cp_get_undo_parts(SCheckpoint* checkpoint, int* part_num, SCheckpointPart* parts, int64_t* consume_bytes) {}

void cos_cp_update(SCheckpoint* checkpoint, int32_t part_index, char const* etag, uint64_t crc64) {
  checkpoint->parts[part_index].completed = 1;
  (void)strncpy(checkpoint->parts[part_index].etag, etag, 127);
  checkpoint->parts[part_index].crc64 = crc64;
}

void cos_cp_build_upload(SCheckpoint* checkpoint, char const* filepath, int64_t size, int32_t mtime,
                         char const* upload_id, int64_t part_size) {
  int i = 0;

  checkpoint->cp_type = COS_CP_TYPE_UPLOAD;
  (void)memset(checkpoint->file_path, 0, TSDB_FILENAME_LEN);
  (void)strncpy(checkpoint->file_path, filepath, TSDB_FILENAME_LEN - 1);

  checkpoint->file_size = size;
  checkpoint->file_last_modified = mtime;
  (void)strncpy(checkpoint->upload_id, upload_id, 127);

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

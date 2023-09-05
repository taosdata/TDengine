#define ALLOW_FORBID_FUNC

#include "vndCos.h"

extern char tsS3Endpoint[];
extern char tsS3AccessKeyId[];
extern char tsS3AccessKeySecret[];
extern char tsS3BucketName[];
extern char tsS3AppId[];

#ifdef USE_COS
#include "cos_api.h"
#include "cos_http_io.h"
#include "cos_log.h"

int32_t s3Init() {
  if (cos_http_io_initialize(NULL, 0) != COSE_OK) {
    return -1;
  }

  // set log level, default COS_LOG_WARN
  cos_log_set_level(COS_LOG_WARN);

  // set log output, default stderr
  cos_log_set_output(NULL);

  return 0;
}

void s3CleanUp() { cos_http_io_deinitialize(); }

static void log_status(cos_status_t *s) {
  cos_warn_log("status->code: %d", s->code);
  if (s->error_code) cos_warn_log("status->error_code: %s", s->error_code);
  if (s->error_msg) cos_warn_log("status->error_msg: %s", s->error_msg);
  if (s->req_id) cos_warn_log("status->req_id: %s", s->req_id);
}

static void s3InitRequestOptions(cos_request_options_t *options, int is_cname) {
  options->config = cos_config_create(options->pool);

  cos_config_t *config = options->config;

  cos_str_set(&config->endpoint, tsS3Endpoint);
  cos_str_set(&config->access_key_id, tsS3AccessKeyId);
  cos_str_set(&config->access_key_secret, tsS3AccessKeySecret);
  cos_str_set(&config->appid, tsS3AppId);

  config->is_cname = is_cname;

  options->ctl = cos_http_controller_create(options->pool, 0);
}

int32_t s3PutObjectFromFile(const char *file_str, const char *object_str) {
  int32_t                code = 0;
  cos_pool_t            *p = NULL;
  int                    is_cname = 0;
  cos_status_t          *s = NULL;
  cos_request_options_t *options = NULL;
  cos_string_t           bucket, object, file;
  cos_table_t           *resp_headers;
  // int                    traffic_limit = 0;

  cos_pool_create(&p, NULL);
  options = cos_request_options_create(p);
  s3InitRequestOptions(options, is_cname);
  cos_table_t *headers = NULL;
  /*
  if (traffic_limit) {
    // 限速值设置范围为819200 - 838860800，即100KB/s - 100MB/s，如果超出该范围将返回400错误
    headers = cos_table_make(p, 1);
    cos_table_add_int(headers, "x-cos-traffic-limit", 819200);
  }
  */
  cos_str_set(&bucket, tsS3BucketName);
  cos_str_set(&file, file_str);
  cos_str_set(&object, object_str);
  s = cos_put_object_from_file(options, &bucket, &object, &file, headers, &resp_headers);
  log_status(s);

  cos_pool_destroy(p);

  if (s->code != 200) {
    return code = s->code;
  }

  return code;
}

void s3DeleteObjectsByPrefix(const char *prefix_str) {
  cos_pool_t            *p = NULL;
  cos_request_options_t *options = NULL;
  int                    is_cname = 0;
  cos_string_t           bucket;
  cos_status_t          *s = NULL;
  cos_string_t           prefix;

  cos_pool_create(&p, NULL);
  options = cos_request_options_create(p);
  s3InitRequestOptions(options, is_cname);
  cos_str_set(&bucket, tsS3BucketName);
  cos_str_set(&prefix, prefix_str);

  s = cos_delete_objects_by_prefix(options, &bucket, &prefix);
  log_status(s);
  cos_pool_destroy(p);
}

void s3DeleteObjects(const char *object_name[], int nobject) {
  cos_pool_t            *p = NULL;
  int                    is_cname = 0;
  cos_string_t           bucket;
  cos_table_t           *resp_headers = NULL;
  cos_request_options_t *options = NULL;
  cos_list_t             object_list;
  cos_list_t             deleted_object_list;
  int                    is_quiet = COS_TRUE;

  cos_pool_create(&p, NULL);
  options = cos_request_options_create(p);
  s3InitRequestOptions(options, is_cname);
  cos_str_set(&bucket, tsS3BucketName);

  cos_list_init(&object_list);
  cos_list_init(&deleted_object_list);

  for (int i = 0; i < nobject; ++i) {
    cos_object_key_t *content = cos_create_cos_object_key(p);
    cos_str_set(&content->key, object_name[i]);
    cos_list_add_tail(&content->node, &object_list);
  }

  cos_status_t *s = cos_delete_objects(options, &bucket, &object_list, is_quiet, &resp_headers, &deleted_object_list);
  log_status(s);

  cos_pool_destroy(p);

  if (cos_status_is_ok(s)) {
    cos_warn_log("delete objects succeeded\n");
  } else {
    cos_warn_log("delete objects failed\n");
  }
}

bool s3Exists(const char *object_name) {
  bool                      ret = false;
  cos_pool_t               *p = NULL;
  int                       is_cname = 0;
  cos_status_t             *s = NULL;
  cos_request_options_t    *options = NULL;
  cos_string_t              bucket;
  cos_string_t              object;
  cos_table_t              *resp_headers;
  cos_table_t              *headers = NULL;
  cos_object_exist_status_e object_exist;

  cos_pool_create(&p, NULL);
  options = cos_request_options_create(p);
  s3InitRequestOptions(options, is_cname);
  cos_str_set(&bucket, tsS3BucketName);
  cos_str_set(&object, object_name);

  s = cos_check_object_exist(options, &bucket, &object, headers, &object_exist, &resp_headers);
  if (object_exist == COS_OBJECT_NON_EXIST) {
    cos_warn_log("object: %.*s non exist.\n", object.len, object.data);
  } else if (object_exist == COS_OBJECT_EXIST) {
    ret = true;
    cos_warn_log("object: %.*s exist.\n", object.len, object.data);
  } else {
    cos_warn_log("object: %.*s unknown status.\n", object.len, object.data);
    log_status(s);
  }

  cos_pool_destroy(p);

  return ret;
}

bool s3Get(const char *object_name, const char *path) {
  bool                   ret = false;
  cos_pool_t            *p = NULL;
  int                    is_cname = 0;
  cos_status_t          *s = NULL;
  cos_request_options_t *options = NULL;
  cos_string_t           bucket;
  cos_string_t           object;
  cos_string_t           file;
  cos_table_t           *resp_headers = NULL;
  cos_table_t           *headers = NULL;
  // int                    traffic_limit = 0;

  //创建内存池
  cos_pool_create(&p, NULL);

  //初始化请求选项
  options = cos_request_options_create(p);
  s3InitRequestOptions(options, is_cname);
  cos_str_set(&bucket, tsS3BucketName);
  /*
  if (traffic_limit) {
    //限速值设置范围为819200 - 838860800，即100KB/s - 100MB/s，如果超出该范围将返回400错误
    headers = cos_table_make(p, 1);
    cos_table_add_int(headers, "x-cos-traffic-limit", 819200);
  }
  */
  //下载对象
  cos_str_set(&file, path);
  cos_str_set(&object, object_name);
  s = cos_get_object_to_file(options, &bucket, &object, headers, NULL, &file, &resp_headers);
  if (cos_status_is_ok(s)) {
    ret = true;
    cos_warn_log("get object succeeded\n");
  } else {
    cos_warn_log("get object failed\n");
  }

  //销毁内存池
  cos_pool_destroy(p);

  return ret;
}

typedef struct {
  int64_t size;
  int32_t atime;
  char    name[TSDB_FILENAME_LEN];
} SEvictFile;

static int32_t evictFileCompareAsce(const void *pLeft, const void *pRight) {
  SEvictFile *lhs = (SEvictFile *)pLeft;
  SEvictFile *rhs = (SEvictFile *)pRight;
  return lhs->atime < rhs->atime ? -1 : 1;
}

void s3EvictCache(const char *path, long object_size) {
  SDiskSize disk_size = {0};
  char      dir_name[TSDB_FILENAME_LEN] = "\0";

  tstrncpy(dir_name, path, TSDB_FILENAME_LEN);
  taosDirName(dir_name);

  if (taosGetDiskSize((char *)dir_name, &disk_size) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    vError("failed to get disk:%s size since %s", path, terrstr());
    return;
  }

  if (object_size >= disk_size.avail - (1 << 30)) {
    // evict too old files
    // 1, list data files' atime under dir(path)
    tdbDirPtr pDir = taosOpenDir(dir_name);
    if (pDir == NULL) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      vError("failed to open %s since %s", dir_name, terrstr());
    }
    SArray        *evict_files = taosArrayInit(16, sizeof(SEvictFile));
    tdbDirEntryPtr pDirEntry;
    while ((pDirEntry = taosReadDir(pDir)) != NULL) {
      char *name = taosGetDirEntryName(pDirEntry);
      if (!strncmp(name + strlen(name) - 5, ".data", 5)) {
        SEvictFile e_file = {0};
        char       entry_name[TSDB_FILENAME_LEN] = "\0";
        int        dir_len = strlen(dir_name);

        memcpy(e_file.name, dir_name, dir_len);
        e_file.name[dir_len] = '/';
        memcpy(e_file.name + dir_len + 1, name, strlen(name));

        taosStatFile(e_file.name, &e_file.size, NULL, &e_file.atime);

        taosArrayPush(evict_files, &e_file);
      }
    }
    taosCloseDir(&pDir);

    // 2, sort by atime
    taosArraySort(evict_files, evictFileCompareAsce);

    // 3, remove files ascendingly until we get enough object_size space
    long   evict_size = 0;
    size_t ef_size = TARRAY_SIZE(evict_files);
    for (size_t i = 0; i < ef_size; ++i) {
      SEvictFile *evict_file = taosArrayGet(evict_files, i);
      (void)taosRemoveFile(evict_file->name);
      evict_size += evict_file->size;
      if (evict_size >= object_size) {
        break;
      }
    }

    taosArrayDestroy(evict_files);
  }
}

long s3Size(const char *object_name) {
  long size = 0;

  cos_pool_t            *p = NULL;
  int                    is_cname = 0;
  cos_status_t          *s = NULL;
  cos_request_options_t *options = NULL;
  cos_string_t           bucket;
  cos_string_t           object;
  cos_table_t           *resp_headers = NULL;

  //创建内存池
  cos_pool_create(&p, NULL);

  //初始化请求选项
  options = cos_request_options_create(p);
  s3InitRequestOptions(options, is_cname);
  cos_str_set(&bucket, tsS3BucketName);

  //获取对象元数据
  cos_str_set(&object, object_name);
  s = cos_head_object(options, &bucket, &object, NULL, &resp_headers);
  // print_headers(resp_headers);
  if (cos_status_is_ok(s)) {
    char *content_length_str = (char *)apr_table_get(resp_headers, COS_CONTENT_LENGTH);
    if (content_length_str != NULL) {
      size = atol(content_length_str);
    }
    cos_warn_log("head object succeeded: %ld\n", size);
  } else {
    cos_warn_log("head object failed\n");
  }

  //销毁内存池
  cos_pool_destroy(p);

  return size;
}

#else

int32_t s3Init() { return 0; }
void    s3CleanUp() {}
int32_t s3PutObjectFromFile(const char *file, const char *object) { return 0; }
void    s3DeleteObjectsByPrefix(const char *prefix) {}
void    s3DeleteObjects(const char *object_name[], int nobject) {}
bool    s3Exists(const char *object_name) { return false; }
bool    s3Get(const char *object_name, const char *path) { return false; }
void    s3EvictCache(const char *path, long object_size) {}
long    s3Size(const char *object_name) { return 0; }

#endif

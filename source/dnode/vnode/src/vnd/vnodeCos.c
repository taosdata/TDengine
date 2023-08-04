#define ALLOW_FORBID_FUNC

#include "vndCos.h"

#include "cos_api.h"
#include "cos_http_io.h"
#include "cos_log.h"

extern char tsS3Endpoint[];
extern char tsS3AcessKeyId[];
extern char tsS3AcessKeySecret[];
extern char tsS3BucketName[];
extern char tsS3AppId[];

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
  cos_str_set(&config->access_key_id, tsS3AcessKeyId);
  cos_str_set(&config->access_key_secret, tsS3AcessKeySecret);
  cos_str_set(&config->appid, tsS3AppId);

  config->is_cname = is_cname;

  options->ctl = cos_http_controller_create(options->pool, 0);
}

void s3PutObjectFromFile(const char *file_str, const char *object_str) {
  cos_pool_t            *p = NULL;
  int                    is_cname = 0;
  cos_status_t          *s = NULL;
  cos_request_options_t *options = NULL;
  cos_string_t           bucket, object, file;
  cos_table_t           *resp_headers;
  int                    traffic_limit = 0;

  cos_pool_create(&p, NULL);
  options = cos_request_options_create(p);
  s3InitRequestOptions(options, is_cname);
  cos_table_t *headers = NULL;
  if (traffic_limit) {
    // 限速值设置范围为819200 - 838860800，即100KB/s - 100MB/s，如果超出该范围将返回400错误
    headers = cos_table_make(p, 1);
    cos_table_add_int(headers, "x-cos-traffic-limit", 819200);
  }
  cos_str_set(&bucket, tsS3BucketName);
  cos_str_set(&file, file_str);
  cos_str_set(&object, object_str);
  s = cos_put_object_from_file(options, &bucket, &object, &file, headers, &resp_headers);
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

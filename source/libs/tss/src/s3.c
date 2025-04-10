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

#include "tssInt.h"
#include "libs3.h"
#include "tutil.h"


// use multipart upload for files larger than this size
#define MULTIPART_CHUNK_SIZE (64 * 1024 * 1024)


// S3 storage instance, also used for Aliyun OSS and Tencent COS
typedef struct {
    // 'type' is inherited from SSharedStorage, it must be the first member
    // to allow casting to SSharedStorage
    const SSharedStorageType* type;

    // type-specific data
    S3BucketContext bucketContext;
    S3PutProperties putProperties;

    // variable-length buffer for hostname, bucket and etc.
    char buf[0];
} SSharedStorageS3 ;



// initInstance initializes the SSharedStorageS3 instance from the access string.
static bool initInstance(SSharedStorageS3* ss, const char* as) {
    strcpy(ss->buf, as);

    // set default values
    ss->bucketContext.uriStyle = S3UriStyleVirtualHost;
    ss->bucketContext.protocol = S3ProtocolHTTPS;
    ss->bucketContext.securityToken = NULL;

    // skip storage type
    char* p = strchr(ss->buf, ':');
    if (p == NULL) {
        tssError("invalid access string: %s", as);
        return false;
    }
    p++;

    // parse key-value pairs
    while (p != NULL) {
        // find key
        char* key = p;
        p = strchr(p, '=');
        if (p == NULL) {
            tssError("invalid access string: %s", as);
            return false;
        }
        *p++ = 0;
        if (strlen(key) == 0) {
            tssError("blank key in access string: %s", as);
            return false;
        }

        // find val
        char* val = p;
        p = strchr(p, ';');
        if (p != NULL) {
            *p++ = 0;
        }
        if (strlen(val) == 0) {
            val = NULL;
        }

        if (taosStrcasecmp(key, "endpoint") == 0) {
            ss->bucketContext.hostName = val;
        } else if (taosStrcasecmp(key, "bucket") == 0) {
            ss->bucketContext.bucketName = val;
        } else if (taosStrcasecmp(key, "uriStyle") == 0) {
            if (taosStrcasecmp(val, "path") == 0) {
                ss->bucketContext.uriStyle = S3UriStylePath;
            } else if (taosStrcasecmp(val, "virtualhost") == 0) {
                ss->bucketContext.uriStyle = S3UriStyleVirtualHost;
            } else {
                tssError("unknown uriStyle '%s' in access string: %s", val, as);
                return false;
            }
        } else if (taosStrcasecmp(key, "protocol") == 0) {
            if (taosStrcasecmp(val, "http") == 0) {
                ss->bucketContext.protocol = S3ProtocolHTTP;
            } else if (taosStrcasecmp(val, "https") == 0) {
                ss->bucketContext.protocol = S3ProtocolHTTPS;
            } else {
                tssError("unknown protocol '%s' in access string: %s", val, as);
                return false;
            }
        } else if (taosStrcasecmp(key, "accessKeyId") == 0) {
            ss->bucketContext.accessKeyId = val;
        } else if (taosStrcasecmp(key, "secretAccessKey") == 0) {
            ss->bucketContext.secretAccessKey = val;
        } else if (taosStrcasecmp(key, "region") == 0) {
            ss->bucketContext.authRegion = val;
        } else {
            tssError("unknown key '%s' in access string: %s", key, as);
            return false;
        }
    }

    if (ss->bucketContext.hostName == NULL) {
        tssError("endpoint is not configured in access string: %s", as);
        return false;
    }

    if (ss->bucketContext.bucketName == NULL) {
        tssError("bucket is not configured in access string: %s", as);
        return false;
    }

    if (ss->bucketContext.accessKeyId == NULL) {
        tssError("accessKeyId is not configured in access string: %s", as);
        return false;
    }

    if (ss->bucketContext.secretAccessKey == NULL) {
        tssError("secretAccessKey is not configured in access string: %s", as);
        return false;
    }

    ss->putProperties.expires = -1;
    ss->putProperties.cannedAcl = S3CannedAclPrivate;

    return true;
}



// createInstance creates a SSharedStorageS3 instance from the access string.
// access string format:
//  s3:endpoint=xxx.xxx.com;bucket=xxx;uriStyle=path;protocol=https;accessKeyId=xxx;secretAccessKey=xxx;region=xxx
static int32_t createInstance(const char* accessString, SSharedStorageS3** ppSS) {
    size_t asLen = strlen(accessString) + 1;
    S3Status status = S3_initialize("tdengine", S3_INIT_ALL, NULL);
    if (status != S3StatusOK) {
        uError("Failed to initialize libs3: %s\n", S3_get_status_name(status));
        TAOS_RETURN(TSDB_CODE_FAILED);
    }

    SSharedStorageS3* ss = (SSharedStorageS3*)taosMemCalloc(1, sizeof(SSharedStorageS3) + asLen);
    if (!ss) {
        tssError("failed to allocate memory for SSharedStorageS3");
        s3_deinitialize();
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    if (initInstance(ss, accessString)) {
        *ppSS = ss;
        TAOS_RETURN(TSDB_CODE_SUCCESS);
    }

    taosMemFree(ss);
    s3_deinitialize();
    TAOS_RETURN(TSDB_CODE_FAILED);
}



static int32_t closeInstance(SSharedStorage* ss) {
    SSharedStorageS3* s = (SSharedStorageS3*)ss;
    taosMemFree(s);
    s3_deinitialize();
    return TSDB_CODE_SUCCESS;
}


// upload

typedef struct {
    S3Status  status;
    char      err_msg[512];
    uint64_t  contentLen;
    TdFilePtr fin;
} uploadCallbackData;


static int should_retry() {
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static S3Status nopPropertiesCallback(const S3ResponseProperties *properties, void *data) {
  return S3StatusOK;
}

static int32_t uploadSmall(SSharedStorageS3* ss, const char* dstPath, uploadCallbackData* cbd) {
  int32_t            code = 0;
  S3PutObjectHandler poh = {
    .responseHandler = {
        .propertiesCallback = &nopPropertiesCallback,
        //.completeCallback = &responseCompleteCallback,
    },
    //.dataCallback = &putObjectDataCallback
  };

  do {
    S3_put_object(&ss->bucketContext, dstPath, cbd->contentLen, &ss->putProperties, 0, 0, &poh, cbd);
  } while (S3_status_is_retryable(cbd->status) && should_retry());

  if (cbd->status != S3StatusOK) {
    // s3PrintError(__FILE__, __LINE__, __func__, data->status, data->err_msg);
    s3PrintError(NULL, __LINE__, __func__, cbd->status, cbd->err_msg);
    code = TAOS_SYSTEM_ERROR(EIO);
  } else if (cbd->contentLen) {
    tssError("failed to put remaining %llu bytes", (unsigned long long)cbd->contentLen);
    code = TAOS_SYSTEM_ERROR(EIO);
  }

  TAOS_RETURN(code);
}


static int32_t upload(SSharedStorage* pss, const char* dstPath, const char* srcPath, int64_t offset, int64_t size) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    if (offset < 0) {
        tssError("invalid offset %ld for file %s: ", offset, srcPath);
        TAOS_RETURN(TSDB_CODE_INVALID_PARA);
    }

    int32_t code = 0;
    uploadCallbackData cbd = { 0 };

    if (taosStatFile(srcPath, (int64_t *)&cbd.contentLen, NULL, NULL) < 0) {
        tssError("failed to stat file %s: ", srcPath);
        TAOS_RETURN(terrno);
    }

    if (size < 0) {
        size = cbd.contentLen - offset;
    }
    if (size < 0 || offset + size > cbd.contentLen) {
        tssError("invalid offset %ld and size %ld for file %s: ", offset, size, srcPath);
        TAOS_RETURN(TSDB_CODE_INVALID_PARA);
    }
    
    if (!(cbd.fin = taosOpenFile(srcPath, TD_FILE_READ))) {
        tssError("failed to open file %s: ", srcPath);
        TAOS_RETURN(terrno);
    }

    if (offset > 0 && taosLSeekFile(cbd.fin, offset, SEEK_SET) < 0) {
        tssError("failed to seek file %s to offset %ld", srcPath, offset);
        (void)taosCloseFile(&cbd.fin);
        TAOS_RETURN(terrno);
    }

    if (cbd.contentLen > MULTIPART_CHUNK_SIZE) {

    } else {

    }

    if (cbd.fin) {
        (void)taosCloseFile(&cbd.fin);
    }
    TAOS_RETURN(code);
}



// Amazon S3
static int32_t s3CreateInstance(const char* as, SSharedStorage** ppSS);

static const SSharedStorageType s3 = {
    .name = "s3",
    .createInstance = s3CreateInstance,
    .closeInstance = closeInstance,
    .upload = upload,
};

static int32_t s3CreateInstance(const char* as, SSharedStorage** ppSS) {
    SSharedStorageS3* ss = NULL;

    int32_t code = createInstance(as, &ss);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }
    ss->type = &s3;

    *ppSS = (SSharedStorage*)ss;
    return TSDB_CODE_SUCCESS;
}


// Aliyun OSS
static int32_t ossCreateInstance(const char* as, SSharedStorage** ppSS);

static const SSharedStorageType oss = {
    .name = "oss",
    .createInstance = ossCreateInstance,
    .closeInstance = closeInstance,
    .upload = upload,
};

static int32_t ossCreateInstance(const char* as, SSharedStorage** ppSS) {
    SSharedStorageS3* ss = NULL;

    int32_t code = createInstance(as, &ss);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }
    ss->type = &oss;

    *ppSS = (SSharedStorage*)ss;
    return TSDB_CODE_SUCCESS;
}


// Tencent COS
static int32_t cosCreateInstance(const char* as, SSharedStorage** ppSS);

static const SSharedStorageType cos = {
    .name = "cos",
    .createInstance = cosCreateInstance,
    .closeInstance = closeInstance,
    .upload = upload,
};

static int32_t cosCreateInstance(const char* as, SSharedStorage** ppSS) {
    SSharedStorageS3* ss = NULL;

    int32_t code = createInstance(as, &ss);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }
    ss->type = &cos;

    *ppSS = (SSharedStorage*)ss;
    return TSDB_CODE_SUCCESS;
}



void s3RegisterType() {
    tssRegisterType(&s3);
    tssRegisterType(&oss);
    tssRegisterType(&cos);
}

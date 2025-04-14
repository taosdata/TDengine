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

        if (taosStrcasecmp(key, "hostname") == 0) {
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
        tssError("hostname is not configured in access string: %s", as);
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
//  s3:hostname=xxx.yyy.com;bucket=xxx;uriStyle=path;protocol=https;accessKeyId=xxx;secretAccessKey=xxx;region=xxx
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



// Common fields of all callback data
typedef struct {
    S3Status  status;
    char      errMsg[512];
} SCallbackData;



// responseCompleteCallback is shared by all operations, it saves the status and generates error
// messages from S3ErrorDetails if there is an error.
static void responseCompleteCallback(S3Status status, const S3ErrorDetails *error, void *data) {
    SCallbackData* cbd = (SCallbackData*)data;
    cbd->status = status;
    if (!error) {
        return;
    }

    int       len = 0;
    const int elen = sizeof(cbd->errMsg);
    
    if (error->message && elen - len > 0) {
        len += tsnprintf(cbd->errMsg + len, elen - len, "  Message: %s\n", error->message);
    }

    if (error->resource && elen - len > 0) {
        len += tsnprintf(cbd->errMsg + len, elen - len, "  Resource: %s\n", error->resource);
    }

    if (error->furtherDetails && elen - len > 0) {
        len += tsnprintf(cbd->errMsg + len, elen - len, "  Further Details: %s\n", error->furtherDetails);
    }

    if (error->extraDetailsCount && elen - len <= 0) {
        return;
    }

    len += tsnprintf(&(cbd->errMsg[len]), elen - len, "%s", "  Extra Details:\n");
    for (int i = 0; i < error->extraDetailsCount && elen > len; i++) {
        const char* name = error->extraDetails[i].name;
        const char* value = error->extraDetails[i].value;
        len += tsnprintf(cbd->errMsg + len, elen - len, "    %s: %s\n", name, value);
    }
}



// a properties callback that does nothing.
static S3Status propertiesCallbackNop(const S3ResponseProperties *properties, void *data) {
    return S3StatusOK;
}



static int shouldRetry() {
    return 1;
}



// functions and data structures for upload

// SUploadSource represents the source of data to be uploaded.
// It can be a file, a memory buffer, or another upload source,
// but cannot be two or more at the same time.
//
// The inner upload source is only used for multipart upload, in which
// case the inner source represents the original source of data, while
// the outer represents the current part being uploaded.
typedef struct {
    TdFilePtr      file;
    const char*    buf;
    SUploadSource* src;
    uint64_t       size;
    uint64_t       offset;
} SUploadSource;


static int readUploadSource(SUploadSource* us, char* buffer, int size) {
    if (us->offset >= us->size) {
        return 0;
    }

    if (size > us->size - us->offset) {
        size = us->size - us->offset;
    }

    if (us->src != NULL) {
        size = readUploadSource(us->src, buffer, size);
    } else if (us->buf != NULL) {
        memcpy(buffer, us->buf + us->offset, size);
    } else {
        size = taosReadFile(us->file, buffer, size);
    }

    if (size > 0) {
        us->offset += size;
    }

    return size;
}



// SUploadCallbackData is used in both simpleUpload and multipartUpload.
typedef struct {
    // common fields
    S3Status  status;
    char      errMsg[512];

    // upload-specific fields
    SUploadSource* src;

    // multipart upload specific fields
    uint64_t chunkSize;
    int      numChunks;
    int      seq;  // sequence number of current part
    char*    uploadId;
    char**   etags;
} SUploadCallbackData;



// dataCallback reads data and returns it to the S3 library.
static int dataCallback(int bufferSize, char* buffer, void* cbd) {
    SUploadCallbackData* ucbd = (SUploadCallbackData*)cbd;
    return readUploadSource(ucbd->src, buffer, bufferSize);
}



// simpleUpload uploads the file in a single request.
static int32_t simpleUpload(SSharedStorageS3* ss, const char* dstPath, SUploadSource* src) {
    int32_t            code = 0;

    SUploadCallbackData ucbd = {0};
    ucbd.src = src;

    S3PutObjectHandler poh = {
        .responseHandler = {
            .propertiesCallback = &propertiesCallbackNop,
            .completeCallback = &responseCompleteCallback,
        },
        .putObjectDataCallback = &dataCallback,
    };

    do {
        S3_put_object(&ss->bucketContext, dstPath, src->size, &ss->putProperties, 0, 0, &poh, &ucbd);
    } while (S3_status_is_retryable(ucbd.status) && shouldRetry());

    if (ucbd.status != S3StatusOK) {
        tssError("failed to upload %s: %d/%s", dstPath, ucbd.status, ucbd.errMsg);
        code = TAOS_SYSTEM_ERROR(EIO);
    } else if (src->size > src->offset) {
        tssError("failed to put remaining %llu bytes", src->size - src->offset);
        code = TAOS_SYSTEM_ERROR(EIO);
    }

    TAOS_RETURN(code);
}



// multipartInitialCallback saves the upload ID to callback data
static S3Status multipartInitialCallback(const char *uploadId, void *cbd) {
    SUploadCallbackData* ucbd = (SUploadCallbackData*)cbd;
    ucbd->uploadId = strdup(uploadId);
    ucbd->status = S3StatusOK;
    return S3StatusOK;
}



// multipartInitialCallback saves the eTag of current part to callback data
static S3Status multipartResponseProperiesCallback(const S3ResponseProperties *properties, void *cbd) {
    SUploadCallbackData* ucbd = (SUploadCallbackData*)cbd;
    ucbd->etags[ucbd->seq] = strdup(properties->eTag);
    return S3StatusOK;
}



static int32_t initMultipartUpload(SSharedStorageS3* ss, const char* dstPath, SUploadCallbackData* ucbd) {
    S3MultipartInitialHandler mih = {
        .responseHandler = {
            .propertiesCallback = &propertiesCallbackNop,
            .completeCallback = &responseCompleteCallback,
        },
        .responseXmlCallback = &multipartInitialCallback,
    };

    do {
        S3_initiate_multipart(&ss->bucketContext, dstPath, 0, &mih, 0, 0, &ucbd);
    } while (S3_status_is_retryable(ucbd->status) && shouldRetry());

    int32_t code = 0;
    if (ucbd->uploadId == NULL || ucbd->status != S3StatusOK) {
        tssError("failed to initiate multipart upload %s: %d/%s", dstPath, ucbd->status, ucbd->errMsg);
        code = TAOS_SYSTEM_ERROR(EIO);
    }

    TAOS_RETURN(code);
}



static int32_t uploadChunks(SSharedStorageS3* ss, const char* dstPath, SUploadCallbackData* ucbd) {
    S3PutObjectHandler poh = {
        .responseHandler = {
            .propertiesCallback = &multipartResponseProperiesCallback,
            .completeCallback = &responseCompleteCallback,
        },
        .putObjectDataCallback = &dataCallback,
    };

    int32_t code = 0;
    SUploadSource* src = ucbd->src->src; // inner source is the original source
    for(ucbd->seq = 0; ucbd->seq < ucbd->numChunks; ucbd->seq++) {
        ucbd->src->offset = 0;
        ucbd->src->size = TMIN(ucbd->chunkSize, src->size - src->offset);

        do {
            S3_upload_part(&ss->bucketContext,
                           dstPath,
                           &ss->putProperties,
                           &poh,
                           ucbd->seq + 1,
                           ucbd->uploadId,
                           ucbd->src->size,
                           NULL,
                           0,
                           &ucbd);
        } while (S3_status_is_retryable(ucbd->status) && shouldRetry());

        if (ucbd->status != S3StatusOK) {
            tssError("failed to upload part %d of %s: %d/%s", ucbd->seq, dstPath, ucbd->status, ucbd->errMsg);
            code = TAOS_SYSTEM_ERROR(EIO);
            break;
        }
    }

    TAOS_RETURN(code);
}



static int32_t commitMultipartUpload(SSharedStorageS3* ss, const char* dstPath, SUploadCallbackData* ucbd) {
    // calculate the size of the XML document
    int64_t size = strlen("<CompleteMultipartUpload></CompleteMultipartUpload>");
    size += ucbd->numChunks * strlen("<Part><PartNumber></PartNumber><ETag></ETag></Part>");
    size += ucbd->numChunks * 5;   // for the part number, 5 digits should be enough
    for (int i = 0; i < ucbd->numChunks; i++) {
        size += strlen(ucbd->etags[i]);
    }

    char* xml = (char*)taosMemMalloc(size);
    if (xml == NULL) {
        tssError("failed to allocate memory for commit XML");
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);  
    }

    // build the XML document
    char* p = xml;
    p += sprintf(p, "<CompleteMultipartUpload>");
    for (int i = 0; i < ucbd->numChunks; i++) {
        p += sprintf(p, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i + 1, ucbd->etags[i]);
    }
    p += sprintf(p, "</CompleteMultipartUpload>");

    // set the upload source to the XML document
    SUploadSource src = {.src = NULL, .buf = xml, .size = p-xml, .offset = 0, .file = NULL};
    SUploadSource* bakSrc = ucbd->src;
    ucbd->src = &src;

    S3MultipartCommitHandler mch = {
        .responseHandler = {
            .propertiesCallback = &propertiesCallbackNop,
            .completeCallback = &responseCompleteCallback,
        },
        .putObjectDataCallback = &dataCallback,
        .responseXmlCallback = NULL,
    };

    do {
        S3_complete_multipart_upload(&ss->bucketContext, dstPath, &mch, ucbd->uploadId, src.size, 0, 0, ucbd);
    } while (S3_status_is_retryable(ucbd->status) && shouldRetry());

    ucbd->src = bakSrc;
    taosMemFree(xml);

    int32_t code = 0;
    if (ucbd->status != S3StatusOK) {
        tssError("failed to commit multipart upload %s: %d/%s", dstPath, ucbd->status, ucbd->errMsg);
        code = TAOS_SYSTEM_ERROR(EIO);
    }

    TAOS_RETURN(code);
}



static int32_t doMultipartUpload(SSharedStorageS3* ss, const char* dstPath, SUploadCallbackData* ucbd) {
    const int maxChunks = 10000;
    SUploadSource* src = ucbd->src->src; // inner source is the original source

    ucbd->chunkSize = MULTIPART_CHUNK_SIZE;
    ucbd->numChunks = (src->size + ucbd->chunkSize - 1) / ucbd->chunkSize;
    if (ucbd->numChunks > maxChunks) {
        ucbd->chunkSize = (src->size + maxChunks - 1) / maxChunks;
        ucbd->numChunks = (src->size + ucbd->chunkSize - 1) / ucbd->chunkSize;
    }

    ucbd->etags = (char**)taosMemCalloc(ucbd->numChunks, sizeof(char*));
    if (!ucbd->etags) {
        tssError("failed to allocate memory for etags");
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    int32_t code = initMultipartUpload(ss, dstPath, &ucbd);
    if (code != TSDB_CODE_SUCCESS) {
        TAOS_RETURN(code);
    }

    code = uploadChunks(ss, dstPath, &ucbd);
    if (code != TSDB_CODE_SUCCESS) {
        TAOS_RETURN(code);
    }

    return commitMultipartUpload(ss, dstPath, &ucbd);
}



static void abortMultipartUpload(SSharedStorageS3* ss, const char* dstPath, SUploadCallbackData* ucbd) {
    S3AbortMultipartUploadHandler amh = {
        .responseHandler = {
            .propertiesCallback = &propertiesCallbackNop,
            .completeCallback = &responseCompleteCallback,
        },
    };

    do {
        S3_abort_multipart_upload(&ss->bucketContext, dstPath, ucbd->uploadId, 0, &amh);
    } while (S3_status_is_retryable(ucbd->status) && shouldRetry());

    if (ucbd->status != S3StatusOK) {
        tssError("failed to abort multipart upload %s: %d/%s", dstPath, ucbd->status, ucbd->errMsg);
    }

    return TSDB_CODE_SUCCESS;
}



// multipartUpload uploads the file via multiple parts requests.
static int32_t multipartUpload(SSharedStorageS3* ss, const char* dstPath, SUploadSource* src) {
    SUploadSource csrc = {.src = src, .buf = NULL, .size = 0, .offset = 0, .file = NULL};

    SUploadCallbackData ucbd = {0};
    ucbd.src = &csrc;

    int32_t code = doMultipartUpload(ss, dstPath, &ucbd);
    if (code != TSDB_CODE_SUCCESS) {
        abortMultipartUpload(ss, dstPath, &ucbd);
    }

    if (ucbd.uploadId) {
        free(ucbd.uploadId);
    }

    if (ucbd.etags) {
        for (int i = 0; i <= ucbd.numChunks; i++) {
            free(ucbd.etags[i]);
        }
        taosMemFree(ucbd.etags);
    }

    TAOS_RETURN(code);
}



// upload uploads a block of data to the shared storage at the specified path.
// It uploads an empty file if size is 0.
static int32_t upload(SSharedStorage* pss, const char* dstPath, const void* data, int64_t size) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    SUploadSource src = {.src = NULL, .buf = (const char*)data, .size = size, .offset = 0, .file = NULL};

    if (size <= MULTIPART_CHUNK_SIZE) {
        return simpleUpload(ss, dstPath, &src);
    } else {
        return multipartUpload(ss, dstPath, &src);
    }
}



// uploadFile uploads a file to the shared storage at the specified path.
// offset is the offset in the file, size is the size of the data to be uploaded.
// If size is negative, upload until the end of the file. If size is 0, upload an empty file.
static int32_t uploadFile(SSharedStorage* pss, const char* dstPath, const char* srcPath, int64_t offset, int64_t size) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    if (offset < 0) {
        tssError("invalid offset %ld for file %s: ", offset, srcPath);
        TAOS_RETURN(TSDB_CODE_INVALID_PARA);
    }

    int64_t fileSize = 0;
    if (taosStatFile(srcPath, &fileSize, NULL, NULL) < 0) {
        tssError("failed to stat file %s: ", srcPath);
        TAOS_RETURN(terrno);
    }

    // if size is negative, upload until the end of the file
    if (size < 0) {
        size = fileSize - offset;
    }
    if (size < 0 || offset + size > fileSize) {
        tssError("invalid offset %ld and size %ld for file %s: ", offset, size, srcPath);
        TAOS_RETURN(TSDB_CODE_INVALID_PARA);
    }

    TdFilePtr file = taosOpenCFile(srcPath, TD_FILE_READ);
    if (file == NULL) {
        tssError("failed to open file %s: ", srcPath);
        TAOS_RETURN(terrno);
    }

    if (offset > 0 && taosLSeekFile(file, offset, SEEK_SET) < 0) {
        tssError("failed to seek file %s to offset %ld", srcPath, offset);
        (void)taosCloseFile(&file);
        TAOS_RETURN(terrno);
    }

    SUploadSource src = {.src = NULL, .buf = NULL, .size = size, .offset = 0, .file = file};

    int32_t code = 0;
    if (size > MULTIPART_CHUNK_SIZE) {
        code = multipartUpload(ss, dstPath, &src);
    } else {
        code = simpleUpload(ss, dstPath, &src);
    }

    (void)taosCloseFile(&file);
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
    // OSS always uses path style URI
    ss->bucketContext.uriStyle = S3UriStylePath;

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



// register the S3, OSS and COS types
void s3RegisterType() {
    tssRegisterType(&s3);
    tssRegisterType(&oss);
    tssRegisterType(&cos);
}

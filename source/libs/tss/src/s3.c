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

#ifdef USE_S3

#include "libs3.h"


// S3 storage instance, also used for Aliyun OSS and Tencent COS.
typedef struct {
    // [type] is inherited from SSharedStorage, it must be the first member
    // to allow casting to SSharedStorage.
    const SSharedStorageType* type;

    // type-specific data

    // default chunk size in a multipart upload, files larger than this size
    // will use multipart upload.
    uint32_t        defaultChunkSizeInMB;
    // max number of allowed chunks in a multipart upload.
    uint32_t        maxChunks;
    // max retry times when encounter retryable errors, default is 3, negative
    // value means unlimited retry until success.
    int32_t         maxRetry;
    S3BucketContext bucketContext;
    S3PutProperties putProperties;

    // variable-length buffer for hostname, bucket and etc.
    char buf[0];
} SSharedStorageS3;



// printConfig implements SSharedStorageType::printConfig.
static void printConfig(SSharedStorage* pss) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    printf("type           : %s\n", ss->type->name);
    printf("endpoint       : %s\n", ss->bucketContext.hostName ? ss->bucketContext.hostName : "<default>");
    printf("bucket         : %s\n", ss->bucketContext.bucketName);
    printf("region         : %s\n", ss->bucketContext.authRegion ? ss->bucketContext.authRegion : "<default>");
    printf("protocol       : %s\n", ss->bucketContext.protocol == S3ProtocolHTTP ? "http" : "https");
    printf("uriStyle       : %s\n", ss->bucketContext.uriStyle == S3UriStylePath ? "path" : "virtualHost");
    printf("accessKeyId    : %s\n", ss->bucketContext.accessKeyId);
    printf("secretAccessKey: [omitted, sensitive information]\n");
    printf("chunkSize      : %uMB\n", ss->defaultChunkSizeInMB);
    printf("maxChunks      : %u\n", ss->maxChunks);
    printf("maxRetry       : %d\n", ss->maxRetry);
}



// initInstance initializes the SSharedStorageS3 instance from the access string.
static bool initInstance(SSharedStorageS3* ss, const char* as) {
    strcpy(ss->buf, as);

    // set default values
    ss->defaultChunkSizeInMB = 64;
    ss->maxChunks = 10000;
    ss->maxRetry = 3;

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
            // endpoint is the host name of the S3 service
            ss->bucketContext.hostName = val;
        } else if (taosStrcasecmp(key, "bucket") == 0) {
            ss->bucketContext.bucketName = val;
        } else if (taosStrcasecmp(key, "uriStyle") == 0 && val != NULL) {
            if (taosStrcasecmp(val, "path") == 0) {
                ss->bucketContext.uriStyle = S3UriStylePath;
            } else if (taosStrcasecmp(val, "virtualhost") == 0) {
                ss->bucketContext.uriStyle = S3UriStyleVirtualHost;
            } else {
                tssError("unknown uriStyle '%s' in access string: %s", val, as);
                return false;
            }
        } else if (taosStrcasecmp(key, "protocol") == 0 && val != NULL) {
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
        } else if (taosStrcasecmp(key, "chunkSize") == 0 && val != NULL) {
            ss->defaultChunkSizeInMB = (uint32_t)atoll(val);
        } else if (taosStrcasecmp(key, "maxChunks") == 0 && val != NULL) {
            ss->maxChunks = (uint32_t)atoll(val);
        } else if (taosStrcasecmp(key, "maxRetry") == 0 && val != NULL) {
            ss->maxRetry = (int32_t)atol(val);
        } else {
            tssError("unknown key '%s' in access string: %s", key, as);
            return false;
        }
    }

    if (ss->defaultChunkSizeInMB == 0) {
        tssError("invalid chunkSize in access string: %s", as);
        return false;
    }

    if (ss->maxChunks <= 1) {
        tssError("invalid maxChunks in access string: %s", as);
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



// createInstance implements SSharedStorageType::createInstance.
// access string format:
//  s3:endpoint=s3.amazonaws.com;bucket=mybucket;uriStyle=path;protocol=https;accessKeyId=AKIA26SHLXUZKC56MEOY;secretAccessKey=xxxxxxx;region=us-east-2;chunkSize=64;maxChunks=10000
static int32_t createInstance(const char* accessString, SSharedStorageS3** ppSS) {
    size_t asLen = strlen(accessString) + 1;
    S3Status status = S3_initialize("tdengine", S3_INIT_ALL, NULL);
    if (status != S3StatusOK) {
        tssError("Failed to initialize libs3: %s", S3_get_status_name(status));
        TAOS_RETURN(TSDB_CODE_FAILED);
    }

    SSharedStorageS3* ss = (SSharedStorageS3*)taosMemCalloc(1, sizeof(SSharedStorageS3) + asLen);
    if (!ss) {
        tssError("failed to allocate memory for SSharedStorageS3");
        S3_deinitialize();
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    if (initInstance(ss, accessString)) {
        *ppSS = ss;
        TAOS_RETURN(TSDB_CODE_SUCCESS);
    }

    taosMemFree(ss);
    S3_deinitialize();
    TAOS_RETURN(TSDB_CODE_FAILED);
}



static int32_t closeInstance(SSharedStorage* pss) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;
    taosMemFree(ss);
    S3_deinitialize();
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


// functions and data structures for upload & uploadFile.

// SUploadSource represents the source of data to be uploaded.
// It can be a file, a memory buffer, or another upload source,
// but cannot be two or more at the same time.
//
// The inner upload source is only used for multipart upload, in which
// case the inner source represents the original source of data, while
// the outer represents the current part being uploaded.
typedef struct SUploadSource {
    TdFilePtr             file;
    const char*           buf;
    struct SUploadSource* src;
    uint64_t              size;
    uint64_t              offset;
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
    uint32_t numChunks;
    uint32_t seq;  // sequence number of current part
    char*    uploadId;
    char**   etags;
} SUploadCallbackData;



// uploadCallback reads data and returns it to the S3 library.
static int uploadCallback(int bufferSize, char* buffer, void* cbd) {
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
        .putObjectDataCallback = &uploadCallback,
    };

    int32_t retry = 0;
    while (1) {
        S3_put_object(&ss->bucketContext, dstPath, src->size, &ss->putProperties, 0, 0, &poh, &ucbd);
        if (!S3_status_is_retryable(ucbd.status)) {
            break;
        }
        if (ss->maxRetry >= 0 && retry >= ss->maxRetry) {
            break;
        }
        ++retry;
        tssDebug("simpleUpload failed %s: %s, retry: %d", dstPath, S3_get_status_name(ucbd.status), retry);
    }

    if (ucbd.status != S3StatusOK) {
        tssError("simpleUpload failed %s: %s/%s", dstPath, S3_get_status_name(ucbd.status), ucbd.errMsg);
        code = TAOS_SYSTEM_ERROR(EIO);
    } else if (src->size > src->offset) {
        tssError("simpleUpload failed to put remaining %lu bytes", src->size - src->offset);
        code = TAOS_SYSTEM_ERROR(EIO);
    }

    TAOS_RETURN(code);
}



// multipartInitialCallback saves the upload ID to callback data
static S3Status multipartInitialCallback(const char *uploadId, void *cbd) {
    SUploadCallbackData* ucbd = (SUploadCallbackData*)cbd;

    char* id = taosStrdup(uploadId);
    if (id == NULL) {
        tssError("failed to allocate memory for upload ID");
        ucbd->status = S3StatusOutOfMemory;
    } else {
        ucbd->status = S3StatusOK;
        ucbd->uploadId = id;
    }

    return ucbd->status;
}



// multipartInitialCallback saves the eTag of current part to callback data
static S3Status multipartResponseProperiesCallback(const S3ResponseProperties *properties, void *cbd) {
    SUploadCallbackData* ucbd = (SUploadCallbackData*)cbd;

    char* etag = taosStrdup(properties->eTag);
    if (etag == NULL) {
        tssError("failed to allocate memory for eTag");
        ucbd->status = S3StatusOutOfMemory;
    } else {
        ucbd->status = S3StatusOK;
        ucbd->etags[ucbd->seq] = etag;
    }

    return ucbd->status;
}



static int32_t initMultipartUpload(SSharedStorageS3* ss, const char* dstPath, SUploadCallbackData* ucbd) {
    S3MultipartInitialHandler mih = {
        .responseHandler = {
            .propertiesCallback = &propertiesCallbackNop,
            .completeCallback = &responseCompleteCallback,
        },
        .responseXmlCallback = &multipartInitialCallback,
    };

    int32_t retry = 0;
    while (1) {
        S3_initiate_multipart(&ss->bucketContext, dstPath, 0, &mih, 0, 0, ucbd);
        if (!S3_status_is_retryable(ucbd->status)) {
            break;
        }
        if (ss->maxRetry >= 0 && retry >= ss->maxRetry) {
            break;
        }
        ++retry;
        tssDebug("initMultipartUpload failed %s: %s, retry: %d", dstPath, S3_get_status_name(ucbd->status), retry);
    }

    int32_t code = 0;
    if (ucbd->uploadId == NULL || ucbd->status != S3StatusOK) {
        tssError("initMultipartUpload failed %s: %s/%s", dstPath, S3_get_status_name(ucbd->status), ucbd->errMsg);
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
        .putObjectDataCallback = &uploadCallback,
    };

    int32_t code = 0;
    SUploadSource* src = ucbd->src->src; // inner source is the original source
    for(ucbd->seq = 0; ucbd->seq < ucbd->numChunks; ucbd->seq++) {
        ucbd->src->offset = 0;
        ucbd->src->size = TMIN(ucbd->chunkSize, src->size - src->offset);

        int32_t retry = 0;
        while (1) {
            S3_upload_part(&ss->bucketContext,
                           dstPath,
                           &ss->putProperties,
                           &poh,
                           (int)(ucbd->seq + 1),
                           ucbd->uploadId,
                           ucbd->src->size,
                           NULL,
                           0,
                           ucbd);
            if (!S3_status_is_retryable(ucbd->status)) {
                break;
            }
            if (ss->maxRetry >= 0 && retry >= ss->maxRetry) {
                break;
            }
            ++retry;
            tssDebug("uploadChunks failed to upload part %d of %s: %s, retry: %d", ucbd->seq, dstPath, S3_get_status_name(ucbd->status), retry);
        }

        if (ucbd->status != S3StatusOK) {
            tssError("uploadChunks failed to upload part %d of %s: %s/%s", ucbd->seq, dstPath, S3_get_status_name(ucbd->status), ucbd->errMsg);
            code = TAOS_SYSTEM_ERROR(EIO);
            break;
        }
    }

    TAOS_RETURN(code);
}



static int32_t commitMultipartUpload(SSharedStorageS3* ss, const char* dstPath, SUploadCallbackData* ucbd) {
    // calculate the size of the XML document
    size_t size = strlen("<CompleteMultipartUpload></CompleteMultipartUpload>");
    size += ucbd->numChunks * strlen("<Part><PartNumber></PartNumber><ETag></ETag></Part>");
    size += ucbd->numChunks * 5;   // for the part number, 5 digits should be enough
    for (uint32_t i = 0; i < ucbd->numChunks; i++) {
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
    for (uint32_t i = 0; i < ucbd->numChunks; i++) {
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
        .putObjectDataCallback = &uploadCallback,
        .responseXmlCallback = NULL,
    };

    int32_t retry = 0;
    while (1) {
        S3_complete_multipart_upload(&ss->bucketContext, dstPath, &mch, ucbd->uploadId, src.size, 0, 0, ucbd);
        if (!S3_status_is_retryable(ucbd->status)) {
            break;
        }
        if (ss->maxRetry >= 0 && retry >= ss->maxRetry) {
            break;
        }
        ++retry;
        tssDebug("commitMultipartUpload failed %s: %s, retry: %d", dstPath, S3_get_status_name(ucbd->status), retry);
    }

    ucbd->src = bakSrc;
    taosMemFree(xml);

    int32_t code = 0;
    if (ucbd->status != S3StatusOK) {
        tssError("commitMultipartUpload failed %s: %s/%s", dstPath, S3_get_status_name(ucbd->status), ucbd->errMsg);
        code = TAOS_SYSTEM_ERROR(EIO);
    }

    TAOS_RETURN(code);
}



static int32_t doMultipartUpload(SSharedStorageS3* ss, const char* dstPath, SUploadCallbackData* ucbd) {
    SUploadSource* src = ucbd->src->src; // inner source is the original source

    ucbd->chunkSize = ss->defaultChunkSizeInMB * (uint64_t)1024 * 1024;
    ucbd->numChunks = (src->size + ucbd->chunkSize - 1) / ucbd->chunkSize;
    if (ucbd->numChunks > ss->maxChunks) {
        ucbd->chunkSize = (src->size + ss->maxChunks - 1) / ss->maxChunks;
        ucbd->numChunks = (src->size + ucbd->chunkSize - 1) / ucbd->chunkSize;
    }

    ucbd->etags = (char**)taosMemCalloc(ucbd->numChunks, sizeof(char*));
    if (!ucbd->etags) {
        tssError("failed to allocate memory for etags");
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    int32_t code = initMultipartUpload(ss, dstPath, ucbd);
    if (code != TSDB_CODE_SUCCESS) {
        TAOS_RETURN(code);
    }

    code = uploadChunks(ss, dstPath, ucbd);
    if (code != TSDB_CODE_SUCCESS) {
        TAOS_RETURN(code);
    }

    return commitMultipartUpload(ss, dstPath, ucbd);
}



static void abortMultipartUpload(SSharedStorageS3* ss, const char* dstPath, SUploadCallbackData* ucbd) {
    S3AbortMultipartUploadHandler amh = {
        .responseHandler = {
            .propertiesCallback = &propertiesCallbackNop,
            .completeCallback = &responseCompleteCallback,
        },
    };

    int32_t retry = 0;
    while (1) {
        S3_abort_multipart_upload(&ss->bucketContext, dstPath, ucbd->uploadId, 0, &amh);
        if (!S3_status_is_retryable(ucbd->status)) {
            break;
        }
        if (ss->maxRetry >= 0 && retry >= ss->maxRetry) {
            break;
        }
        ++retry;
        tssDebug("abortMultipartUpload failed %s: %s, retry: %d", dstPath, S3_get_status_name(ucbd->status), retry);
    }

    if (ucbd->status != S3StatusOK) {
        tssError("abortMultipartUpload failed %s: %s/%s", dstPath, S3_get_status_name(ucbd->status), ucbd->errMsg);
    }
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
        taosMemoryFree(ucbd.uploadId);
    }

    if (ucbd.etags) {
        for (uint32_t i = 0; i < ucbd.numChunks; i++) {
            taosMemoryFree(ucbd.etags[i]);
        }
        taosMemFree(ucbd.etags);
    }

    TAOS_RETURN(code);
}



// upload implements SSharedStorageType::upload.
static int32_t upload(SSharedStorage* pss, const char* dstPath, const void* data, int64_t size) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    SUploadSource src = {.src = NULL, .buf = (const char*)data, .size = size, .offset = 0, .file = NULL};

    if (size <= ss->defaultChunkSizeInMB * (uint64_t)1024 * 1024) {
        return simpleUpload(ss, dstPath, &src);
    } else {
        tssInfo("multipart uploading: %s, size %ld", dstPath, size);
        return multipartUpload(ss, dstPath, &src);
    }
}



// uploadFile implements SSharedStorageType::uploadFile.
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

    TdFilePtr file = taosOpenFile(srcPath, TD_FILE_READ);
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
    if (size <= ss->defaultChunkSizeInMB * (uint64_t)1024 * 1024) {
        code = simpleUpload(ss, dstPath, &src);
    } else {
        tssInfo("multipart uploading: %s to %s, size %ld", srcPath, dstPath, size);
        code = multipartUpload(ss, dstPath, &src);
    }

    (void)taosCloseFile(&file);
    TAOS_RETURN(code);
}



// functions and data structures for readFile & downloadFile.

typedef struct {
    S3Status  status;
    char      errMsg[512];
    // * [buf] & [file] are destination of the downloaded data, they are
    //   mutually exclusive.
    // * [offset] is the number of bytes already downloaded.
    // * [size] is the total size to be downloaded:
    //   - if [buf] is used, [size] is also the size of the buffer, a zero
    //     [size] means no data to be downloaded.
    //   - if [file] is used, a zero [size] means download until the end of
    //     the file.
    char*     buf;
    TdFilePtr file;
    int64_t   offset;
    int64_t   size;
} SDownloadCallbackData;



static S3Status downloadCallback(int bufferSize, const char *buffer, void *cbd) {
    SDownloadCallbackData* dcbd = cbd;

    if (dcbd->buf != NULL) {
        (void)memcpy(dcbd->buf + dcbd->offset, buffer, bufferSize);
        dcbd->offset += bufferSize;
        dcbd->status = S3StatusOK;
        return S3StatusOK;
    }

    int64_t wrote = taosWriteFile(dcbd->file, buffer, bufferSize);
    if (wrote > 0) {
        dcbd->offset += wrote;
    }

    if (wrote == bufferSize) {
        dcbd->status = S3StatusOK;
    } else {
        dcbd->status = S3StatusAbortedByCallback;
    }

    return dcbd->status;
}



static int32_t doDownload(SSharedStorageS3* ss, const char* path, int64_t offset, SDownloadCallbackData* dcbd) {
    int32_t code = 0;

    S3GetObjectHandler goh = {
        .responseHandler = {
            .propertiesCallback = &propertiesCallbackNop,
            .completeCallback = &responseCompleteCallback,
        },
        .getObjectDataCallback = &downloadCallback,
    };

    for (int retry = 0; retry < 5; retry++) {
        int64_t start = offset + dcbd->offset, bytes = 0;

        // S3_get_object() will return data up to the end of the content if bytes is 0,
        // this is only desired when (dcbd->file != NULL && dcbd->size == 0).
        if (dcbd->buf != NULL || dcbd->size != 0) {
            bytes = dcbd->size - dcbd->offset;
            if (bytes == 0) { // have already read all the required data.
                break;
            }
        }

        S3_get_object(&ss->bucketContext, path, NULL, start, bytes, NULL, 0, &goh, dcbd);

        if( dcbd->status == S3StatusErrorSlowDown ) {
            taosMsleep(taosRand() % 2000 + 1000);
            tssInfo("retrying download %s, retry %d", path, retry + 1);
            continue;
        }

        if (dcbd->status == S3StatusErrorNoSuchKey || dcbd->status == S3StatusHttpErrorNotFound) {
            tssError("failed to download %s: %s/%s", path, S3_get_status_name(dcbd->status), dcbd->errMsg);
            code = TSDB_CODE_NOT_FOUND;
        } else if( dcbd->status != S3StatusOK ) {
            tssError("failed to download %s: %s/%s", path, S3_get_status_name(dcbd->status), dcbd->errMsg);
            code = TAOS_SYSTEM_ERROR(EIO);
        }

        break;
    }

    TAOS_RETURN(code);
}



// readFile implements SSharedStorageType::readFile.
static int32_t readFile(SSharedStorage* pss, const char* srcPath, int64_t offset, char* buffer, int64_t* size) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    SDownloadCallbackData dcbd = { .buf = buffer, .size = * size };
    int32_t code = doDownload(ss, srcPath, offset, &dcbd);
    *size = dcbd.offset;
    return code;
}



// downloadFile implements SSharedStorageType::downloadFile.
static int32_t downloadFile(SSharedStorage* pss, const char* srcPath, const char* dstPath, int64_t offset, int64_t size) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    TdFilePtr file = taosOpenFile(dstPath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    if (file == NULL) {
        tssError("failed to open file %s: ", dstPath);
        TAOS_RETURN(terrno);
    }

    SDownloadCallbackData dcbd = { .file = file, .size = size };
    int32_t code = doDownload(ss, srcPath, offset, &dcbd);
    (void)taosCloseFile(&file);

    TAOS_RETURN(code);
}



// functions and data structures for listFile.

typedef struct {
    S3Status status;
    char     errMsg[512];
    int      isTruncated;
    char     nextMarker[1024];
    SArray*  paths;
} SListCallbackData;



static S3Status listFileCallback(int                        isTruncated,
                                 const char*                nextMarker,
                                 int                        contentsCount,
                                 const S3ListBucketContent* contents,
                                 int                        commonPrefixesCount,
                                 const char**               commonPrefixes,
                                 void*                      cbd) {
    SListCallbackData* lcbd = (SListCallbackData*)cbd;

    lcbd->isTruncated = isTruncated;
    if (nextMarker != NULL) {
        strncpy(lcbd->nextMarker, nextMarker, sizeof(lcbd->nextMarker));
        lcbd->nextMarker[sizeof(lcbd->nextMarker) - 1] = 0;
    } else {
        lcbd->nextMarker[0] = 0;
    }

    lcbd->status = S3StatusOK;
    for (int i = 0; i < contentsCount; i++) {
        const char* key = contents[i].key;
        if (key[strlen(key) - 1] == '/') { // skip directories
            continue;
        }
        
        char* path = taosStrdup(key);
        if (path == NULL) {
            tssError("failed to allocate memory for list path");
            lcbd->status = S3StatusOutOfMemory;
            break;
        }

        if (taosArrayPush(lcbd->paths, &path) == NULL) {
            tssError("failed to append path to list");
            taosMemoryFree(path);
            lcbd->status = S3StatusOutOfMemory;
            break;
        }
    }

    return lcbd->status;
}


// listFile implements SSharedStorageType::listFile.
static int32_t listFile(SSharedStorage* pss, const char* prefix, SArray* paths) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    SListCallbackData lcbd = {.paths = paths};

    S3ListBucketHandler lbh = {
        .responseHandler = {
            .propertiesCallback = &propertiesCallbackNop,
            .completeCallback = &responseCompleteCallback,
        },
        .listBucketCallback = &listFileCallback,
    };

    do {
        lcbd.isTruncated = 0;
        S3_list_bucket(&ss->bucketContext, prefix, lcbd.nextMarker, NULL, 0, NULL, 0, &lbh, &lcbd);
    } while(lcbd.status == S3StatusOK && lcbd.isTruncated);

    int32_t code = 0;
    if (lcbd.status != S3StatusOK) {
        tssError("failed to list %s: %s/%s", prefix, S3_get_status_name(lcbd.status), lcbd.errMsg);

        for(size_t i = 0; i < taosArrayGetSize(paths); i++) {
            char* path = *(char**)taosArrayGet(paths, i);
            taosMemoryFree(path);
        }
        taosArrayClear(paths);

        code = TAOS_SYSTEM_ERROR(EIO);
    }

    TAOS_RETURN(code);
}



// functions and data structures for deleteFile.

// deleteFile implements SSharedStorageType::deleteFile.
static int32_t deleteFile(SSharedStorage* pss, const char* path) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    S3ResponseHandler rh = {
        .propertiesCallback = &propertiesCallbackNop,
        .completeCallback = &responseCompleteCallback,
    };

    SCallbackData cbd = {0};
    int32_t retry = 0;
    while (1) {
        S3_delete_object(&ss->bucketContext, path, NULL, 0, &rh, &cbd);
        if (!S3_status_is_retryable(cbd.status)) {
            break;
        }
        if (ss->maxRetry >= 0 && retry >= ss->maxRetry) {
            break;
        }
        ++retry;
        tssDebug("deleteFile failed %s: %s, retry: %d", path, S3_get_status_name(cbd.status), retry);
    }

    if (cbd.status == S3StatusOK || cbd.status == S3StatusErrorNoSuchKey || cbd.status == S3StatusHttpErrorNotFound) {
        TAOS_RETURN(TSDB_CODE_SUCCESS);
    }

    tssError("deleteFile failed %s: %s/%s", path, S3_get_status_name(cbd.status), cbd.errMsg);
    TAOS_RETURN(TSDB_CODE_FAILED);
}


// functions and data structures for getFileSize.

typedef struct {
    S3Status  status;
    char      errMsg[512];
    int64_t   size;
} SSizeCallbackData;


static S3Status sizePropertiesCallback(const S3ResponseProperties* properties, void* data) {
    SSizeCallbackData* scbd = (SSizeCallbackData*)data;
    if (properties->contentLength > 0) {
        scbd->status = S3StatusOK;
        scbd->size = properties->contentLength;
    } else {
        scbd->status = S3StatusErrorNoSuchKey;
    }
    return scbd->status;
}


// getFileSize implements SSharedStorageType::getFileSize.
static int32_t getFileSize(SSharedStorage* pss, const char* path, int64_t* size) {
    SSharedStorageS3* ss = (SSharedStorageS3*)pss;

    S3ResponseHandler rh = {
        .propertiesCallback = &sizePropertiesCallback,
        .completeCallback = &responseCompleteCallback,
    };

    SSizeCallbackData scbd = {0};
    int32_t retry = 0;
    while (1) {
        S3_head_object(&ss->bucketContext, path, NULL, 0, &rh, &scbd);
        if (!S3_status_is_retryable(scbd.status)) {
            break;
        }
        if (ss->maxRetry >= 0 && retry >= ss->maxRetry) {
            break;
        }
        ++retry;
        tssDebug("getFileSize failed %s: %s, retry: %d", path, S3_get_status_name(scbd.status), retry);
    }

    int code = 0;
    if (scbd.status == S3StatusOK) {
        *size = scbd.size;
    } else if (scbd.status == S3StatusErrorNoSuchKey || scbd.status == S3StatusHttpErrorNotFound) {
        tssError("getFileSize: file %s does not exist", path);
        code = TSDB_CODE_NOT_FOUND;
    } else {
        tssError("getFileSize failed %s: %s/%s", path, S3_get_status_name(scbd.status), scbd.errMsg);
        code = TSDB_CODE_FAILED;
    }

    TAOS_RETURN(code);
}



// Amazon S3
static int32_t s3CreateInstance(const char* as, SSharedStorage** ppSS);

static const SSharedStorageType sstS3 = {
    .name = "s3",
    .printConfig = &printConfig,
    .createInstance = &s3CreateInstance,
    .closeInstance = &closeInstance,
    .upload = &upload,
    .uploadFile = &uploadFile,
    .readFile = &readFile,
    .downloadFile = &downloadFile,
    .listFile = &listFile,
    .deleteFile = &deleteFile,
    .getFileSize = &getFileSize,
};

static int32_t s3CreateInstance(const char* as, SSharedStorage** ppSS) {
    SSharedStorageS3* ss = NULL;

    int32_t code = createInstance(as, &ss);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }
    ss->type = &sstS3;

    *ppSS = (SSharedStorage*)ss;
    return TSDB_CODE_SUCCESS;
}


#if 0
// Aliyun OSS
static int32_t ossCreateInstance(const char* as, SSharedStorage** ppSS);

static const SSharedStorageType sstOss = {
    .name = "oss",
    .printConfig = &printConfig,
    .createInstance = &ossCreateInstance,
    .closeInstance = &closeInstance,
    .upload = &upload,
    .uploadFile = &uploadFile,
    .readFile = &readFile,
    .downloadFile = &downloadFile,
    .listFile = &listFile,
    .deleteFile = &deleteFile,
    .getFileSize = &getFileSize,
};

static int32_t ossCreateInstance(const char* as, SSharedStorage** ppSS) {
    SSharedStorageS3* ss = NULL;

    int32_t code = createInstance(as, &ss);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }
    ss->type = &sstOss;
    // OSS always uses path style URI.
    ss->bucketContext.uriStyle = S3UriStylePath;

    *ppSS = (SSharedStorage*)ss;
    return TSDB_CODE_SUCCESS;
}


// Tencent COS
static int32_t cosCreateInstance(const char* as, SSharedStorage** ppSS);

static const SSharedStorageType sstCos = {
    .name = "cos",
    .printConfig = &printConfig,
    .createInstance = &cosCreateInstance,
    .closeInstance = &closeInstance,
    .upload = &upload,
    .uploadFile = &uploadFile,
    .readFile = &readFile,
    .downloadFile = &downloadFile,
    .listFile = &listFile,
    .deleteFile = &deleteFile,
    .getFileSize = &getFileSize,
};

static int32_t cosCreateInstance(const char* as, SSharedStorage** ppSS) {
    SSharedStorageS3* ss = NULL;

    int32_t code = createInstance(as, &ss);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }
    ss->type = &sstCos;

    *ppSS = (SSharedStorage*)ss;
    return TSDB_CODE_SUCCESS;
}
#endif // 0



// register the S3, OSS and COS types.
void s3RegisterType() {
    tssRegisterType(&sstS3);
#if 0
    tssRegisterType(&sstOss);
    tssRegisterType(&sstCos);
#endif // 0
}

#endif // USE_S3
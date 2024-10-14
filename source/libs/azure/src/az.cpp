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

#define ALLOW_FORBID_FUNC

#include "az.h"

#include "os.h"
#include "taoserror.h"
#include "tglobal.h"

#if defined(USE_S3)

#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>
#include "td_block_blob_client.hpp"

// Add appropriate using namespace directives
using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;

extern char tsS3Hostname[][TSDB_FQDN_LEN];
extern char tsS3AccessKeyId[][TSDB_FQDN_LEN];
extern char tsS3AccessKeySecret[][TSDB_FQDN_LEN];
extern char tsS3BucketName[TSDB_FQDN_LEN];

extern int8_t tsS3Enabled;
extern int8_t tsS3EpNum;

int32_t azBegin() { return TSDB_CODE_SUCCESS; }

void azEnd() {}

static void azDumpCfgByEp(int8_t epIndex) {
  // clang-format off
  (void)fprintf(stdout,
                "%-24s %s\n"
                "%-24s %s\n"
                "%-24s %s\n"
    //          "%-24s %s\n"
                "%-24s %s\n"
                "%-24s %s\n",
                "hostName", tsS3Hostname[epIndex],
                "bucketName", tsS3BucketName,
                "protocol", "https only",
    //"uristyle", (uriStyleG[epIndex] == S3UriStyleVirtualHost ? "virtualhost" : "path"),
                "accessKey", tsS3AccessKeyId[epIndex],
                "accessKeySecret", tsS3AccessKeySecret[epIndex]);
  // clang-format on
}

static int32_t azListBucket(char const *bucketname) {
  int32_t           code = 0;
  const std::string delimiter = "/";
  std::string       accountName = tsS3AccessKeyId[0];
  std::string       accountKey = tsS3AccessKeySecret[0];
  std::string       accountURL = tsS3Hostname[0];
  accountURL = "https://" + accountURL;

  try {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);

    StorageSharedKeyCredential *pSharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);

    BlobServiceClient blobServiceClient(accountURL, sharedKeyCredential);

    std::string containerName = bucketname;
    auto        containerClient = blobServiceClient.GetBlobContainerClient(containerName);

    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = "s3";

    (void)fprintf(stderr, "objects:\n");
    // std::set<std::string> listBlobs;
    for (auto pageResult = containerClient.ListBlobs(options); pageResult.HasPage(); pageResult.MoveToNextPage()) {
      for (const auto &blob : pageResult.Blobs) {
        (void)fprintf(stderr, "%s\n", blob.Name.c_str());
      }
    }
  } catch (const Azure::Core::RequestFailedException &e) {
    uError("%s failed at line %d since %d(%s)", __func__, __LINE__, static_cast<int>(e.StatusCode),
           e.ReasonPhrase.c_str());
    // uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TAOS_SYSTEM_ERROR(EIO)));

    code = TAOS_SYSTEM_ERROR(EIO);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t azCheckCfg() {
  int32_t code = 0, lino = 0;
  int8_t  i = 0;

  // for (; i < tsS3EpNum; i++) {
  (void)fprintf(stdout, "test s3 ep (%d/%d):\n", i + 1, tsS3EpNum);
  // s3DumpCfgByEp(i);
  azDumpCfgByEp(0);

  // test put
  char        testdata[17] = "0123456789abcdef";
  const char *objectname[] = {"s3test.txt"};
  char        path[PATH_MAX] = {0};
  int         ds_len = strlen(TD_DIRSEP);
  int         tmp_len = strlen(tsTempDir);

  (void)snprintf(path, PATH_MAX, "%s", tsTempDir);
  if (strncmp(tsTempDir + tmp_len - ds_len, TD_DIRSEP, ds_len) != 0) {
    (void)snprintf(path + tmp_len, PATH_MAX - tmp_len, "%s", TD_DIRSEP);
    (void)snprintf(path + tmp_len + ds_len, PATH_MAX - tmp_len - ds_len, "%s", objectname[0]);
  } else {
    (void)snprintf(path + tmp_len, PATH_MAX - tmp_len, "%s", objectname[0]);
  }

  uint8_t *pBlock = NULL;
  int      c_offset = 10;
  int      c_len = 6;
  char     buf[7] = {0};

  TdFilePtr fp = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_TRUNC);
  if (!fp) {
    (void)fprintf(stderr, "failed to open test file: %s.\n", path);
    // uError("ERROR: %s Failed to open %s", __func__, path);
    TAOS_CHECK_GOTO(terrno, &lino, _next);
  }
  if (taosWriteFile(fp, testdata, strlen(testdata)) < 0) {
    (void)fprintf(stderr, "failed to write test file: %s.\n", path);
    TAOS_CHECK_GOTO(terrno, &lino, _next);
  }
  if (taosFsyncFile(fp) < 0) {
    (void)fprintf(stderr, "failed to fsync test file: %s.\n", path);
    TAOS_CHECK_GOTO(terrno, &lino, _next);
  }
  (void)taosCloseFile(&fp);

  (void)fprintf(stderr, "\nstart to put object: %s, file: %s content: %s\n", objectname[0], path, testdata);
  code = azPutObjectFromFileOffset(path, objectname[0], 0, 16);
  if (code != 0) {
    (void)fprintf(stderr, "put object %s : failed.\n", objectname[0]);
    TAOS_CHECK_GOTO(code, &lino, _next);
  }
  (void)fprintf(stderr, "put object %s: success.\n\n", objectname[0]);

  // list buckets
  (void)fprintf(stderr, "start to list bucket %s by prefix s3.\n", tsS3BucketName);
  // code = s3ListBucketByEp(tsS3BucketName, i);
  code = azListBucket(tsS3BucketName);
  if (code != 0) {
    (void)fprintf(stderr, "listing bucket %s : failed.\n", tsS3BucketName);
    TAOS_CHECK_GOTO(code, &lino, _next);
  }
  (void)fprintf(stderr, "listing bucket %s: success.\n\n", tsS3BucketName);

  // test range get
  (void)fprintf(stderr, "start to range get object %s offset: %d len: %d.\n", objectname[0], c_offset, c_len);
  code = azGetObjectBlock(objectname[0], c_offset, c_len, true, &pBlock);
  if (code != 0) {
    (void)fprintf(stderr, "get object %s : failed.\n", objectname[0]);
    TAOS_CHECK_GOTO(code, &lino, _next);
  }

  (void)memcpy(buf, pBlock, c_len);
  taosMemoryFree(pBlock);
  (void)fprintf(stderr, "object content: %s\n", buf);
  (void)fprintf(stderr, "get object %s: success.\n\n", objectname[0]);

  // delete test object
  (void)fprintf(stderr, "start to delete object: %s.\n", objectname[0]);
  // code = azDeleteObjectsByPrefix(objectname[0]);
  azDeleteObjectsByPrefix(objectname[0]);
  /*
  if (code != 0) {
    (void)fprintf(stderr, "delete object %s : failed.\n", objectname[0]);
    TAOS_CHECK_GOTO(code, &lino, _next);
  }
  */
  (void)fprintf(stderr, "delete object %s: success.\n\n", objectname[0]);

_next:
  if (fp) {
    (void)taosCloseFile(&fp);
  }

  if (TSDB_CODE_SUCCESS != code) {
    (void)fprintf(stderr, "s3 check failed, code: %d, line: %d, index: %d.\n", code, lino, i);
  }

  (void)fprintf(stdout, "=================================================================\n");
  //}

  // azEnd();

  TAOS_RETURN(code);
}

int32_t azPutObjectFromFileOffset(const char *file, const char *object_name, int64_t offset, int64_t size) {
  int32_t code = 0;

  std::string endpointUrl = tsS3Hostname[0];        // GetEndpointUrl();
  std::string accountName = tsS3AccessKeyId[0];     // GetAccountName();
  std::string accountKey = tsS3AccessKeySecret[0];  // GetAccountKey();

  try {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);

    std::string                 accountURL = tsS3Hostname[0];
    StorageSharedKeyCredential *pSharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);

    accountURL = "https://" + accountURL;
    BlobServiceClient blobServiceClient(accountURL, sharedKeyCredential);

    std::string containerName = tsS3BucketName;
    auto        containerClient = blobServiceClient.GetBlobContainerClient(containerName);

    // Create the container if it does not exist
    // std::cout << "Creating container: " << containerName << std::endl;
    // containerClient.CreateIfNotExists();

    std::string blobName = "blob.txt";
    uint8_t     blobContent[] = "Hello Azure!";
    // Create the block blob client
    // BlockBlobClient blobClient = containerClient.GetBlockBlobClient(blobName);
    // TDBlockBlobClient blobClient(containerClient.GetBlobClient(blobName));
    TDBlockBlobClient blobClient(containerClient.GetBlobClient(object_name));

    // Upload the blob
    // std::cout << "Uploading blob: " << blobName << std::endl;
    // blobClient.UploadFrom(blobContent, sizeof(blobContent));
    blobClient.UploadFrom(file, offset, size);
    //(void)_azUploadFrom(blobClient, file, offset, size);
    /*
        auto blockBlobClient = BlockBlobClient(endpointUrl, sharedKeyCredential);

        // Create some data to upload into the blob.
        std::vector<uint8_t> data = {1, 2, 3, 4};
        Azure::Core::IO::MemoryBodyStream stream(data);

        Azure::Response<Models::UploadBlockBlobResult> response = blockBlobClient.Upload(stream);

        Models::UploadBlockBlobResult model = response.Value;
        std::cout << "Last modified date of uploaded blob: " << model.LastModified.ToString()
                  << std::endl;
    */
  } catch (const Azure::Core::RequestFailedException &e) {
    /*
    std::cout << "Status Code: " << static_cast<int>(e.StatusCode) << ", Reason Phrase: " << e.ReasonPhrase
              << std::endl;
    std::cout << e.what() << std::endl;
    */
    code = TAOS_SYSTEM_ERROR(EIO);
    uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t azGetObjectBlockImpl(const char *object_name, int64_t offset, int64_t size, bool check, uint8_t **ppBlock) {
  int32_t     code = TSDB_CODE_SUCCESS;
  std::string accountName = tsS3AccessKeyId[0];
  std::string accountKey = tsS3AccessKeySecret[0];
  std::string accountURL = tsS3Hostname[0];
  uint8_t    *buf = NULL;

  try {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);

    StorageSharedKeyCredential *pSharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);

    accountURL = "https://" + accountURL;
    BlobServiceClient blobServiceClient(accountURL, sharedKeyCredential);

    std::string containerName = tsS3BucketName;
    auto        containerClient = blobServiceClient.GetBlobContainerClient(containerName);

    TDBlockBlobClient blobClient(containerClient.GetBlobClient(object_name));

    Blobs::DownloadBlobToOptions options;
    options.Range = Azure::Core::Http::HttpRange();
    options.Range.Value().Offset = offset;
    options.Range.Value().Length = size;

    buf = (uint8_t *)taosMemoryCalloc(1, size);
    if (!buf) {
      return terrno;
    }

    auto res = blobClient.DownloadTo(buf, size, options);
    if (check && res.Value.ContentRange.Length.Value() != size) {
      code = TAOS_SYSTEM_ERROR(EIO);
      uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      TAOS_RETURN(code);
    }

    *ppBlock = buf;
  } catch (const Azure::Core::RequestFailedException &e) {
    uError("%s failed at line %d since %d(%s)", __func__, __LINE__, static_cast<int>(e.StatusCode),
           e.ReasonPhrase.c_str());
    code = TAOS_SYSTEM_ERROR(EIO);
    uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));

    if (buf) {
      taosMemoryFree(buf);
    }
    *ppBlock = NULL;

    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t azGetObjectBlock(const char *object_name, int64_t offset, int64_t size, bool check, uint8_t **ppBlock) {
  int32_t code = TSDB_CODE_SUCCESS;

  // May use an exponential backoff policy for retries with 503
  int        retryCount = 0;
  static int maxRetryCount = 5;
  static int minRetryInterval = 1000;  // ms
  static int maxRetryInterval = 3000;  // ms

_retry:
  code = azGetObjectBlockImpl(object_name, offset, size, check, ppBlock);
  if (TSDB_CODE_SUCCESS != code && retryCount++ < maxRetryCount) {
    taosMsleep(taosRand() % (maxRetryInterval - minRetryInterval + 1) + minRetryInterval);
    uInfo("%s: 0x%x(%s) and retry get object", __func__, code, tstrerror(code));
    goto _retry;
  }

  TAOS_RETURN(code);
}

void azDeleteObjectsByPrefix(const char *prefix) {
  const std::string delimiter = "/";
  std::string       accountName = tsS3AccessKeyId[0];
  std::string       accountKey = tsS3AccessKeySecret[0];
  std::string       accountURL = tsS3Hostname[0];
  accountURL = "https://" + accountURL;

  try {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);

    StorageSharedKeyCredential *pSharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);

    BlobServiceClient blobServiceClient(accountURL, sharedKeyCredential);

    std::string containerName = tsS3BucketName;
    auto        containerClient = blobServiceClient.GetBlobContainerClient(containerName);

    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = prefix;

    std::set<std::string> listBlobs;
    for (auto pageResult = containerClient.ListBlobs(options); pageResult.HasPage(); pageResult.MoveToNextPage()) {
      for (const auto &blob : pageResult.Blobs) {
        listBlobs.insert(blob.Name);
      }
    }

    for (auto blobName : listBlobs) {
      auto blobClient = containerClient.GetAppendBlobClient(blobName);
      blobClient.Delete();
    }
  } catch (const Azure::Core::RequestFailedException &e) {
    uError("%s failed at line %d since %d(%s)", __func__, __LINE__, static_cast<int>(e.StatusCode),
           e.ReasonPhrase.c_str());
    // uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TAOS_SYSTEM_ERROR(EIO)));
  }
}

int32_t azPutObjectFromFile2(const char *file, const char *object, int8_t withcp) {
  int32_t  code = 0, lino = 0;
  uint64_t contentLength = 0;

  if (taosStatFile(file, (int64_t *)&contentLength, NULL, NULL) < 0) {
    uError("ERROR: %s Failed to stat file %s: ", __func__, file);
    TAOS_RETURN(terrno);
  }

  code = azPutObjectFromFileOffset(file, object, 0, contentLength);
  if (code != 0) {
    uError("ERROR: %s Failed to put file %s: ", __func__, file);
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }

_exit:
  if (code) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return 0;
}

int32_t azGetObjectToFile(const char *object_name, const char *fileName) {
  int32_t     code = TSDB_CODE_SUCCESS;
  std::string accountName = tsS3AccessKeyId[0];
  std::string accountKey = tsS3AccessKeySecret[0];
  std::string accountURL = tsS3Hostname[0];
  accountURL = "https://" + accountURL;

  try {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);

    StorageSharedKeyCredential *pSharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);

    BlobServiceClient blobServiceClient(accountURL, sharedKeyCredential);

    std::string containerName = tsS3BucketName;
    auto        containerClient = blobServiceClient.GetBlobContainerClient(containerName);

    TDBlockBlobClient blobClient(containerClient.GetBlobClient(object_name));

    auto res = blobClient.DownloadTo(fileName);
    if (res.Value.ContentRange.Length.Value() <= 0) {
      code = TAOS_SYSTEM_ERROR(EIO);
      uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      TAOS_RETURN(code);
    }
  } catch (const Azure::Core::RequestFailedException &e) {
    uError("%s failed at line %d since %d(%s)", __func__, __LINE__, static_cast<int>(e.StatusCode),
           e.ReasonPhrase.c_str());
    code = TAOS_SYSTEM_ERROR(EIO);
    uError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

int32_t azGetObjectsByPrefix(const char *prefix, const char *path) {
  const std::string delimiter = "/";
  std::string       accountName = tsS3AccessKeyId[0];
  std::string       accountKey = tsS3AccessKeySecret[0];
  std::string       accountURL = tsS3Hostname[0];
  accountURL = "https://" + accountURL;

  try {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);

    StorageSharedKeyCredential *pSharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);

    BlobServiceClient blobServiceClient(accountURL, sharedKeyCredential);

    std::string containerName = tsS3BucketName;
    auto        containerClient = blobServiceClient.GetBlobContainerClient(containerName);

    Azure::Storage::Blobs::ListBlobsOptions options;
    options.Prefix = prefix;

    std::set<std::string> listBlobs;
    for (auto pageResult = containerClient.ListBlobs(options); pageResult.HasPage(); pageResult.MoveToNextPage()) {
      for (const auto &blob : pageResult.Blobs) {
        listBlobs.insert(blob.Name);
      }
    }

    for (auto blobName : listBlobs) {
      const char *tmp = strchr(blobName.c_str(), '/');
      tmp = (tmp == NULL) ? blobName.c_str() : tmp + 1;
      char fileName[PATH_MAX] = {0};
      if (path[strlen(path) - 1] != TD_DIRSEP_CHAR) {
        (void)snprintf(fileName, PATH_MAX, "%s%s%s", path, TD_DIRSEP, tmp);
      } else {
        (void)snprintf(fileName, PATH_MAX, "%s%s", path, tmp);
      }
      if (!azGetObjectToFile(blobName.c_str(), fileName)) {
        TAOS_RETURN(TSDB_CODE_FAILED);
      }
    }
  } catch (const Azure::Core::RequestFailedException &e) {
    uError("%s failed at line %d since %d(%s)", __func__, __LINE__, static_cast<int>(e.StatusCode),
           e.ReasonPhrase.c_str());
    TAOS_RETURN(TSDB_CODE_FAILED);
  }

  return 0;
}

int32_t azDeleteObjects(const char *object_name[], int nobject) {
  for (int i = 0; i < nobject; ++i) {
    azDeleteObjectsByPrefix(object_name[i]);
  }

  return 0;
}

#else

int32_t azBegin() { return TSDB_CODE_SUCCESS; }

void azEnd() {}

int32_t azCheckCfg() { return TSDB_CODE_SUCCESS; }

int32_t azPutObjectFromFileOffset(const char *file, const char *object_name, int64_t offset, int64_t size) {
  return TSDB_CODE_SUCCESS;
}

int32_t azGetObjectBlock(const char *object_name, int64_t offset, int64_t size, bool check, uint8_t **ppBlock) {
  return TSDB_CODE_SUCCESS;
}

void azDeleteObjectsByPrefix(const char *prefix) {}

int32_t azPutObjectFromFile2(const char *file, const char *object, int8_t withcp) { return 0; }

int32_t azGetObjectsByPrefix(const char *prefix, const char *path) { return 0; }

int32_t azGetObjectToFile(const char *object_name, const char *fileName) { return 0; }

int32_t azDeleteObjects(const char *object_name[], int nobject) { return 0; }

#endif

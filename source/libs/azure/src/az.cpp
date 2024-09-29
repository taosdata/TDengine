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

int32_t azBegin() { return TSDB_CODE_SUCCESS; }

void azEnd() {}

int32_t azCheckCfg() {
  int32_t code = 0, lino = 0;

  // TODO:

  azEnd();

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

int32_t azGetObjectBlock(const char *object_name, int64_t offset, int64_t size, bool check, uint8_t **ppBlock) {
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

    uint8_t *buf = (uint8_t *)taosMemoryCalloc(1, size);
    if (!buf) {
      return terrno;
    }

    Blobs::DownloadBlobToOptions options;
    // options.TransferOptions.Concurrency = concurrency;
    // if (offset.HasValue() || length.HasValue()) {
    options.Range = Azure::Core::Http::HttpRange();
    options.Range.Value().Offset = offset;
    options.Range.Value().Length = size;
    //}
    /*
    if (initialChunkSize.HasValue()) {
      options.TransferOptions.InitialChunkSize = initialChunkSize.Value();
    }
    if (chunkSize.HasValue()) {
      options.TransferOptions.ChunkSize = chunkSize.Value();
    }
    */

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
    TAOS_RETURN(code);
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

void azDeleteObjectsByPrefix(const char *prefix) { return TSDB_CODE_SUCCESS; }

#endif

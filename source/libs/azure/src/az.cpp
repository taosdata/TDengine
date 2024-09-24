#define ALLOW_FORBID_FUNC

#include "az.h"

#include "os.h"
#include "taoserror.h"
#include "tglobal.h"

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

#include <iostream>
/*
static Azure::Response<Models::UploadBlockBlobFromResult> _azUploadFrom(const BlockBlobClient &blobClient,
                                                                        const std::string &fileName, int64_t offset,
                                                                        int64_t size) {
  const UploadBlockBlobFromOptions &options = UploadBlockBlobFromOptions();
  const Azure::Core::Context       &context = Azure::Core::Context();

  _internal::FileReader fileReader(fileName);

  if (size <= options.TransferOptions.SingleUploadThreshold) {
    Azure::Core::IO::_internal::RandomAccessFileBodyStream contentStream(fileReader.GetHandle(), offset, size);

    UploadBlockBlobOptions uploadBlockBlobOptions;
    uploadBlockBlobOptions.HttpHeaders = options.HttpHeaders;
    uploadBlockBlobOptions.Metadata = options.Metadata;
    uploadBlockBlobOptions.Tags = options.Tags;
    uploadBlockBlobOptions.AccessTier = options.AccessTier;
    uploadBlockBlobOptions.ImmutabilityPolicy = options.ImmutabilityPolicy;
    uploadBlockBlobOptions.HasLegalHold = options.HasLegalHold;

    return Upload(contentStream, uploadBlockBlobOptions, context);
  }

  constexpr int64_t DefaultStageBlockSize = 4 * 1024 * 1024ULL;
  constexpr int64_t MaxStageBlockSize = 4000 * 1024 * 1024ULL;
  constexpr int64_t MaxBlockNumber = 50000;
  constexpr int64_t BlockGrainSize = 1 * 1024 * 1024;

  std::vector<std::string> blockIds;
  auto                     getBlockId = [](int64_t id) {
    constexpr size_t BlockIdLength = 64;
    std::string      blockId = std::to_string(id);
    blockId = std::string(BlockIdLength - blockId.length(), '0') + blockId;
    return Azure::Core::Convert::Base64Encode(std::vector<uint8_t>(blockId.begin(), blockId.end()));
  };

  auto uploadBlockFunc = [&](int64_t offset, int64_t length, int64_t chunkId, int64_t numChunks) {
    Azure::Core::IO::_internal::RandomAccessFileBodyStream contentStream(fileReader.GetHandle(), offset, length);
    StageBlockOptions                                      chunkOptions;
    auto blockInfo = StageBlock(getBlockId(chunkId), contentStream, chunkOptions, context);
    if (chunkId == numChunks - 1) {
      blockIds.resize(static_cast<size_t>(numChunks));
    }
  };

  int64_t chunkSize;
  if (options.TransferOptions.ChunkSize.HasValue()) {
    chunkSize = options.TransferOptions.ChunkSize.Value();
  } else {
    int64_t minChunkSize = (fileReader.GetFileSize() + MaxBlockNumber - 1) / MaxBlockNumber;
    minChunkSize = (minChunkSize + BlockGrainSize - 1) / BlockGrainSize * BlockGrainSize;
    chunkSize = (std::max)(DefaultStageBlockSize, minChunkSize);
  }
  if (chunkSize > MaxStageBlockSize) {
    throw Azure::Core::RequestFailedException("Block size is too big.");
  }

  _internal::ConcurrentTransfer(0, fileReader.GetFileSize(), chunkSize, options.TransferOptions.Concurrency,
                                uploadBlockFunc);

  for (size_t i = 0; i < blockIds.size(); ++i) {
    blockIds[i] = getBlockId(static_cast<int64_t>(i));
  }
  CommitBlockListOptions commitBlockListOptions;
  commitBlockListOptions.HttpHeaders = options.HttpHeaders;
  commitBlockListOptions.Metadata = options.Metadata;
  commitBlockListOptions.Tags = options.Tags;
  commitBlockListOptions.AccessTier = options.AccessTier;
  commitBlockListOptions.ImmutabilityPolicy = options.ImmutabilityPolicy;
  commitBlockListOptions.HasLegalHold = options.HasLegalHold;
  auto commitBlockListResponse = blobClient.CommitBlockList(blockIds, commitBlockListOptions, context);

  Models::UploadBlockBlobFromResult result;
  result.ETag = commitBlockListResponse.Value.ETag;
  result.LastModified = commitBlockListResponse.Value.LastModified;
  result.VersionId = commitBlockListResponse.Value.VersionId;
  result.IsServerEncrypted = commitBlockListResponse.Value.IsServerEncrypted;
  result.EncryptionKeySha256 = commitBlockListResponse.Value.EncryptionKeySha256;
  result.EncryptionScope = commitBlockListResponse.Value.EncryptionScope;

  return Azure::Response<Models::UploadBlockBlobFromResult>(std::move(result),
                                                            std::move(commitBlockListResponse.RawResponse));
}
*/
int32_t azPutObjectFromFileOffset(const char *file, const char *object_name, int64_t offset, int64_t size) {
  int32_t code = 0;

  std::string endpointUrl = tsS3Hostname[0];        // GetEndpointUrl();
  std::string accountName = tsS3AccessKeyId[0];     // GetAccountName();
  std::string accountKey = tsS3AccessKeySecret[0];  // GetAccountKey();

  try {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);
    /*
std::string accountURL = tsS3Hostname[0];
StorageSharedKeyCredential *pSharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
std::shared_ptr<StorageSharedKeyCredential> sharedKeyCredential(pSharedKeyCredential);

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
TDBlockBlobClient blobClient(containerClient.GetBlobClient(blobName));

// Upload the blob
// std::cout << "Uploading blob: " << blobName << std::endl;
blobClient.UploadFrom(blobContent, sizeof(blobContent));*/
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
    return 1;
  }

  return code;
}

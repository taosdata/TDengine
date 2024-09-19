#define ALLOW_FORBID_FUNC

#include "az.h"

#include "os.h"
#include "taoserror.h"
#include "tglobal.h"

#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

// Add appropriate using namespace directives
using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;

extern char tsS3Hostname[][TSDB_FQDN_LEN];
extern char tsS3AccessKeyId[][TSDB_FQDN_LEN];
extern char tsS3AccessKeySecret[][TSDB_FQDN_LEN];
extern char tsS3BucketName[TSDB_FQDN_LEN];

#include <iostream>

int32_t azPutObjectFromFileOffset(const char *file, const char *object_name, int64_t offset, int64_t size) {
  int32_t code = 0;

  std::string endpointUrl = tsS3Hostname[0];        // GetEndpointUrl();
  std::string accountName = tsS3AccessKeyId[0];     // GetAccountName();
  std::string accountKey = tsS3AccessKeySecret[0];  // GetAccountKey();

  try {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);

    std::string       accountURL = tsS3Hostname[0];
    BlobServiceClient blobServiceClient(accountURL, sharedKeyCredential);

    std::string containerName = tsS3BucketName;
    auto        containerClient = blobServiceClient.GetBlobContainerClient(containerName);

    // Create the container if it does not exist
    std::cout << "Creating container: " << containerName << std::endl;
    // containerClient.CreateIfNotExists();

    std::string blobName = "blob.txt";
    uint8_t     blobContent[] = "Hello Azure!";
    // Create the block blob client
    BlockBlobClient blobClient = containerClient.GetBlockBlobClient(blobName);

    // Upload the blob
    std::cout << "Uploading blob: " << blobName << std::endl;
    blobClient.UploadFrom(blobContent, sizeof(blobContent));
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
    std::cout << "Status Code: " << static_cast<int>(e.StatusCode) << ", Reason Phrase: " << e.ReasonPhrase
              << std::endl;
    std::cout << e.what() << std::endl;
    return 1;
  }

  return code;
}

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

#include <iostream>

// Include the necessary SDK headers
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

// Add appropriate using namespace directives
using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;

// Secrets should be stored & retrieved from secure locations such as Azure::KeyVault. For
// convenience and brevity of samples, the secrets are retrieved from environment variables.

std::string GetEndpointUrl() {
  // return std::getenv("AZURE_STORAGE_ACCOUNT_URL");
  std::string accountId = getenv("ablob_account_id");
  if (accountId.empty()) {
    return accountId;
  }

  return accountId + ".blob.core.windows.net";
}

std::string GetAccountName() {
  //  return std::getenv("AZURE_STORAGE_ACCOUNT_NAME");
  return getenv("ablob_account_id");
}

std::string GetAccountKey() {
  // return std::getenv("AZURE_STORAGE_ACCOUNT_KEY");

  return getenv("ablob_account_secret");
}

int main() {
  std::string endpointUrl = GetEndpointUrl();
  std::string accountName = GetAccountName();
  std::string accountKey = GetAccountKey();

  try {
    auto sharedKeyCredential = std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);

    std::string       accountURL = "https://fd2d01cd892f844eeaa2273.blob.core.windows.net";
    BlobServiceClient blobServiceClient(accountURL, sharedKeyCredential);

    std::string containerName = "myblobcontainer";
    // auto containerClient = blobServiceClient.GetBlobContainerClient("myblobcontainer");
    auto containerClient = blobServiceClient.GetBlobContainerClient("td-test");

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
  } catch (const Azure::Core::RequestFailedException& e) {
    std::cout << "Status Code: " << static_cast<int>(e.StatusCode) << ", Reason Phrase: " << e.ReasonPhrase
              << std::endl;
    std::cout << e.what() << std::endl;

    return 1;
  }

  return 0;
}

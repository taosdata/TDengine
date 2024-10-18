/**
 * @file
 * @brief Application that consumes the Azure SDK for C++.
 *
 * @remark Set environment variable `STORAGE_CONNECTION_STRING` before running the application.
 *
 */

#include <azure/storage/blobs.hpp>

#include <exception>
#include <iostream>

using namespace Azure::Storage::Blobs;

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  /**************** Container SDK client ************************/
  /****************   Create container  ************************/
  try {
    auto containerClient =
        BlobContainerClient::CreateFromConnectionString(std::getenv("STORAGE_CONNECTION_STRING"), "td-test");

    // Create the container if it does not exist
    // std::cout << "Creating container: " << containerName << std::endl;
    // containerClient.CreateIfNotExists();

    /**************** Container SDK client ************************/
    /****************      list blobs (one page) ******************/
    // auto response = containerClient.ListBlobsSinglePage();
    // auto response = containerClient.ListBlobs();
    // auto blobListPage = response.Value;
    // auto blobListPage = response.Blobs;
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

    for (auto page = containerClient.ListBlobs(/*options*/); page.HasPage(); page.MoveToNextPage()) {
      for (auto& blob : page.Blobs) {
        std::cout << blob.Name << std::endl;
      }
    }

  } catch (const std::exception& ex) {
    std::cout << ex.what();
    return 1;
  }

  return 0;
}

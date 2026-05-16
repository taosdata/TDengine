#pragma once

#include <string>

struct DataChannel {
    std::string channel_type = "native"; // "native", "websocket", "restful", or "file_stream"
};

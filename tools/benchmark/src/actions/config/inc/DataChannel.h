#ifndef DATA_CHANNEL_H
#define DATA_CHANNEL_H

#include <string>

struct DataChannel {
    std::string channel_type = "native"; // "native", "websocket", "restful", or "file_stream"
};

#endif // DATA_CHANNEL_H
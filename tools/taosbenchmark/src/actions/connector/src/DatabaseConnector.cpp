#include <stdexcept>
#include "NativeConnector.h"
#include "WebsocketConnector.h"
#include "RestfulConnector.h"

std::unique_ptr<DatabaseConnector> DatabaseConnector::create(
    const DataChannel& channel,
    const ConnectionInfo& connInfo)
{
    if (channel.channel_type == "native") {
        return std::make_unique<NativeConnector>(connInfo);
    } else if (channel.channel_type == "websocket") {
        return std::make_unique<WebsocketConnector>(connInfo);
    } else if (channel.channel_type == "restful") {
        return std::make_unique<RestfulConnector>(connInfo);
    }

    throw std::invalid_argument("Unsupported channel type: " + channel.channel_type);
}

using TDengine.Driver.Client.Native;
using TDengine.Driver.Client.Websocket;

namespace TDengine.Driver.Client
{
    public static class DbDriver
    {
        public static ITDengineClient Open(ConnectionStringBuilder builder)
        {
            if (builder.Protocol == TDengineConstant.ProtocolWebSocket)
            {
                return new WSClient(builder);
            }
            return new NativeClient(builder);
        }
    }
}
using TDengine.Driver;
using TDengine.Driver.Impl.WebSocketMethods;
using Xunit;

namespace Driver.Test.Driver
{
    public class WebSocketConnectionAvailabilityTests
    {
        [Theory]
        [InlineData((int)TDengineError.InternalErrorCode.WS_CONNECTION_CLOSED, false)]
        [InlineData((int)TDengineError.InternalErrorCode.WS_RECEIVE_CLOSE_FRAME, false)]
        [InlineData((int)TDengineError.InternalErrorCode.WS_WRITE_TIMEOUT, false)]
        [InlineData((int)TDengineError.InternalErrorCode.WS_UNEXPECTED_MESSAGE, false)]
        [InlineData((int)TDengineError.InternalErrorCode.WS_RECONNECT_FAILED, false)]
        [InlineData(0x0001, true)]
        public void IsConnectionAvailableByTdengineErrorShouldClassifyCodes(int code, bool expected)
        {
            var err = new TDengineError(code, "test");
            var actual = BaseConnection.IsConnectionAvailableByTdengineError(err);
            Assert.Equal(expected, actual);
        }
    }
}

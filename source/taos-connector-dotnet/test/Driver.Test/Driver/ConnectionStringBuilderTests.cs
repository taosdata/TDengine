using System;
using System.Linq;
using TDengine.Driver;
using Xunit;

namespace Driver.Test.Driver
{
    public class ConnectionStringBuilderTests
    {
        [Theory]
        [InlineData(-1)]
        [InlineData(65536)]
        public void PortSetterShouldRejectInvalidValues(int port)
        {
            var builder = new ConnectionStringBuilder(string.Empty);
            var ex = Assert.Throws<ArgumentException>(() => builder.Port = port);
            Assert.Equal("port", ex.ParamName);
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(",,")]
        public void GetFailoverAddressesShouldRejectEmptyHost(string host)
        {
            var builder = new ConnectionStringBuilder("protocol=WebSocket;useSSL=false");
            builder.Host = host;

            var ex = Assert.Throws<ArgumentException>(() => builder.GetFailoverAddresses());
            Assert.Equal("host", ex.ParamName);
        }

        [Fact]
        public void GetFailoverAddressesShouldUseWebSocketDefaultPortWhenPortNotSpecified()
        {
            var builder = new ConnectionStringBuilder("protocol=WebSocket;host=localhost;useSSL=false");
            var addresses = builder.GetFailoverAddresses();

            Assert.Single(addresses);
            Assert.Equal("localhost", addresses[0].Host);
            Assert.Equal(6041, addresses[0].Port);
        }

        [Fact]
        public void GetFailoverAddressesShouldUseConfiguredFallbackPort()
        {
            var builder = new ConnectionStringBuilder(
                "protocol=WebSocket;host=first,second:6050;port=6042;useSSL=false");
            var addresses = builder.GetFailoverAddresses().ToArray();

            Assert.Equal(2, addresses.Length);
            Assert.Equal("first", addresses[0].Host);
            Assert.Equal(6042, addresses[0].Port);
            Assert.Equal("second", addresses[1].Host);
            Assert.Equal(6050, addresses[1].Port);
        }

        [Fact]
        public void GetFailoverAddressesShouldDeduplicateByCacheKey()
        {
            var builder = new ConnectionStringBuilder(
                "protocol=WebSocket;host=example.com:6041,example.com,EXAMPLE.com:6041;port=6041;useSSL=false");
            var addresses = builder.GetFailoverAddresses();

            Assert.Single(addresses);
            Assert.Equal("example.com", addresses[0].Host.ToLowerInvariant());
            Assert.Equal(6041, addresses[0].Port);
        }

        [Fact]
        public void GetFailoverAddressesShouldKeepNativePortZeroWhenNoPortSpecified()
        {
            var builder = new ConnectionStringBuilder("protocol=Native;host=localhost;port=0");
            var addresses = builder.GetFailoverAddresses();

            Assert.Single(addresses);
            Assert.Equal("localhost", addresses[0].Host);
            Assert.Equal(0, addresses[0].Port);
        }
    }
}

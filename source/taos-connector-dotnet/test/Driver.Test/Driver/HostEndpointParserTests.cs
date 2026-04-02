using System;
using TDengine.Driver;
using Xunit;

namespace Driver.Test.Driver
{
    public class HostEndpointParserTests
    {
        [Fact]
        public void ParseHostEndpointShouldParseAbsoluteUri()
        {
            HostEndpointParser.ParseHostEndpoint("ws://Example.COM:6050/path?q=1", "host",
                out var host, out var port);

            Assert.Equal("example.com", host.ToLowerInvariant());
            Assert.Equal(6050, port);
        }

        [Fact]
        public void ParseHostEndpointShouldParseBracketedIpv6WithPort()
        {
            HostEndpointParser.ParseHostEndpoint("[2001:db8::1]:6041", "host",
                out var host, out var port);

            Assert.Equal("2001:db8::1", host);
            Assert.Equal(6041, port);
        }

        [Fact]
        public void ParseHostEndpointShouldRejectBareIpv6InMultiHostMode()
        {
            var ex = Assert.Throws<ArgumentException>(() => HostEndpointParser.ParseHostEndpoint(
                "2001:db8::1",
                "host",
                out _,
                out _,
                allowBareIpv6: false));

            Assert.Equal("host", ex.ParamName);
        }

        [Theory]
        [InlineData(":6041")]
        [InlineData(":")]
        public void ParseHostEndpointShouldRejectLeadingColonEndpoint(string endpoint)
        {
            var ex = Assert.Throws<ArgumentException>(() => HostEndpointParser.ParseHostEndpoint(
                endpoint,
                "host",
                out _,
                out _));

            Assert.Equal("host", ex.ParamName);
        }

        [Fact]
        public void ValidateEndpointPortShouldRejectOutOfRange()
        {
            var ex = Assert.Throws<ArgumentException>(() =>
                HostEndpointParser.ValidateEndpointPort(65536, "host"));

            Assert.Equal("host", ex.ParamName);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public void ValidateEndpointPortShouldRejectZeroAndNegative(int port)
        {
            var ex = Assert.Throws<ArgumentException>(() =>
                HostEndpointParser.ValidateEndpointPort(port, "host"));

            Assert.Equal("host", ex.ParamName);
        }

        [Theory]
        [InlineData("localhost:0")]
        [InlineData("127.0.0.1:0")]
        public void ParseHostEndpointShouldRejectExplicitPortZero(string endpoint)
        {
            var ex = Assert.Throws<ArgumentException>(() => HostEndpointParser.ParseHostEndpoint(
                endpoint,
                "host",
                out _,
                out _));

            Assert.Equal("host", ex.ParamName);
        }

        [Fact]
        public void ParseHostEndpointShouldRejectBracketedIpv6WithPortZero()
        {
            var ex = Assert.Throws<ArgumentException>(() => HostEndpointParser.ParseHostEndpoint(
                "[2001:db8::1]:0",
                "host",
                out _,
                out _));

            Assert.Equal("host", ex.ParamName);
        }

        [Fact]
        public void TryParseAbsoluteUriShouldReturnFalseForNonUriInput()
        {
            var parsed = HostEndpointParser.TryParseAbsoluteUri("localhost:6041", "host", out var host, out var port);

            Assert.False(parsed);
            Assert.Null(host);
            Assert.Equal(0, port);
        }

        [Theory]
        [InlineData("WebSocket", false, "Example.COM", 6041, "ws://example.com:6041")]
        [InlineData("WebSocket", true, "2001:db8::1", 443, "wss://[2001:db8::1]:443")]
        [InlineData("Native", false, "LOCALHOST", 6030, "native://localhost:6030")]
        public void BuildFailoverCacheKeyShouldNormalizeHostAndProtocol(string protocol, bool useSSL, string host,
            int port, string expected)
        {
            var actual = HostEndpointParser.BuildFailoverCacheKey(protocol, useSSL, host, port);
            Assert.Equal(expected, actual);
        }
    }
}

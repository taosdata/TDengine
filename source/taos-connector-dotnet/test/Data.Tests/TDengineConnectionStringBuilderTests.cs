using System;
using TDengine.Data.Client;
using TDengine.Driver;
using TDengine.Driver.Client;
using Xunit;
using Xunit.Abstractions;

namespace Data.Tests
{
    public class TDengineConnectionStringBuilderTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public TDengineConnectionStringBuilderTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void DefaultNative_ShouldSetDefaultValues()
        {
            var builder = new TDengineConnectionStringBuilder("");

            builder.DefaultNative();

            Assert.Equal(6030, builder.Port);
            Assert.Equal("localhost", builder.Host);
            Assert.Equal(TDengineConstant.ProtocolNative, builder.Protocol);
        }

        [Fact]
        public void DefaultWebSocket_ShouldSetDefaultValues()
        {
            var builder = new TDengineConnectionStringBuilder("");

            builder.DefaultWebSocket();

            Assert.Equal(6041, builder.Port);
            Assert.Equal("localhost", builder.Host);
            Assert.Equal(TDengineConstant.ProtocolWebSocket, builder.Protocol);
        }

        [Fact]
        public void Parse()
        {
            var builder =
                new TDengineConnectionStringBuilder(
                    "host=127.0.0.1;port=6030;username=root;password=taosdata;protocol=Native;db=test");
            Assert.Equal("127.0.0.1", builder.Host);
            Assert.Equal(6030, builder.Port);
            Assert.Equal("root", builder.Username);
            Assert.Equal("taosdata", builder.Password);
            Assert.Equal("test", builder.Database);
            Assert.Equal(TDengineConstant.ProtocolNative, builder.Protocol);
            builder.Clear();
            Assert.Equal(string.Empty, builder.Host);
            Assert.Equal(0, builder.Port);
            Assert.Equal(string.Empty, builder.Username);
            Assert.Equal(string.Empty, builder.Password);
            Assert.Equal(string.Empty, builder.Database);
            Assert.Equal(TDengineConstant.ProtocolNative, builder.Protocol);
            builder.Database = "test2";
            Assert.Equal("test2", builder.Database);
            builder.Remove("db");
            Assert.Equal(string.Empty, builder.Database);
        }

        [Fact]
        public void ParseWebSocket()
        {
            var builder = new TDengineConnectionStringBuilder(
                "host=127.0.0.1;" +
                "port=6041;" +
                "username=root;" +
                "password=taosdata;" +
                "protocol=WebSocket;" +
                "db=test;" +
                "enableCompression=true;" +
                "connTimeout=00:00:10;" +
                "readTimeout=00:00:20;" +
                "writeTimeout=00:00:30;" +
                "timezone=UTC;" +
                "useSSL=true;" +
                "token=123456;" +
                "autoReconnect=true;" +
                "reconnectIntervalMs=10;" +
                "reconnectRetryCount=5");
            Assert.Equal("127.0.0.1", builder.Host);
            Assert.Equal(6041, builder.Port);
            Assert.Equal("root", builder.Username);
            Assert.Equal("taosdata", builder.Password);
            Assert.Equal("test", builder.Database);
            Assert.Equal(TDengineConstant.ProtocolWebSocket, builder.Protocol);
            Assert.True(builder.EnableCompression);
            Assert.Equal(10, builder.ConnTimeout.TotalSeconds);
            Assert.Equal(20, builder.ReadTimeout.TotalSeconds);
            Assert.Equal(30, builder.WriteTimeout.TotalSeconds);
            Assert.Equal("UTC", builder.Timezone.Id);
            Assert.True(builder.UseSSL);
            Assert.Equal("123456", builder.Token);
            Assert.True(builder.AutoReconnect);
            Assert.Equal(10, builder.ReconnectIntervalMs);
            Assert.Equal(5, builder.ReconnectRetryCount);
        }

        [Fact]
        public void ParseHostList()
        {
            var builder = new TDengineConnectionStringBuilder(
                "host=127.0.0.1:6030,127.0.0.2:6031;protocol=WebSocket;username=root;password=taosdata");
            Assert.Equal("127.0.0.1:6030,127.0.0.2:6031", builder.Host);
        }

        [Fact]
        public void NativeMultiHostShouldThrowWhenOpen()
        {
            var builder = new TDengineConnectionStringBuilder(
                "host=127.0.0.1:6030,127.0.0.2:6031;protocol=Native;username=root;password=taosdata");
            Assert.Throws<ArgumentException>(() =>
            {
                using (var client = DbDriver.Open(builder))
                {
                }
            });
        }

        [Fact]
        public void NativeDuplicateHostShouldThrowWhenOpen()
        {
            var builder = new TDengineConnectionStringBuilder(
                "host=127.0.0.1:6030,127.0.0.1:6030;protocol=Native;username=root;password=taosdata");
            Assert.Throws<ArgumentException>(() =>
            {
                using (var client = DbDriver.Open(builder))
                {
                }
            });
        }

        [Fact]
        public void ParseInvalidPort()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                var builder = new TDengineConnectionStringBuilder("host=127.0.0.1;port=70000");
            });
        }

        [Fact]
        public void ParseTimezone()
        {
            _testOutputHelper.WriteLine($"{Environment.Version.Major}");
            if (Environment.Version.Major >= 6)
            {
                var builder =
                    new TDengineConnectionStringBuilder(
                        "host=127.0.0.1;port=6030;username=root;password=taosdata;protocol=Native;db=test;connectionTimezone=UTC");
                Assert.Equal("127.0.0.1", builder.Host);
                Assert.Equal(6030, builder.Port);
                Assert.Equal("root", builder.Username);
                Assert.Equal("taosdata", builder.Password);
                Assert.Equal("test", builder.Database);
                Assert.Equal("UTC", builder.ConnectionTimezone.Id);
                Assert.Equal(TDengineConstant.ProtocolNative, builder.Protocol);
                Assert.Throws<TimeZoneNotFoundException>(() =>
                {
                    var invalidBuilder = new TDengineConnectionStringBuilder("connectionTimezone=InvalidTimezone");
                });
                // Test for both timezone and connectionTimezone being set
                Assert.Throws<ArgumentException>(() =>
                {
                    var invalidBuilder = new TDengineConnectionStringBuilder("timezone=UTC;connectionTimezone=UTC");
                });
                // .NET 6+ supports IANA
                builder = new TDengineConnectionStringBuilder("connectionTimezone=Europe/Paris");
                Assert.Equal("Europe/Paris", builder.ConnectionTimezone.Id);
            }
            else
            {
                if (Environment.OSVersion.Platform == PlatformID.Win32NT)
                {
                    Assert.Throws<TimeZoneNotFoundException>(() =>
                    {
                        var invalidBuilder = new TDengineConnectionStringBuilder("connectionTimezone=Europe/Paris");
                    });
                }

                Assert.Throws<ArgumentException>(() =>
                {
                    var invalidBuilder = new TDengineConnectionStringBuilder("connectionTimezone=UTC");
                });
            }
        }

        [Fact]
        public void BearerTokenTest()
        {
            var builder =
                new TDengineConnectionStringBuilder("host=127.0.0.1;bearerToken=abcdef;protocol=Native;db=test");
            Assert.Equal("127.0.0.1", builder.Host);
            Assert.Equal("abcdef", builder.BearerToken);
        }

        [Theory]
        [InlineData("host=  ;protocol=WebSocket;username=root;password=taosdata")]
        [InlineData("host=,,;protocol=WebSocket;username=root;password=taosdata")]
        [InlineData("host=,;protocol=WebSocket;username=root;password=taosdata")]
        public void WebSocketEmptyHostShouldThrowWhenOpen(string connectionString)
        {
            var builder = new TDengineConnectionStringBuilder(connectionString);
            Assert.Throws<ArgumentException>(() =>
            {
                using (var client = DbDriver.Open(builder))
                {
                }
            });
        }

        [Theory]
        [InlineData("host=;protocol=Native;username=root;password=taosdata")]
        [InlineData("host= ;protocol=Native;username=root;password=taosdata")]
        public void NativeEmptyHostShouldNotThrowArgumentExceptionWhenOpen(string connectionString)
        {
            var builder = new TDengineConnectionStringBuilder(connectionString);
            var ex = Record.Exception(() =>
            {
                using (var client = DbDriver.Open(builder))
                {
                }
            });

            Assert.False(ex is ArgumentException, ex?.ToString());
        }

        [Fact]
        public void NullNativeHostShouldNotThrowArgumentExceptionWhenOpen()
        {
            var builder = new TDengineConnectionStringBuilder("protocol=Native;username=root;password=taosdata");
            builder.Host = null;
            var ex = Record.Exception(() =>
            {
                using (var client = DbDriver.Open(builder))
                {
                }
            });

            Assert.False(ex is ArgumentException, ex?.ToString());
        }
    }
}

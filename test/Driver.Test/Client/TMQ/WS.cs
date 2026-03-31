using System;
using System.Collections.Generic;
using TDengine.Driver.Impl.WebSocketMethods;
using Xunit;

namespace Driver.Test.Client.TMQ
{
    public partial class Consumer
    {
        [Fact]
        public void WSConsumerTest()
        {
            var db = "ws_tmq_consumer_test";
            var topic = "ws_tmq_consumer_test_topic";
            this.NewConsumerTest(this._wsConnectString, db, topic, this._wsTMQCfg);
        }

        [Fact]
        public void WSConsumerSeekTest()
        {
            var db = "ws_tmq_seek_test";
            var topic = "ws_tmq_seek_test_topic";
            this.ConsumerSeekTest(this._wsConnectString, db, topic, this._wsTMQCfg);
        }

        [Fact]
        public void WSConsumerCommitTest()
        {
            var db = "ws_tmq_commit_test";
            var topic = "ws_tmq_commit_test_topic";
            this.ConsumerCommitTest(this._wsConnectString, db, topic, this._wsTMQCfg);
        }

        [Fact]
        public void WSAutoCommitTest()
        {
            var db = "ws_tmq_auto_commit_test";
            var topic = "ws_tmq_auto_commit_test_topic";
            this.ConsumerAutoCommitTest(this._wsConnectString, db, topic, this._wsTMQCfgAutoCommit);
        }

        [Fact]
        public void WSConsumerMultiPollTest()
        {
            var db = "ws_tmq_multi_poll_test";
            var topic = "ws_tmq_multi_poll_test_topic";
            var cfg = new Dictionary<string, string>(this._wsTMQCfg)
            {
                ["auto.offset.reset"] = "latest"
            };
            this.ConsumerMultiPollTest(this._wsConnectString, db, topic, cfg);
        }

        [Theory]
        // Test SSL and non-SSL cases
        [InlineData("false", "", "localhost", "", "ws://localhost:6041/rest/tmq")]
        [InlineData("true", "", "example.com", "xyz", "wss://example.com:443/rest/tmq?token=xyz")]

        // Test custom ports
        [InlineData("false", "8080", "127.0.0.1", "abc", "ws://127.0.0.1:8080/rest/tmq?token=abc")]
        [InlineData("true", "8443", "api.test", "", "wss://api.test:8443/rest/tmq")]

        // Test IPv6 addresses (bare and bracketed)
        [InlineData("false", "", "2001:db8::1", "a&b", "ws://[2001:db8::1]:6041/rest/tmq?token=a&b")]
        [InlineData("true", "443", "2001:db8::1", "", "wss://[2001:db8::1]:443/rest/tmq")]
        [InlineData("false", "", "[2001:db8::1]", "a&b", "ws://[2001:db8::1]:6041/rest/tmq?token=a&b")]
        [InlineData("true", "443", "[2001:db8::1]", "", "wss://[2001:db8::1]:443/rest/tmq")]
        [InlineData("false", "6049", "[2001:db8::1]:6049", "", "ws://[2001:db8::1]:6049/rest/tmq")]

        // Test edge cases
        [InlineData("false", "6041", "localhost", null, "ws://localhost:6041/rest/tmq")]
        [InlineData("true", "443", "localhost", " ", "wss://localhost:443/rest/tmq?token= ")]

        // Test multi-host failover (should use first address)
        [InlineData("false", "", "[2001:db8::1],localhost:6041", "", "ws://[2001:db8::1]:6041/rest/tmq")]
        [InlineData("false", "", "[2001:db8::1]:6049,localhost:6041", "", "ws://[2001:db8::1]:6049/rest/tmq")]
        [InlineData("false", "", "localhost:6042,localhost:6043", "", "ws://localhost:6042/rest/tmq")]
        [InlineData("true", "", "[2001:db8::1],localhost", "", "wss://[2001:db8::1]:443/rest/tmq")]
        public void GetUrl_ShouldReturnCorrectUrl(string useSsl, string port, string host, string token,
            string expectedUrl)
        {
            // Arrange
            var cfg = new Dictionary<string, string>()
            {
                { "useSSL", useSsl },
                { "td.connect.port", port },
                { "td.connect.ip", host },
                { "token", token }
            };

            var options = new TMQOptions(cfg);
            // Act
            string actualUrl = TMQConnection.GetUrl(options);

            // Assert
            Assert.Equal(expectedUrl, actualUrl);
        }

        [Fact]
        public void GetUrl_ShouldHandleNullBuilder()
        {
            // Arrange
            TMQOptions options = null;

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => TMQConnection.GetUrl(options));
        }

        [Fact]
        public void GetUrl_ShouldThrowArgumentException_WhenTdConnectIpIsEmpty()
        {
            var cfg = new Dictionary<string, string>
            {
                { "useSSL", "false" },
                { "td.connect.ip", string.Empty }
            };

            var options = new TMQOptions(cfg);
            var ex = Assert.Throws<ArgumentException>(() => TMQConnection.GetUrl(options));
            Assert.Equal("td.connect.ip", ex.ParamName);
        }

        [Theory]
        [InlineData("2001:db8::1,localhost:6041")]
        [InlineData("2001:db8::1:6049,localhost:6041")]
        public void GetUrl_InvalidMultiHostBareIpv6ShouldThrowArgumentException(string host)
        {
            var cfg = new Dictionary<string, string>
            {
                { "useSSL", "false" },
                { "td.connect.ip", host }
            };

            var options = new TMQOptions(cfg);
            Assert.Throws<ArgumentException>(() => TMQConnection.GetUrl(options));
        }

        [Theory]
        [InlineData(",,")]
        [InlineData(",")]
        public void GetUrl_OnlyDelimiterHostShouldThrowArgumentException(string host)
        {
            var cfg = new Dictionary<string, string>
            {
                { "useSSL", "false" },
                { "td.connect.ip", host }
            };

            var options = new TMQOptions(cfg);
            Assert.Throws<ArgumentException>(() => TMQConnection.GetUrl(options));
        }

        [Theory]
        [InlineData(":6041")]
        [InlineData(":")]
        public void GetUrl_LeadingColonHostShouldThrowArgumentException(string host)
        {
            var cfg = new Dictionary<string, string>
            {
                { "useSSL", "false" },
                { "td.connect.ip", host }
            };

            var options = new TMQOptions(cfg);
            Assert.Throws<ArgumentException>(() => TMQConnection.GetUrl(options));
        }

        [Theory]
        [InlineData("localhost:0")]
        [InlineData("localhost:0,localhost:6041")]
        [InlineData("[2001:db8::1]:0")]
        public void GetUrl_ExplicitPortZeroShouldThrowArgumentException(string host)
        {
            var cfg = new Dictionary<string, string>
            {
                { "useSSL", "false" },
                { "td.connect.ip", host }
            };

            var options = new TMQOptions(cfg);
            Assert.Throws<ArgumentException>(() => TMQConnection.GetUrl(options));
        }

        [Theory]
        [InlineData("abc")]
        [InlineData("-1")]
        [InlineData("65536")]
        public void GetUrl_InvalidTdConnectPortShouldThrowArgumentException(string port)
        {
            var cfg = new Dictionary<string, string>
            {
                { "useSSL", "false" },
                { "td.connect.ip", "localhost" },
                { "td.connect.port", port }
            };

            var options = new TMQOptions(cfg);
            var ex = Assert.Throws<ArgumentException>(() => TMQConnection.GetUrl(options));
            Assert.Equal("td.connect.port", ex.ParamName);
        }

        [Fact]
        public void GetUrl_EndpointPortShouldTakePrecedenceOverInvalidTdConnectPort()
        {
            var cfg = new Dictionary<string, string>
            {
                { "useSSL", "false" },
                { "td.connect.ip", "localhost:6050" },
                { "td.connect.port", "invalid-port" }
            };

            var options = new TMQOptions(cfg);
            var url = TMQConnection.GetUrl(options);
            Assert.Equal("ws://localhost:6050/rest/tmq", url);
        }

        [Fact]
        public void WSConsumerTimezoneTest()
        {
            var db = "ws_tmq_timezone_test";
            var topic = "ws_tmq_timezone_test_topic";
            var tz = "Europe/Paris";
            this.ConsumerTimezoneTest(this._wsConnectString, db, topic, tz, this._wsTMQCfg);
        }

        [Fact]
        public void WSResultTest()
        {
            var db = "ws_tmq_result_test";
            var topic = "ws_tmq_result_test_topic";
            this.ResultTest(this._wsConnectString, db, topic, this._wsTMQCfg);
        }
        
        [Fact]
        public void WSConsumerConfigTest()
        {
            var db = "ws_tmq_consumer_config_test";
            var topic = "ws_tmq_consumer_config_test_topic";
            this.ConsumerConfigTest(this._wsConnectString, db, topic, this._wsConsumerConfig);
        }
    }
}

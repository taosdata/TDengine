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

        // Test IPv6 addresses
        [InlineData("false", "", "2001:db8::1", "a&b", "ws://[2001:db8::1]:6041/rest/tmq?token=a&b")]
        [InlineData("true", "443", "2001:db8::1", "", "wss://[2001:db8::1]:443/rest/tmq")]

        // Test edge cases
        [InlineData("false", "6041", "localhost", null, "ws://localhost:6041/rest/tmq")]
        [InlineData("true", "443", "localhost", " ", "wss://localhost:443/rest/tmq?token= ")]
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
            Assert.Throws<NullReferenceException>(() => TMQConnection.GetUrl(options));
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
    }
}
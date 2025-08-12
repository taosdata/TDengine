using System.Collections.Generic;
using Xunit;

namespace Driver.Test.Client.TMQ
{
    public partial class Consumer
    {
        [Fact]
        public void NativeConsumerTest()
        {
            var db = "tmq_consumer_test";
            var topic = "tmq_consumer_test_topic";
            this.NewConsumerTest(this._nativeConnectString, db, topic, this._nativeTMQCfg);
        }

        [Fact]
        public void NativeConsumerSeekTest()
        {
            var db = "tmq_seek_test";
            var topic = "tmq_seek_test_topic";
            this.ConsumerSeekTest(this._nativeConnectString, db, topic, this._nativeTMQCfg);
        }

        [Fact]
        public void NativeConsumerCommitTest()
        {
            var db = "tmq_commit_test";
            var topic = "tmq_commit_test_topic";
            this.ConsumerCommitTest(this._nativeConnectString, db, topic, this._nativeTMQCfg);
        }

        [Fact]
        public void NativeAutoCommitTest()
        {
            var db = "tmq_auto_commit_test";
            var topic = "tmq_auto_commit_test_topic";
            this.ConsumerAutoCommitTest(this._nativeConnectString, db, topic, this._nativeTMQCfgAutoCommit);
        }

        [Fact]
        public void NativeConsumerMultiPollTest()
        {
            var db = "tmq_multi_poll_test";
            var topic = "tmq_multi_poll_test_topic";
            var cfg = new Dictionary<string, string>(this._nativeTMQCfgAutoCommit)
            {
                ["auto.offset.reset"] = "latest"
            };
            this.ConsumerMultiPollTest(this._nativeConnectString, db, topic, cfg);
        }
        [Fact]
        public void NativeConsumerTimezoneTest()
        {
            var db = "tmq_timezone_test";
            var topic = "tmq_timezone_test_topic";
            var tz = "Europe/Paris";
            this.ConsumerTimezoneTest(this._nativeConnectString, db, topic, tz, this._nativeTMQCfg);
        }

        [Fact]
        public void NativeResultTest()
        {
            var db = "tmq_result_test";
            var topic = "tmq_result_test_topic";
            this.ResultTest(this._nativeConnectString, db, topic, this._nativeTMQCfg);
        }
    }
}
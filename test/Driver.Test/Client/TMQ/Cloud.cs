using System;
using System.Collections.Generic;
using TDengine.Driver.Impl.WebSocketMethods;
using Xunit;

namespace Driver.Test.Client.TMQ
{
    public partial class Consumer
    {
        [Fact]
        public void CloudConsumerTest()
        {
            var db = "cs_test";
            var topic = "cs_tmq_test_decimal_topic";
            if (string.IsNullOrEmpty(this._cloudConnectString) || this._cloudTMQCfg == null)
            {
                _output.WriteLine("Cloud connection string is not set. Skipping CloudConsumerTest.");
                return;
            }

            this.NewConsumerTest(this._cloudConnectString, db, topic, this._cloudTMQCfg);
        }
    }
}
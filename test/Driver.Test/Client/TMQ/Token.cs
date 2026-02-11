using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TDengine.TMQ;
using Xunit;
using Xunit.Sdk;

namespace Driver.Test.Client.TMQ
{
    public class EnterpriseInlineDataAttribute : DataAttribute
    {
        private readonly object[] _data;

        public EnterpriseInlineDataAttribute(params object[] data)
        {
            _data = data;
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return new[] { _data };
        }
        
        public override string Skip => !Consumer.IsEnterpriseTest ? "Enterprise edition is required for token-based authentication. Skipping." : string.Empty;
    }


    
    public partial class Consumer
    {
        private Dictionary<string, string> GetTokenConfigFromType(string type)
        {
            switch (type)
            {
                case "native":
                    return this._nativeBearerTokenCfg;
                case "ws":
                    return this._wsBearerTokenCfg;
                default:
                    throw new System.ArgumentException($"Unsupported connect type: {type}");
            }
        }

        private ConsumerConfig GetConsumerConfigFromType(string type)
        {
            switch (type)
            {
                case "native":
                    return this._nativeBearerTokenConsumerConfig;
                case "ws":
                    return this._wsBearerTokenConsumerConfig;
                default:
                    throw new System.ArgumentException($"Unsupported connect type: {type}");
            }
        }
        
        private string GetConnectStringFromType(string type)
        {
            if (type == "native")
            {
                return this._nativeConnectString;
            }

            if (type == "ws")
            {
                return this._wsConnectString;
            }
            throw new System.ArgumentException($"Unsupported connect type: {type}");
        }
        
        [Theory]
        [EnterpriseInlineDataAttribute("native","token_tmq_consumer_test","token_tmq_consumer_test_topic")]
        [EnterpriseInlineDataAttribute("ws","token_ws_tmq_consumer_test","token_ws_tmq_consumer_test_topic")]
        public void TokenConsumerTest(string connectType, string db, string topic)
        {
            this.NewConsumerTest(GetConnectStringFromType(connectType), db, topic, GetTokenConfigFromType(connectType));
        }

        [Theory]
        [EnterpriseInlineDataAttribute("native","token_tmq_seek_test","token_tmq_seek_test_topic")]
        [EnterpriseInlineDataAttribute("ws","token_ws_tmq_seek_test","token_ws_tmq_seek_test_topic")]
        public void TokenConsumerSeekTest(string connectType, string db, string topic)
        {
            this.ConsumerSeekTest(GetConnectStringFromType(connectType), db, topic, GetTokenConfigFromType(connectType));
        }
        

        [Theory]
        [EnterpriseInlineDataAttribute("native","token_tmq_commit_test","token_tmq_commit_test_topic")]
        [EnterpriseInlineDataAttribute("ws","token_ws_tmq_commit_test","token_ws_tmq_commit_test_topic")]
        public void TokenConsumerCommitTest(string connectType, string db, string topic)
        {
            this.ConsumerCommitTest(GetConnectStringFromType(connectType), db, topic, GetTokenConfigFromType(connectType));
        }

        [Theory]
        [EnterpriseInlineDataAttribute("native","token_tmq_auto_commit_test","token_tmq_auto_commit_test_topic")]
        [EnterpriseInlineDataAttribute("ws","token_ws_tmq_auto_commit_test","token_ws_tmq_auto_commit_test_topic")]
        public void TokenAutoCommitTest(string connectType, string db, string topic)
        {
            var cfg = new Dictionary<string, string>(GetTokenConfigFromType(connectType))
            {
                ["auto.offset.reset"] = "earliest",
                ["enable.auto.commit"] = "true",
                ["auto.commit.interval.ms"] = "100",
            };
            this.ConsumerAutoCommitTest(GetConnectStringFromType(connectType), db, topic, cfg);
        }

        [Theory]
        [EnterpriseInlineDataAttribute("native","token_tmq_multi_poll_test","token_tmq_multi_poll_test_topic")]
        [EnterpriseInlineDataAttribute("ws","token_ws_tmq_multi_poll_test","token_ws_tmq_multi_poll_test_topic")]
        public void TokenConsumerMultiPollTest(string connectType, string db, string topic)
        {
            var cfg = new Dictionary<string, string>(GetTokenConfigFromType(connectType))
            {
                ["auto.offset.reset"] = "latest"
            };
            this.ConsumerMultiPollTest(GetConnectStringFromType(connectType), db, topic, cfg);
        }

        [Theory]
        [EnterpriseInlineDataAttribute("native","token_tmq_timezone_test","token_tmq_timezone_test_topic","Europe/Paris")]
        [EnterpriseInlineDataAttribute("ws","token_ws_tmq_timezone_test","token_ws_tmq_timezone_test_topic","Europe/Paris")]
        public void TokenConsumerTimezoneTest(string connectType, string db, string topic, string tz)
        {
            this.ConsumerTimezoneTest(GetConnectStringFromType(connectType), db, topic, tz, GetTokenConfigFromType(connectType));
        }

        [Theory]
        [EnterpriseInlineDataAttribute("native","token_tmq_result_test","token_tmq_result_test_topic")]
        [EnterpriseInlineDataAttribute("ws","token_ws_tmq_result_test","token_ws_tmq_result_test_topic")]
        public void TokenResultTest(string connectType, string db, string topic)
        {
            this.ResultTest(GetConnectStringFromType(connectType), db, topic, GetTokenConfigFromType(connectType));
        }
        
        [Theory]
        [EnterpriseInlineDataAttribute("native","token_tmq_consumer_config_test","token_tmq_consumer_config_test_topic")]
        [EnterpriseInlineDataAttribute("ws","token_ws_tmq_consumer_config_test","token_ws_tmq_consumer_config_test_topic")]
        public void TokenConsumerConfigTest(string connectType, string db, string topic)
        {
            this.ConsumerConfigTest(GetConnectStringFromType(connectType), db, topic, GetConsumerConfigFromType(connectType));
        }
    }
}
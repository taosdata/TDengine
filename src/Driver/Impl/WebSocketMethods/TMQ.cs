using System;
using System.Collections.Generic;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.Driver.Impl.WebSocketMethods
{
    public class TMQConnection : BaseConnection
    {
        public TMQConnection(TMQOptions options, TimeSpan connectTimeout = default,
            TimeSpan readTimeout = default, TimeSpan writeTimeout = default) : base(
            GetUrl(options), connectTimeout, readTimeout,
            writeTimeout, options.TDEnableCompression == "true")
        {
        }

        private static string GetUrl(TMQOptions options)
        {
            var schema = "ws";
            var port = options.TDConnectPort;
            if (options.TDUseSSL == "true")
            {
                schema = "wss";
                if (string.IsNullOrEmpty(options.TDConnectPort))
                {
                    port = "443";
                }
            }
            else
            {
                if (string.IsNullOrEmpty(options.TDConnectPort))
                {
                    port = "6041";
                }
            }

            if (string.IsNullOrEmpty(options.TDToken))
            {
                return $"{schema}://{options.TDConnectIp}:{port}/rest/tmq";
            }
            else
            {
                return $"{schema}://{options.TDConnectIp}:{port}/rest/tmq?token={options.TDToken}";
            }
        }

        public WSTMQSubscribeResp Subscribe(List<string> topics, TMQOptions options)
        {
            return Subscribe(_GetReqId(), topics, options);
        }

        public WSTMQSubscribeResp Subscribe(ulong reqId, List<string> topics, TMQOptions options)
        {
            return SendJsonBackJson<WSTMQSubscribeReq, WSTMQSubscribeResp>(WSTMQAction.TMQSubscribe,
                new WSTMQSubscribeReq
                {
                    ReqId = reqId,
                    User = options.TDConnectUser,
                    Password = options.TDConnectPasswd,
                    Db = options.TDDatabase,
                    GroupId = options.GroupId,
                    ClientId = options.ClientId,
                    OffsetRest = options.AutoOffsetReset,
                    Topics = topics,
                    AutoCommit = "false",
                    AutoCommitIntervalMs = options.AutoCommitIntervalMs,
                    WithTableName = options.MsgWithTableName,
                    SessionTimeoutMs = options.SessionTimeoutMs,
                    MaxPollIntervalMs = options.MaxPollIntervalMs,
                    Config = options.GetOtherProperties()
                }, reqId);
        }

        public WSTMQPollResp Poll(long blockingTime)
        {
            return Poll(_GetReqId(), blockingTime);
        }

        public WSTMQPollResp Poll(ulong reqId, long blockingTime)
        {
            return SendJsonBackJson<WSTMQPollReq, WSTMQPollResp>(WSTMQAction.TMQPoll, new WSTMQPollReq
            {
                ReqId = reqId,
                BlockingTime = blockingTime
            }, reqId);
        }

        public byte[] FetchBlock(ulong reqId, ulong messageId)
        {
            return SendJsonBackBytes(WSTMQAction.TMQFetchBlock, new WSTMQFetchBlockReq
            {
                ReqId = reqId,
                MessageId = messageId
            }, reqId);
        }

        public byte[] FetchRawBlock(ulong messageId)
        {
            return FetchRawBlock(_GetReqId(), messageId);
        }

        public byte[] FetchRawBlock(ulong reqId, ulong messageId)
        {
            return SendJsonBackBytes(WSTMQAction.TMQFetchRaw, new WSTMQFetchBlockReq
            {
                ReqId = reqId,
                MessageId = messageId
            }, reqId);
        }

        public WSTMQCommitResp Commit()
        {
            return Commit(_GetReqId());
        }

        public WSTMQCommitResp Commit(ulong reqId)
        {
            return SendJsonBackJson<WSTMQCommitReq, WSTMQCommitResp>(WSTMQAction.TMQCommit, new WSTMQCommitReq
            {
                ReqId = reqId,
            }, reqId);
        }

        public WSTMQUnsubscribeResp Unsubscribe()
        {
            return Unsubscribe(_GetReqId());
        }

        public WSTMQUnsubscribeResp Unsubscribe(ulong reqId)
        {
            return SendJsonBackJson<WSTMQUnsubscribeReq, WSTMQUnsubscribeResp>(WSTMQAction.TMQUnsubscribe,
                new WSTMQUnsubscribeReq
                {
                    ReqId = reqId
                }, reqId);
        }

        public WSTMQGetTopicAssignmentResp Assignment(string topic)
        {
            return Assignment(_GetReqId(), topic);
        }

        public WSTMQGetTopicAssignmentResp Assignment(ulong reqId, string topic)
        {
            return SendJsonBackJson<WSTMQGetTopicAssignmentReq, WSTMQGetTopicAssignmentResp>(
                WSTMQAction.TMQGetTopicAssignment, new WSTMQGetTopicAssignmentReq
                {
                    ReqId = reqId,
                    Topic = topic
                }, reqId);
        }

        public WSTMQOffsetSeekResp Seek(string topic, int vgroupId, long offset)
        {
            return Seek(_GetReqId(), topic, vgroupId, offset);
        }

        public WSTMQOffsetSeekResp Seek(ulong reqId, string topic, int vgroupId, long offset)
        {
            return SendJsonBackJson<WSTMQOffsetSeekReq, WSTMQOffsetSeekResp>(WSTMQAction.TMQSeek,
                new WSTMQOffsetSeekReq
                {
                    ReqId = reqId,
                    Topic = topic,
                    VGroupId = vgroupId,
                    Offset = offset
                }, reqId);
        }

        public WSTMQCommitOffsetResp CommitOffset(string topic, int vgroupId, long offset)
        {
            return CommitOffset(_GetReqId(), topic, vgroupId, offset);
        }

        public WSTMQCommitOffsetResp CommitOffset(ulong reqId, string topic, int vgroupId, long offset)
        {
            return SendJsonBackJson<WSTMQCommitOffsetReq, WSTMQCommitOffsetResp>(WSTMQAction.TMQCommitOffset,
                new WSTMQCommitOffsetReq
                {
                    ReqId = reqId,
                    Topic = topic,
                    VGroupId = vgroupId,
                    Offset = offset
                }, reqId);
        }

        public WSTMQCommittedResp Committed(List<WSTopicVgroupId> tvIds)
        {
            return Committed(_GetReqId(), tvIds);
        }

        public WSTMQCommittedResp Committed(ulong reqId, List<WSTopicVgroupId> tvIds)
        {
            return SendJsonBackJson<WSTMQCommittedReq, WSTMQCommittedResp>(WSTMQAction.TMQCommitted,
                new WSTMQCommittedReq
                {
                    ReqId = reqId,
                    TopicVgroupIds = tvIds,
                }, reqId);
        }

        public WSTMQPositionResp Position(List<WSTopicVgroupId> tvIds)
        {
            return Position(_GetReqId(), tvIds);
        }

        public WSTMQPositionResp Position(ulong reqId, List<WSTopicVgroupId> tvIds)
        {
            return SendJsonBackJson<WSTMQPositionReq, WSTMQPositionResp>(WSTMQAction.TMQPosition,
                new WSTMQPositionReq
                {
                    ReqId = reqId,
                    TopicVgroupIds = tvIds,
                }, reqId);
        }

        public WSTMQListTopicsResp Subscription()
        {
            return Subscription(_GetReqId());
        }

        public WSTMQListTopicsResp Subscription(ulong reqId)
        {
            return SendJsonBackJson<WSTMQListTopicsReq, WSTMQListTopicsResp>(WSTMQAction.TMQListTopics,
                new WSTMQListTopicsReq
                {
                    ReqId = reqId
                }, reqId);
        }
    }

    public class TMQOptions
    {
        protected IDictionary<string, string> properties;
        public string GroupId => Get("group.id");

        public string ClientId => Get("client.id");

        public string EnableAutoCommit => Get("enable.auto.commit");

        public string AutoCommitIntervalMs => Get("auto.commit.interval.ms");

        public string AutoOffsetReset => Get("auto.offset.reset");

        public string MsgWithTableName => Get("msg.with.table.name");

        public string TDConnectIp => Get("td.connect.ip");

        public string TDUseSSL => Get("useSSL");

        public string TDToken => Get("token");

        public string TDEnableCompression => Get("ws.message.enableCompression");

        public string TDConnectUser => Get("td.connect.user");

        public string TDConnectPasswd => Get("td.connect.pass");

        public string TDConnectPort => Get("td.connect.port");

        public string TDDatabase => Get("td.connect.db");

        public string TDConnectType => Get("td.connect.type");

        public string TDReconnect => Get("ws.autoReconnect");

        public string TDReconnectRetryCount => Get("ws.reconnect.retry.count");

        public string TDReconnectIntervalMs => Get("ws.reconnect.interval.ms");

        public string SessionTimeoutMs => Get("session.timeout.ms");

        public string MaxPollIntervalMs => Get("max.poll.interval.ms");

        public TMQOptions(IEnumerable<KeyValuePair<string, string>> config)
        {
            this.properties = new Dictionary<string, string>();

            foreach (var kv in config)
            {
                this.properties[kv.Key] = kv.Value;
            }
        }

        public string Get(string key)
        {
            if (this.properties.TryGetValue(key, out var value))
            {
                return value;
            }

            return string.Empty;
        }

        private Dictionary<string, bool> knownProperties = new Dictionary<string, bool>()
        {
            { "group.id", true },
            { "client.id", true },
            { "enable.auto.commit", true },
            { "auto.commit.interval.ms", true },
            { "auto.offset.reset", true },
            { "msg.with.table.name", true },
            { "td.connect.ip", true },
            { "useSSL", true },
            { "token", true },
            { "ws.message.enableCompression", true },
            { "td.connect.user", true },
            { "td.connect.pass", true },
            { "td.connect.port", true },
            { "td.connect.db", true },
            { "td.connect.type", true },
            { "ws.autoReconnect", true },
            { "ws.reconnect.retry.count", true },
            { "ws.reconnect.interval.ms", true },
            { "session.timeout.ms", true },
            { "max.poll.interval.ms", true },
        };

        public Dictionary<string, string> GetOtherProperties()
        {
            var otherProperties = new Dictionary<string, string>();
            foreach (var property in properties)
            {
                if (!knownProperties.ContainsKey(property.Key))
                {
                    otherProperties[property.Key] = property.Value;
                }
            }

            return otherProperties;
        }
    }
}
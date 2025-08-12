using System;
using System.Collections.Generic;
using TDengine.Driver;
using TDengine.Driver.Impl.WebSocketMethods;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;
using TDengineHelper;

namespace TDengine.TMQ.WebSocket
{
    public class Consumer<TValue> : IConsumer<TValue>
    {
        private readonly TMQOptions _options;
        private TMQConnection _connection;
        private readonly bool _autoCommit;
        private readonly int _autoCommitInterval;
        private DateTime _nextCommitTime;
        private readonly bool _reconnect;
        private readonly int _reconnectRetryCount;
        private readonly int _reconnectRetryIntervalMs;
        private List<string> _topics;
        private ulong _lastMessageId = 0;
        private TimeZoneInfo _tz = TimeZoneInfo.Local;

        private IDeserializer<TValue> valueDeserializer;

        private Dictionary<Type, object> defaultDeserializers = new Dictionary<Type, object>
        {
            { typeof(Dictionary<string, object>), DictionaryDeserializer.Dictionary },
        };

        private readonly object _reconnectLock = new object();

        public Consumer(ConsumerBuilder<TValue> builder)
        {
            _options = new TMQOptions(builder.Config);
            if (builder.ValueDeserializer == null)
            {
                if (!defaultDeserializers.TryGetValue(typeof(TValue), out object deserializer))
                {
                    throw new InvalidOperationException(
                        $"Value deserializer was not specified and there is no default deserializer defined for type {typeof(TValue).Name}.");
                }

                this.valueDeserializer = (IDeserializer<TValue>)deserializer;
            }
            else
            {
                this.valueDeserializer = builder.ValueDeserializer;
            }

            if (_options.EnableAutoCommit == "true")
            {
                _autoCommit = true;
                if (!string.IsNullOrEmpty(_options.AutoCommitIntervalMs))
                {
                    if (!int.TryParse(_options.AutoCommitIntervalMs, out _autoCommitInterval))
                        throw new ArgumentException($"Invalid auto commit interval {_options.AutoCommitIntervalMs}");
                }
                else
                    _autoCommitInterval = 5000;
            }

            if (_options.TDReconnect == "true")
            {
                _reconnect = true;
                if (!int.TryParse(_options.TDReconnectRetryCount, out _reconnectRetryCount))
                    throw new ArgumentException($"Invalid reconnect retry count {_options.TDReconnectRetryCount}");
                if (_reconnectRetryCount < 0)
                    throw new ArgumentException($"Invalid reconnect retry count {_options.TDReconnectRetryCount}");
                if (!int.TryParse(_options.TDReconnectIntervalMs, out _reconnectRetryIntervalMs))
                    throw new ArgumentException($"Invalid reconnect retry intervalMs {_options.TDReconnectIntervalMs}");
                if (_reconnectRetryIntervalMs < 0)
                    throw new ArgumentException($"Invalid reconnect retry intervalMs {_options.TDReconnectIntervalMs}");
            }

            if (!string.IsNullOrEmpty(_options.ConnectionTimezone))
            {
                try
                {
                    _tz = TimeZoneInfo.FindSystemTimeZoneById(_options.ConnectionTimezone);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Invalid connection timezone {_options.ConnectionTimezone}", e);
                }
            }
            _connection = new TMQConnection(_options);
        }

        private void Reconnect()
        {
            if (!_reconnect)
                return;
            lock (_reconnectLock)
            {
                if (_connection != null)
                {
                    // connection is available, no need to reconnect
                    if (_connection.IsAvailable()) return;
                }

                TMQConnection connection = null;
                for (int i = 0; i < _reconnectRetryCount; i++)
                {
                    try
                    {
                        System.Threading.Thread.Sleep(_reconnectRetryIntervalMs);
                        connection = new TMQConnection(_options);
                        if (_topics != null)
                        {
                            connection.Subscribe(_topics, _options);
                        }

                        break;
                    }
                    catch (Exception)
                    {
                        if (connection != null)
                        {
                            connection.Close();
                            connection = null;
                        }
                    }
                }

                if (connection == null)
                {
                    throw new TDengineError((int)TDengineError.InternalErrorCode.WS_RECONNECT_FAILED,
                        "websocket connection reconnect failed");
                }

                if (_connection != null)
                {
                    _connection.Close();
                }

                _connection = connection;
            }
        }

        public ConsumeResult<TValue> Consume(int millisecondsTimeout)
        {
            if (_autoCommit)
            {
                var now = DateTime.Now;
                if (now >= _nextCommitTime)
                {
                    _connection.Commit();
                    _nextCommitTime = now.AddMilliseconds(_autoCommitInterval);
                }
            }

            try
            {
                return DoConsume(millisecondsTimeout);
            }
            catch (Exception e)
            {
                if (_connection.IsAvailable(e))
                {
                    throw;
                }

                Reconnect();
                return DoConsume(millisecondsTimeout);
            }
        }

        private ConsumeResult<TValue> DoConsume(int millisecondsTimeout)
        {
            var resp = _connection.Poll(millisecondsTimeout, _lastMessageId);
            if (!resp.HaveMessage)
            {
                return null;
            }

            _lastMessageId = resp.MessageId;
            var consumeResult = new ConsumeResult<TValue>(resp.MessageId, resp.Topic, resp.VgroupId, resp.Offset,
                (TMQ_RES)resp.MessageType);
            if (!NeedGetData((TMQ_RES)resp.MessageType)) return null;
            var result = new TMQWSRows(resp, _connection, _tz);
            while (result.Read())
            {
                var value = this.valueDeserializer.Deserialize(result, false, null);
                consumeResult.Message.Add(new TmqMessage<TValue> { Value = value, TableName = result.TableName });
            }

            return consumeResult;
        }

        public List<TopicPartition> Assignment
        {
            get
            {
                var result = new List<TopicPartition>();
                var topics = Subscription();
                foreach (var topic in topics)
                {
                    var resp = _connection.Assignment(topic);
                    foreach (var assignment in resp.Assignment)
                    {
                        result.Add(new TopicPartition(topic, assignment.VGroupId));
                    }
                }

                return result;
            }
        }

        public List<string> Subscription()
        {
            var resp = _connection.Subscription();
            return resp.Topics;
        }

        public void Subscribe(IEnumerable<string> topic)
        {
            var topics = (List<string>)topic;
            DoSubscribe(topics);
        }

        public void Subscribe(string topic)
        {
            var topics = new List<string> { topic };
            DoSubscribe(topics);
        }

        private void DoSubscribe(List<string> topics)
        {
            try
            {
                _connection.Subscribe(topics, _options);
                _topics = topics;
            }
            catch (Exception e)
            {
                if (_connection.IsAvailable(e))
                {
                    throw;
                }

                Reconnect();
                _connection.Subscribe(topics, _options);
            }
        }

        public void Unsubscribe()
        {
            _connection.Unsubscribe();
        }

        public void Commit(ConsumeResult<TValue> consumerResult)
        {
            _connection.CommitOffset(consumerResult.Topic, consumerResult.Partition, consumerResult.Offset);
        }

        public List<TopicPartitionOffset> Commit()
        {
            _connection.Commit();
            return Committed(TimeSpan.Zero);
        }

        public void Commit(IEnumerable<TopicPartitionOffset> tpos)
        {
            foreach (var tpo in tpos)
            {
                _connection.CommitOffset(tpo.Topic, tpo.Partition, tpo.Offset);
            }
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            _connection.Seek(tpo.Topic, tpo.Partition, tpo.Offset);
        }

        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            var assignment = Assignment;
            var args = new List<WSTopicVgroupId>(assignment.Count);
            var result = new List<TopicPartitionOffset>(assignment.Count);
            foreach (var topicPartition in assignment)
            {
                args.Add(new WSTopicVgroupId
                {
                    Topic = topicPartition.Topic,
                    VGroupId = topicPartition.Partition,
                });
            }

            var resp = _connection.Committed(args);
            for (int i = 0; i < args.Count; i++)
            {
                result.Add(new TopicPartitionOffset(args[i].Topic, args[i].VGroupId, resp.Committed[i]));
            }

            return result;
        }

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            var args = new List<WSTopicVgroupId>();
            var result = new List<TopicPartitionOffset>();
            foreach (var topicPartition in partitions)
            {
                args.Add(new WSTopicVgroupId
                {
                    Topic = topicPartition.Topic,
                    VGroupId = topicPartition.Partition,
                });
            }

            var resp = _connection.Committed(args);
            for (int i = 0; i < args.Count; i++)
            {
                result.Add(new TopicPartitionOffset(args[i].Topic, args[i].VGroupId, resp.Committed[i]));
            }

            return result;
        }

        public Offset Position(TopicPartition partition)
        {
            var vgid = new List<WSTopicVgroupId>(1)
            {
                new WSTopicVgroupId
                {
                    Topic = partition.Topic,
                    VGroupId = partition.Partition
                }
            };
            var resp = _connection.Position(vgid);
            return resp.Position[0];
        }

        public void Close()
        {
            if (_connection != null && _connection.IsAvailable())
                _connection.Close();
        }

        private bool NeedGetData(TMQ_RES type)
        {
            return type == TMQ_RES.TMQ_RES_DATA || type == TMQ_RES.TMQ_RES_METADATA;
        }
    }
}
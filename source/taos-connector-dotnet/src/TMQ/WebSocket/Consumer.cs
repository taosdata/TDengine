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
        private volatile TMQConnection _connection;
        private volatile FailoverAddressLease _addressLease;
        private int _closed;
        private readonly IReadOnlyList<FailoverAddress> _failoverAddresses;
        private readonly bool _autoCommit;
        private readonly int _autoCommitInterval;
        private DateTime _nextCommitTime;
        private readonly bool _reconnect;
        private readonly int _reconnectRetryCount;
        private readonly int _reconnectRetryIntervalMs;
        private List<string> _topics;
        private ulong _lastMessageId = 0;
        private TimeZoneInfo _tz = TimeZoneInfo.Local;
        private bool _autoCommitInProgress;

        private IDeserializer<TValue> valueDeserializer;

        private Dictionary<Type, object> defaultDeserializers = new Dictionary<Type, object>
        {
            { typeof(Dictionary<string, object>), DictionaryDeserializer.Dictionary },
        };

        private readonly object _reconnectLock = new object();
        private readonly object _batchCommitLock = new object();
        private int _batchCommitInProgress;
        private TMQConnection _batchCommitConnection;
        private readonly List<Tuple<TMQConnection, FailoverAddressLease>> _deferredCloseResources =
            new List<Tuple<TMQConnection, FailoverAddressLease>>();

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

            _failoverAddresses = _options.GetFailoverAddresses();

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
            if (!FailoverConnector.TryOpen(_failoverAddresses, 1, 0, false, null,
                    address => OpenTmqConnection(address, false), out var connection, out var lease,
                    out var lastException))
            {
                if (lastException != null)
                {
                    throw lastException;
                }

                throw new TDengineError((int)TDengineError.InternalErrorCode.WS_CONNECT_FAILED,
                    "websocket connection failed");
            }

            _connection = connection;
            _addressLease = lease;
        }

        private TMQConnection OpenTmqConnection(FailoverAddress address, bool resubscribeTopics)
        {
            TMQConnection connection = null;
            try
            {
                connection = new TMQConnection(_options, address);
                if (resubscribeTopics && _topics != null)
                {
                    connection.Subscribe(_topics, _options);
                }

                return connection;
            }
            catch
            {
                if (connection != null)
                {
                    connection.Close();
                }

                throw;
            }
        }

        private bool IsClosed()
        {
            return System.Threading.Volatile.Read(ref _closed) == 1;
        }

        private void ThrowIfClosed()
        {
            if (IsClosed())
            {
                throw new ObjectDisposedException(nameof(Consumer<TValue>));
            }
        }

        private TMQConnection GetConnectionOrThrowClosed()
        {
            var connection = _connection;
            if (connection == null || IsClosed())
            {
                throw new ObjectDisposedException(nameof(Consumer<TValue>));
            }

            return connection;
        }

        private static void CloseConnectionAndLease(TMQConnection connection, FailoverAddressLease lease)
        {
            if (connection != null)
            {
                connection.Close();
            }

            if (lease != null)
            {
                lease.Dispose();
            }
        }

        private void CloseOrDeferConnectionAndLease(TMQConnection connection, FailoverAddressLease lease)
        {
            if (connection == null && lease == null)
            {
                return;
            }

            var deferred = false;
            lock (_reconnectLock)
            {
                if (_batchCommitInProgress > 0 && ReferenceEquals(connection, _batchCommitConnection))
                {
                    for (var i = 0; i < _deferredCloseResources.Count; i++)
                    {
                        var pending = _deferredCloseResources[i];
                        if (ReferenceEquals(pending.Item1, connection) && ReferenceEquals(pending.Item2, lease))
                        {
                            deferred = true;
                            break;
                        }
                    }

                    if (!deferred)
                    {
                        _deferredCloseResources.Add(Tuple.Create(connection, lease));
                        deferred = true;
                    }
                }
            }

            if (!deferred)
            {
                CloseConnectionAndLease(connection, lease);
            }
        }

        private void EndBatchCommit()
        {
            List<Tuple<TMQConnection, FailoverAddressLease>> deferredResources = null;
            lock (_reconnectLock)
            {
                _batchCommitInProgress--;
                if (_batchCommitInProgress == 0)
                {
                    _batchCommitConnection = null;
                    if (_deferredCloseResources.Count > 0)
                    {
                        deferredResources =
                            new List<Tuple<TMQConnection, FailoverAddressLease>>(_deferredCloseResources);
                        _deferredCloseResources.Clear();
                    }
                }
            }

            if (deferredResources == null)
            {
                return;
            }

            for (var i = 0; i < deferredResources.Count; i++)
            {
                var resource = deferredResources[i];
                CloseConnectionAndLease(resource.Item1, resource.Item2);
            }
        }

        private void Reconnect()
        {
            if (!_reconnect)
                return;

            ThrowIfClosed();
            while (true)
            {
                FailoverAddress preferredAddress;
                lock (_reconnectLock)
                {
                    if (_connection != null)
                    {
                        // connection is available, no need to reconnect
                        if (_connection.IsAvailable()) return;
                    }

                    if (IsClosed())
                    {
                        throw new ObjectDisposedException(nameof(Consumer<TValue>));
                    }

                    preferredAddress = _addressLease == null ? null : _addressLease.Address;
                }

                if (!FailoverConnector.TryOpen(_failoverAddresses, _reconnectRetryCount, _reconnectRetryIntervalMs,
                        true, preferredAddress, address => OpenTmqConnection(address, true), out var connection,
                        out var lease, out var lastException))
                {
                    lock (_reconnectLock)
                    {
                        if (_connection != null && _connection.IsAvailable())
                        {
                            return;
                        }

                        if (IsClosed())
                        {
                            throw new ObjectDisposedException(nameof(Consumer<TValue>));
                        }
                    }

                    var reason = lastException == null
                        ? "websocket connection reconnect failed"
                        : $"websocket connection reconnect failed: {lastException.Message}";
                    throw new TDengineError((int)TDengineError.InternalErrorCode.WS_RECONNECT_FAILED,
                        reason);
                }

                TMQConnection oldConnection = null;
                FailoverAddressLease oldLease = null;
                var needDiscard = false;
                lock (_reconnectLock)
                {
                    if (IsClosed())
                    {
                        needDiscard = true;
                    }
                    else if (_connection != null && _connection.IsAvailable())
                    {
                        needDiscard = true;
                    }

                    if (!needDiscard)
                    {
                        oldConnection = _connection;
                        oldLease = _addressLease;
                        _connection = connection;
                        _addressLease = lease;
                    }
                }

                if (needDiscard)
                {
                    connection.Close();
                    lease.Dispose();
                    return;
                }

                if (oldConnection != null)
                {
                    CloseOrDeferConnectionAndLease(oldConnection, oldLease);
                }

                return;
            }
        }

        public ConsumeResult<TValue> Consume(int millisecondsTimeout)
        {
            ThrowIfClosed();
            AutoCommitIfNeeded();

            try
            {
                return DoConsume(millisecondsTimeout);
            }
            catch (Exception e)
            {
                var currentConnection = _connection;
                if (currentConnection != null && currentConnection.IsAvailable(e))
                {
                    throw;
                }

                ThrowIfClosed();
                Reconnect();
                return DoConsume(millisecondsTimeout);
            }
        }

        private void AutoCommitIfNeeded()
        {
            if (!_autoCommit)
            {
                return;
            }

            TMQConnection commitConnection;
            DateTime now;
            lock (_reconnectLock)
            {
                ThrowIfClosed();
                if (_autoCommitInProgress)
                {
                    return;
                }

                now = DateTime.Now;
                if (now < _nextCommitTime)
                {
                    return;
                }

                _autoCommitInProgress = true;
                commitConnection = GetConnectionOrThrowClosed();
            }

            try
            {
                commitConnection.Commit();
            }
            catch (Exception)
            {
                // Auto commit is best-effort and should not fail consume calls.
            }
            finally
            {
                lock (_reconnectLock)
                {
                    if (!IsClosed())
                    {
                        var completionTime = DateTime.Now;
                        if (completionTime >= _nextCommitTime)
                        {
                            _nextCommitTime = completionTime.AddMilliseconds(_autoCommitInterval);
                        }
                    }

                    _autoCommitInProgress = false;
                }
            }
        }

        private ConsumeResult<TValue> DoConsume(int millisecondsTimeout)
        {
            var connection = GetConnectionOrThrowClosed();
            var resp = connection.Poll(millisecondsTimeout, _lastMessageId);
            if (!resp.HaveMessage)
            {
                return null;
            }

            _lastMessageId = resp.MessageId;
            var consumeResult = new ConsumeResult<TValue>(resp.MessageId, resp.Topic, resp.VgroupId, resp.Offset,
                (TMQ_RES)resp.MessageType);
            if (!NeedGetData((TMQ_RES)resp.MessageType)) return null;
            var result = new TMQWSRows(resp, connection, _tz);
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
                    var connection = GetConnectionOrThrowClosed();
                    var resp = connection.Assignment(topic);
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
            var connection = GetConnectionOrThrowClosed();
            var resp = connection.Subscription();
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
                var connection = GetConnectionOrThrowClosed();
                connection.Subscribe(topics, _options);
                _topics = topics;
            }
            catch (Exception e)
            {
                var currentConnection = _connection;
                if (currentConnection != null && currentConnection.IsAvailable(e))
                {
                    throw;
                }

                ThrowIfClosed();
                Reconnect();
                var newConnection = GetConnectionOrThrowClosed();
                newConnection.Subscribe(topics, _options);
                _topics = topics;
            }
        }

        public void Unsubscribe()
        {
            var connection = GetConnectionOrThrowClosed();
            connection.Unsubscribe();
        }

        public void Commit(ConsumeResult<TValue> consumerResult)
        {
            var connection = GetConnectionOrThrowClosed();
            connection.CommitOffset(consumerResult.Topic, consumerResult.Partition, consumerResult.Offset);
        }

        public List<TopicPartitionOffset> Commit()
        {
            var connection = GetConnectionOrThrowClosed();
            connection.Commit();
            return Committed(TimeSpan.Zero);
        }

        public void Commit(IEnumerable<TopicPartitionOffset> tpos)
        {
            var offsets = new List<TopicPartitionOffset>();
            foreach (var tpo in tpos)
            {
                offsets.Add(tpo);
            }

            lock (_batchCommitLock)
            {
                TMQConnection connection;
                lock (_reconnectLock)
                {
                    connection = GetConnectionOrThrowClosed();
                    _batchCommitInProgress++;
                    _batchCommitConnection = connection;
                }

                try
                {
                    for (var i = 0; i < offsets.Count; i++)
                    {
                        var tpo = offsets[i];
                        connection.CommitOffset(tpo.Topic, tpo.Partition, tpo.Offset);
                    }
                }
                finally
                {
                    EndBatchCommit();
                }
            }
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            var connection = GetConnectionOrThrowClosed();
            connection.Seek(tpo.Topic, tpo.Partition, tpo.Offset);
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

            var connection = GetConnectionOrThrowClosed();
            var resp = connection.Committed(args);
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

            var connection = GetConnectionOrThrowClosed();
            var resp = connection.Committed(args);
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
            var connection = GetConnectionOrThrowClosed();
            var resp = connection.Position(vgid);
            return resp.Position[0];
        }

        public void Close()
        {
            if (System.Threading.Interlocked.Exchange(ref _closed, 1) == 1)
            {
                return;
            }

            TMQConnection oldConnection;
            FailoverAddressLease oldLease;

            lock (_reconnectLock)
            {
                oldConnection = _connection;
                oldLease = _addressLease;
                _connection = null;
                _addressLease = null;
            }

            CloseOrDeferConnectionAndLease(oldConnection, oldLease);
        }

        private bool NeedGetData(TMQ_RES type)
        {
            return type == TMQ_RES.TMQ_RES_DATA || type == TMQ_RES.TMQ_RES_METADATA;
        }
    }
}

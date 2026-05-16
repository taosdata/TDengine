using System;
using System.Collections.Generic;
using TDengine.Driver;
using NativeMethods = TDengine.Driver.Impl.NativeMethods.NativeMethods;

namespace TDengine.TMQ
{
    public class TmqMessage<TValue>
    {
        public string TableName { get; set; }
        public TValue Value { get; set; }
    }

    /// <summary>
    ///  Represent consume result.
    /// </summary>
    public class ConsumeResult<TValue> : IDisposable
    {
        /// <summary>
        ///     The topic associated with the message.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        ///     The partition associated with the message.
        /// </summary>
        public Partition Partition { get; set; }

        /// <summary>
        ///     The partition offset associated with the message.
        /// </summary>
        public Offset Offset { get; set; }

        public ulong MessageId { get; }

        public TMQ_RES Type { get; set; }

        /// <summary>
        ///     The TopicPartition associated with the message.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        /// <summary>
        ///     The TopicPartitionOffset associated with the message.
        /// </summary>
        public TopicPartitionOffset TopicPartitionOffset
        {
            get => new TopicPartitionOffset(Topic, Partition, Offset);
            set
            {
                Topic = value.Topic;
                Partition = value.Partition;
                Offset = value.Offset;
            }
        }

        public List<TmqMessage<TValue>> Message { get; set; }

        public ConsumeResult(string topic, Partition partition, Offset offset, TMQ_RES type)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Type = type;
            Message = new List<TmqMessage<TValue>>();
        }
        
        public ConsumeResult(ulong messageId,string topic, Partition partition, Offset offset, TMQ_RES type):this(topic, partition, offset, type)
        {
            MessageId = messageId;
        }

        public void Dispose()
        {
        }
    }
}
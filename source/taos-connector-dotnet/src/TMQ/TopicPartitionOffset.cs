namespace TDengine.TMQ
{
    /// <summary>
    ///     Represents a TMQ (topic, partition, offset) tuple.
    /// </summary>
    public class TopicPartitionOffset
    {
        /// <summary>
        ///     Initializes a new TopicPartitionOffset instance.
        /// </summary>
        /// <param name="topic">
        ///     A TMQ topic name.
        /// </param>
        /// <param name="partition">
        ///     A TMQ partition.
        /// </param>
        /// <param name="offset">
        ///     A TMQ offset value.
        /// </param>
        public TopicPartitionOffset(string topic, Partition partition, Offset offset)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
        }

        /// <summary>
        ///     Gets the TMQ topic name.
        /// </summary>
        public string Topic { get; }

        /// <summary>
        ///     Gets the TMQ partition.
        /// </summary>
        public Partition Partition { get; }

        /// <summary>
        ///     Gets the TMQ partition offset value.
        /// </summary>
        public Offset Offset { get; }
        

        /// <summary>
        ///     Gets the TopicPartition component of this TopicPartitionOffset instance.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        /// <summary>
        ///     Tests whether this TopicPartitionOffset instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a TopicPartitionOffset and all properties are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartitionOffset))
            {
                return false;
            }

            var tp = (TopicPartitionOffset)obj;
            return tp.Partition == Partition && tp.Topic == Topic && tp.Offset == Offset;
        }

        /// <summary>
        ///     Returns a hash code for this TopicPartitionOffset.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this TopicPartitionOffset.
        /// </returns>
        public override int GetHashCode()  
            // x by prime number is quick and gives decent distribution.
            => (Partition.GetHashCode()*251 + Topic.GetHashCode())*251 + Offset.GetHashCode();

        /// <summary>
        ///     Tests whether TopicPartitionOffset instance a is equal to TopicPartitionOffset instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionOffset instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionOffset instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionOffset instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(TopicPartitionOffset a, TopicPartitionOffset b)
        {
            if (a is null)
            {
                return (b is null);
            }
            
            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether TopicPartitionOffset instance a is not equal to TopicPartitionOffset instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionOffset instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionOffset instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionOffset instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(TopicPartitionOffset a, TopicPartitionOffset b)
            => !(a == b);

        /// <summary>
        ///     Returns a string representation of the TopicPartitionOffset object.
        /// </summary>
        /// <returns>
        ///     A string that represents the TopicPartitionOffset object.
        /// </returns>
        public override string ToString()
            => $"{Topic} [{Partition}] @{Offset}";
    }
}

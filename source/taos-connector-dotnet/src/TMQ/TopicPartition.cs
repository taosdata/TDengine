using System;

namespace TDengine.TMQ
{
    /// <summary>
    ///     Represents a TMQ (topic, partition) tuple.
    /// </summary>
    public class TopicPartition : IComparable
    {
        /// <summary>
        ///     Initializes a new TopicPartition instance.
        /// </summary>
        /// <param name="topic">
        ///     A TMQ topic name.
        /// </param>
        /// <param name="partition">
        ///     A TMQ partition.
        /// </param>
        public TopicPartition(string topic, Partition partition)
        {
            Topic = topic;
            Partition = partition;
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
        ///     Tests whether this TopicPartition instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a TopicPartition and all properties are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartition))
            {
                return false;
            }

            var tp = (TopicPartition)obj;
            return tp.Partition == Partition && tp.Topic == Topic;
        }

        /// <summary>
        ///     Returns a hash code for this TopicPartition.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this TopicPartition.
        /// </returns>
        public override int GetHashCode()
            // x by prime number is quick and gives decent distribution.
            => Partition.GetHashCode()*251 + Topic.GetHashCode();

        /// <summary>
        ///     Tests whether TopicPartition instance a is equal to TopicPartition instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartition instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartition instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartition instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(TopicPartition a, TopicPartition b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether TopicPartition instance a is not equal to TopicPartition instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartition instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartition instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartition instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(TopicPartition a, TopicPartition b)
            => !(a == b);

        /// <summary>
        ///     Returns a string representation of the TopicPartition object.
        /// </summary>
        /// <returns>
        ///     A string that represents the TopicPartition object.
        /// </returns>
        public override string ToString()
            => $"{Topic} [{Partition}]";

        /// <summary>
        ///     Compares the current instance with another object of the same type and returns
        ///     an integer that indicates whether the current instance precedes, follows, or
        ///     occurs in the same position in the sort order as the other object.
        /// </summary>
        /// <returns>
        ///     Less than zero:	This instance precedes obj in the sort order.
        ///     Zero:	This instance occurs in the same position in the sort order as obj.
        ///     Greater than zero:	This instance follows obj in the sort order.
        /// </returns>
        public int CompareTo(object obj)
        {
            if (obj.GetType() != GetType())
            {
                throw new ArgumentException($"Object of type {obj.GetType()} cannot be compared to object of type {GetType()}");
            }

            var topicCompareResult = String.Compare(Topic, ((TopicPartition)obj).Topic, StringComparison.Ordinal);
            if (topicCompareResult != 0)
            {
                return topicCompareResult;
            }

            return Partition.Value.CompareTo(((TopicPartition)obj).Partition.Value);
        }
    }
}

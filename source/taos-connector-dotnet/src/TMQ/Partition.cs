using System;

namespace TDengine.TMQ
{
    public struct Partition : IEquatable<Partition>
    {
        private const int Unavailable = -1;

        /// <summary>
        ///     A special value that refers to an unspecified / unknown partition.
        /// </summary>
        public static readonly Partition Any = new Partition(Unavailable);

        /// <summary>
        ///     Initializes a new instance of the Partition structure.
        /// </summary>
        /// <param name="partition">
        ///     The partition value
        /// </param>
        public Partition(int partition)
        {
            Value = partition;
        }

        /// <summary>
        ///     Gets the int value corresponding to this partition.
        /// </summary>
        public int Value { get; }

        /// <summary>
        ///     Gets whether or not this is one of the special 
        ///     partition values.
        /// </summary>
        public bool IsSpecial
            => Value == Unavailable;

        /// <summary>
        ///     Tests whether this Partition value is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a Partition instance and has the same value. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj is Partition p)
            {
                return Equals(p);
            }

            return false;
        }

        /// <summary>
        ///     Tests whether this Partition value is equal to the specified Partition.
        /// </summary>
        /// <param name="other">
        ///     The partition to test.
        /// </param>
        /// <returns>
        ///     true if other has the same value. false otherwise.
        /// </returns>
        public bool Equals(Partition other)
            => other.Value == Value;

        /// <summary>
        ///     Tests whether Partition value a is equal to Partition value b.
        /// </summary>
        /// <param name="a">
        ///     The first Partition value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Partition value to compare.
        /// </param>
        /// <returns>
        ///     true if Partition value a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(Partition a, Partition b)
            => a.Equals(b);

        /// <summary>
        ///     Tests whether Partition value a is not equal to Partition value b.
        /// </summary>
        /// <param name="a">
        ///     The first Partition value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Partition value to compare.
        /// </param>
        /// <returns>
        ///     true if Partition value a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(Partition a, Partition b)
            => !(a == b);

        /// <summary>
        ///     Tests whether Partition value a is greater than Partition value b.
        /// </summary>
        /// <param name="a">
        ///     The first Partition value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Partition value to compare.
        /// </param>
        /// <returns>
        ///     true if Partition value a is greater than Partition value b. false otherwise.
        /// </returns>
        public static bool operator >(Partition a, Partition b)
            => a.Value > b.Value;

        /// <summary>
        ///     Tests whether Partition value a is less than Partition value b.
        /// </summary>
        /// <param name="a">
        ///     The first Partition value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Partition value to compare.
        /// </param>
        /// <returns>
        ///     true if Partition value a is less than Partition value b. false otherwise.
        /// </returns>
        public static bool operator <(Partition a, Partition b)
            => a.Value < b.Value;

        /// <summary>
        ///     Tests whether Partition value a is greater than or equal to Partition value b.
        /// </summary>
        /// <param name="a">
        ///     The first Partition value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Partition value to compare.
        /// </param>
        /// <returns>
        ///     true if Partition value a is greater than or equal to Partition value b. false otherwise.
        /// </returns>
        public static bool operator >=(Partition a, Partition b)
            => a.Value >= b.Value;

        /// <summary>
        ///     Tests whether Partition value a is less than or equal to Partition value b.
        /// </summary>
        /// <param name="a">
        ///     The first Partition value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Partition value to compare.
        /// </param>
        /// <returns>
        ///     true if Partition value a is less than or equal to Partition value b. false otherwise.
        /// </returns>
        public static bool operator <=(Partition a, Partition b)
            => a.Value <= b.Value;

        /// <summary>
        ///     Returns a hash code for this Partition.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this Partition.
        /// </returns>
        public override int GetHashCode()
            => Value.GetHashCode();

        /// <summary>
        ///     Converts the specified int value to an Partition value.
        /// </summary>
        /// <param name="v">
        ///     The int value to convert.
        /// </param>
        public static implicit operator Partition(int v)
            => new Partition(v);

        /// <summary>
        ///     Converts the specified Partition value to an int value.
        /// </summary>
        /// <param name="o">
        ///     The Partition value to convert.
        /// </param>
        public static implicit operator int(Partition o)
            => o.Value;

        /// <summary>
        ///     Returns a string representation of the Partition object.
        /// </summary>
        /// <returns>
        ///     A string that represents the Partition object.
        /// </returns>
        public override string ToString()
        {
            switch (Value)
            {
                case Unavailable:
                    return $"[Any]";
                default:
                    return $"[{Value}]";
            }
        }
    }
}

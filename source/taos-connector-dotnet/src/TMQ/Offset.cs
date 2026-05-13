using System;

namespace TDengine.TMQ
{
    public struct Offset : IEquatable<Offset>
    {
        private const long OffsetInvalid = -2147467247;

        /// <summary>
        ///     A special value that refers to an invalid, unassigned or default partition offset.
        /// </summary>
        public static readonly Offset Unset = new Offset(OffsetInvalid);

        /// <summary>
        ///     Initializes a new instance of the Offset structure.
        /// </summary>
        /// <param name="offset">
        ///     The offset value
        /// </param>
        public Offset(long offset)
        {
            Value = offset;
        }

        /// <summary>
        ///     Gets the long value corresponding to this offset.
        /// </summary>
        public long Value { get; }

        /// <summary>
        ///     Gets whether or not this is one of the special 
        ///     offset values.
        /// </summary>
        public bool IsSpecial => Value == OffsetInvalid;

        /// <summary>
        ///     Tests whether this Offset value is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is an Offset and has the same value. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj is Offset o)
            {
                return Equals(o);
            }

            return false;
        }

        /// <summary>
        ///     Tests whether this Offset value is equal to the specified Offset.
        /// </summary>
        /// <param name="other">
        ///     The offset to test.
        /// </param>
        /// <returns>
        ///     true if other has the same value. false otherwise.
        /// </returns>
        public bool Equals(Offset other)
            => other.Value == Value;

        /// <summary>
        ///     Tests whether Offset value a is equal to Offset value b.
        /// </summary>
        /// <param name="a">
        ///     The first Offset value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Offset value to compare.
        /// </param>
        /// <returns>
        ///     true if Offset value a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(Offset a, Offset b)
            => a.Equals(b);

        /// <summary>
        ///     Tests whether Offset value a is not equal to Offset value b.
        /// </summary>
        /// <param name="a">
        ///     The first Offset value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Offset value to compare.
        /// </param>
        /// <returns>
        ///     true if Offset value a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(Offset a, Offset b)
            => !(a == b);

        /// <summary>
        ///     Tests whether Offset value a is greater than Offset value b.
        /// </summary>
        /// <param name="a">
        ///     The first Offset value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Offset value to compare.
        /// </param>
        /// <returns>
        ///     true if Offset value a is greater than Offset value b. false otherwise.
        /// </returns>
        public static bool operator >(Offset a, Offset b)
            => a.Value > b.Value;

        /// <summary>
        ///     Tests whether Offset value a is less than Offset value b.
        /// </summary>
        /// <param name="a">
        ///     The first Offset value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Offset value to compare.
        /// </param>
        /// <returns>
        ///     true if Offset value a is less than Offset value b. false otherwise.
        /// </returns>
        public static bool operator <(Offset a, Offset b)
            => a.Value < b.Value;

        /// <summary>
        ///     Tests whether Offset value a is greater than or equal to Offset value b.
        /// </summary>
        /// <param name="a">
        ///     The first Offset value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Offset value to compare.
        /// </param>
        /// <returns>
        ///     true if Offset value a is greater than or equal to Offset value b. false otherwise.
        /// </returns>
        public static bool operator >=(Offset a, Offset b)
            => a.Value >= b.Value;

        /// <summary>
        ///     Tests whether Offset value a is less than or equal to Offset value b.
        /// </summary>
        /// <param name="a">
        ///     The first Offset value to compare.
        /// </param>
        /// <param name="b">
        ///     The second Offset value to compare.
        /// </param>
        /// <returns>
        ///     true if Offset value a is less than or equal to Offset value b. false otherwise.
        /// </returns>
        public static bool operator <=(Offset a, Offset b)
            => a.Value <= b.Value;

        /// <summary>
        ///     Add an integer value to an Offset value.
        /// </summary>
        /// <param name="a">
        ///     The Offset value to add the integer value to.
        /// </param>
        /// <param name="b">
        ///     The integer value to add to the Offset value.
        /// </param>
        /// <returns>
        ///     The Offset value incremented by the integer value b.
        /// </returns>
        public static Offset operator +(Offset a, int b)
            => new Offset(a.Value + b);

        /// <summary>
        ///     Add a long value to an Offset value.
        /// </summary>
        /// <param name="a">
        ///     The Offset value to add the long value to.
        /// </param>
        /// <param name="b">
        ///     The long value to add to the Offset value.
        /// </param>
        /// <returns>
        ///     The Offset value incremented by the long value b.
        /// </returns>
        public static Offset operator +(Offset a, long b)
            => new Offset(a.Value + b);

        /// <summary>
        ///     Returns a hash code for this Offset.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this Offset.
        /// </returns>
        public override int GetHashCode()
            => Value.GetHashCode();

        /// <summary>
        ///     Converts the specified long value to an Offset value.
        /// </summary>
        /// <param name="v">
        ///     The long value to convert.
        /// </param>
        public static implicit operator Offset(long v)
            => new Offset(v);

        /// <summary>
        ///     Converts the specified Offset value to a long value.
        /// </summary>
        /// <param name="o">
        ///     The Offset value to convert.
        /// </param>
        public static implicit operator long(Offset o)
            => o.Value;

        /// <summary>
        ///     Returns a string representation of the Offset object.
        /// </summary>
        /// <returns>
        ///     A string that represents the Offset object.
        /// </returns>
        public override string ToString()
        {
            switch (Value)
            {
                case OffsetInvalid:
                    return $"Unset [{OffsetInvalid}]";
                default:
                    return Value.ToString();
            }
        }
    }
}

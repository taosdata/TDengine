/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Globalization;
using System.Numerics;

namespace Avro
{
    /// <summary>
    /// Represents a big decimal.
    /// </summary>
    #pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
    #pragma warning disable CA2225 // Operator overloads have named alternates
    public struct AvroDecimal : IConvertible, IFormattable, IComparable, IComparable<AvroDecimal>, IEquatable<AvroDecimal>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDecimal"/> class from a given double.
        /// </summary>
        /// <param name="value">The double value.</param>
        public AvroDecimal(double value)
            : this((decimal)value)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDecimal"/> class from a given float.
        /// </summary>
        /// <param name="value">The float value.</param>
        public AvroDecimal(float value)
            : this((decimal)value)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDecimal"/> class from a given decimal.
        /// </summary>
        /// <param name="value">The decimal value.</param>
        public AvroDecimal(decimal value)
        {
            var bytes = GetBytesFromDecimal(value);

            var unscaledValueBytes = new byte[12];
            Array.Copy(bytes, unscaledValueBytes, unscaledValueBytes.Length);

            var unscaledValue = new BigInteger(unscaledValueBytes);
            var scale = bytes[14];

            if (bytes[15] == 128)
                unscaledValue *= BigInteger.MinusOne;

            UnscaledValue = unscaledValue;
            Scale = scale;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDecimal"/> class from a given int.
        /// </summary>
        /// <param name="value">The int value.</param>
        public AvroDecimal(int value)
            : this(new BigInteger(value), 0) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDecimal"/> class from a given long.
        /// </summary>
        /// <param name="value">The long value.</param>
        public AvroDecimal(long value)
            : this(new BigInteger(value), 0) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDecimal"/> class from a given unsigned int.
        /// </summary>
        /// <param name="value">The unsigned int value.</param>
        public AvroDecimal(uint value)
            : this(new BigInteger(value), 0) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDecimal"/> class from a given unsigned long.
        /// </summary>
        /// <param name="value">The unsigned long value.</param>
        public AvroDecimal(ulong value)
            : this(new BigInteger(value), 0) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDecimal"/> class from a given <see cref="BigInteger"/>
        /// and a scale.
        /// </summary>
        /// <param name="unscaledValue">The double value.</param>
        /// <param name="scale">The scale.</param>
        public AvroDecimal(BigInteger unscaledValue, int scale)
        {
            UnscaledValue = unscaledValue;
            Scale = scale;
        }

        /// <summary>
        /// Gets the unscaled integer value represented by the current <see cref="AvroDecimal"/>.
        /// </summary>
        public BigInteger UnscaledValue { get; }

        /// <summary>
        /// Gets the scale of the current <see cref="AvroDecimal"/>.
        /// </summary>
        public int Scale { get; }

        /// <summary>
        /// Gets the sign of the current <see cref="AvroDecimal"/>.
        /// </summary>
        internal int Sign
        {
            get { return UnscaledValue.Sign; }
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a string.
        /// </summary>
        /// <returns>A string representation of the numeric value.</returns>
        public override string ToString()
        {
            var number = UnscaledValue.ToString($"D{Scale + 1}", CultureInfo.CurrentCulture);

            if (Scale > 0)
                return number.Insert(number.Length - Scale, CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);

            return number;
        }

        public static bool operator ==(AvroDecimal left, AvroDecimal right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(AvroDecimal left, AvroDecimal right)
        {
            return !left.Equals(right);
        }

        public static bool operator >(AvroDecimal left, AvroDecimal right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator >=(AvroDecimal left, AvroDecimal right)
        {
            return left.CompareTo(right) >= 0;
        }

        public static bool operator <(AvroDecimal left, AvroDecimal right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator <=(AvroDecimal left, AvroDecimal right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static bool operator ==(AvroDecimal left, decimal right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(AvroDecimal left, decimal right)
        {
            return !left.Equals(right);
        }

        public static bool operator >(AvroDecimal left, decimal right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator >=(AvroDecimal left, decimal right)
        {
            return left.CompareTo(right) >= 0;
        }

        public static bool operator <(AvroDecimal left, decimal right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator <=(AvroDecimal left, decimal right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static bool operator ==(decimal left, AvroDecimal right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(decimal left, AvroDecimal right)
        {
            return !left.Equals(right);
        }

        public static bool operator >(decimal left, AvroDecimal right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator >=(decimal left, AvroDecimal right)
        {
            return left.CompareTo(right) >= 0;
        }

        public static bool operator <(decimal left, AvroDecimal right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator <=(decimal left, AvroDecimal right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static explicit operator byte(AvroDecimal value)
        {
            return ToByte(value);
        }

        /// <summary>
        /// Creates a byte from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>A byte.</returns>
        public static byte ToByte(AvroDecimal value)
        {
            return value.ToType<byte>();
        }

        public static explicit operator sbyte(AvroDecimal value)
        {
            return ToSByte(value);
        }

        /// <summary>
        /// Creates a signed byte from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>A signed byte.</returns>
        public static sbyte ToSByte(AvroDecimal value)
        {
            return value.ToType<sbyte>();
        }

        public static explicit operator short(AvroDecimal value)
        {
            return ToInt16(value);
        }

        /// <summary>
        /// Creates a short from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>A short.</returns>
        public static short ToInt16(AvroDecimal value)
        {
            return value.ToType<short>();
        }

        public static explicit operator int(AvroDecimal value)
        {
            return ToInt32(value);
        }

        /// <summary>
        /// Creates an int from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>An int.</returns>
        public static int ToInt32(AvroDecimal value)
        {
            return value.ToType<int>();
        }

        public static explicit operator long(AvroDecimal value)
        {
            return ToInt64(value);
        }

        /// <summary>
        /// Creates a long from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>A long.</returns>
        public static long ToInt64(AvroDecimal value)
        {
            return value.ToType<long>();
        }

        public static explicit operator ushort(AvroDecimal value)
        {
            return ToUInt16(value);
        }

        /// <summary>
        /// Creates an unsigned short from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>An unsigned short.</returns>
        public static ushort ToUInt16(AvroDecimal value)
        {
            return value.ToType<ushort>();
        }

        public static explicit operator uint(AvroDecimal value)
        {
            return ToUInt32(value);
        }

        /// <summary>
        /// Creates an unsigned int from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>An unsigned int.</returns>
        public static uint ToUInt32(AvroDecimal value)
        {
            return value.ToType<uint>();
        }

        public static explicit operator ulong(AvroDecimal value)
        {
            return ToUInt64(value);
        }

        /// <summary>
        /// Creates an unsigned long from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>An unsigned long.</returns>
        public static ulong ToUInt64(AvroDecimal value)
        {
            return value.ToType<ulong>();
        }

        public static explicit operator float(AvroDecimal value)
        {
            return ToSingle(value);
        }

        /// <summary>
        /// Creates a double from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>A double.</returns>
        public static float ToSingle(AvroDecimal value)
        {
            return value.ToType<float>();
        }

        public static explicit operator double(AvroDecimal value)
        {
            return ToDouble(value);
        }

        /// <summary>
        /// Creates a double from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>A double.</returns>
        public static double ToDouble(AvroDecimal value)
        {
            return value.ToType<double>();
        }

        public static explicit operator decimal(AvroDecimal value)
        {
            return ToDecimal(value);
        }

        /// <summary>
        /// Creates a decimal from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>A decimal.</returns>
        public static decimal ToDecimal(AvroDecimal value)
        {
            return value.ToType<decimal>();
        }

        public static explicit operator BigInteger(AvroDecimal value)
        {
            return ToBigInteger(value);
        }

        /// <summary>
        /// Creates a <see cref="BigInteger"/> from a given <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="value">The <see cref="AvroDecimal"/>.</param>
        /// <returns>A <see cref="BigInteger"/>.</returns>
        public static BigInteger ToBigInteger(AvroDecimal value)
        {
            var scaleDivisor = BigInteger.Pow(new BigInteger(10), value.Scale);
            var scaledValue = BigInteger.Divide(value.UnscaledValue, scaleDivisor);
            return scaledValue;
        }

        public static implicit operator AvroDecimal(byte value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(sbyte value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(short value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(int value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(long value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(ushort value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(uint value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(ulong value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(float value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(double value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(decimal value)
        {
            return new AvroDecimal(value);
        }

        public static implicit operator AvroDecimal(BigInteger value)
        {
            return new AvroDecimal(value, 0);
        } 

        /// <summary>
        /// Converts the numeric value of the current <see cref="AvroDecimal"/> to a given type.
        /// </summary>
        /// <typeparam name="T">The type to which the value of the current <see cref="AvroDecimal"/> should be converted.</typeparam>
        /// <returns>A value of type <typeparamref name="T"/> converted from the current <see cref="AvroDecimal"/>.</returns>
        public T ToType<T>()
            where T : struct
        {
            return (T)((IConvertible)this).ToType(typeof(T), null);
        }

        /// <summary>
        /// Converts the numeric value of the current <see cref="AvroDecimal"/> to a given type.
        /// </summary>
        /// <param name="conversionType">The type to which the value of the current <see cref="AvroDecimal"/> should be converted.</param>
        /// <param name="provider">An System.IFormatProvider interface implementation that supplies culture-specific formatting information.</param>
        /// <returns></returns>
        object IConvertible.ToType(Type conversionType, IFormatProvider provider)
        {
            var scaleDivisor = BigInteger.Pow(new BigInteger(10), Scale);
            var remainder = BigInteger.Remainder(UnscaledValue, scaleDivisor);
            var scaledValue = BigInteger.Divide(UnscaledValue, scaleDivisor);

            if (scaledValue > new BigInteger(Decimal.MaxValue))
                throw new ArgumentOutOfRangeException("value", "The value " + UnscaledValue + " cannot fit into " + conversionType.Name + ".");

            var leftOfDecimal = (decimal)scaledValue;
            var rightOfDecimal = ((decimal)remainder) / ((decimal)scaleDivisor);

            var value = leftOfDecimal + rightOfDecimal;
            return Convert.ChangeType(value, conversionType, provider);
        }

        /// <summary>
        /// Returns a value that indicates whether the current <see cref="AvroDecimal"/> and a specified object
        /// have the same value.
        /// </summary>
        /// <param name="obj">The object to compare.</param>
        /// <returns>true if the obj argument is an <see cref="AvroDecimal"/> object, and its value
        /// is equal to the value of the current <see cref="AvroDecimal"/> instance; otherwise false.
        /// </returns>
        public override bool Equals(object obj)
        {
            return (obj is AvroDecimal) && Equals((AvroDecimal)obj);
        }

        /// <summary>
        /// Returns the hash code for the current <see cref="AvroDecimal"/>.
        /// </summary>
        /// <returns>The hash code.</returns>
        public override int GetHashCode()
        {
            return UnscaledValue.GetHashCode() ^ Scale.GetHashCode();
        }

        /// <summary>
        /// Returns the <see cref="TypeCode"/> for the current <see cref="AvroDecimal"/>.
        /// </summary>
        /// <returns><see cref="TypeCode.Object"/>.</returns>
        TypeCode IConvertible.GetTypeCode()
        {
            return TypeCode.Object;
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a boolean.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>true or false, which reflects the value of the current <see cref="AvroDecimal"/>.</returns>
        bool IConvertible.ToBoolean(IFormatProvider provider)
        {
            return Convert.ToBoolean(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a byte.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>A byte.</returns>
        byte IConvertible.ToByte(IFormatProvider provider)
        {
            return Convert.ToByte(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a char.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>This method always throws an <see cref="InvalidCastException"/>.</returns>
        char IConvertible.ToChar(IFormatProvider provider)
        {
            throw new InvalidCastException("Cannot cast BigDecimal to Char");
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a <see cref="DateTime"/>.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>This method always throws an <see cref="InvalidCastException"/>.</returns>
        DateTime IConvertible.ToDateTime(IFormatProvider provider)
        {
            throw new InvalidCastException("Cannot cast BigDecimal to DateTime");
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a decimal.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>A decimal.</returns>
        decimal IConvertible.ToDecimal(IFormatProvider provider)
        {
            return Convert.ToDecimal(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a double.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>A double.</returns>
        double IConvertible.ToDouble(IFormatProvider provider)
        {
            return Convert.ToDouble(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a short.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>A short.</returns>
        short IConvertible.ToInt16(IFormatProvider provider)
        {
            return Convert.ToInt16(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to an int.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>An int.</returns>
        int IConvertible.ToInt32(IFormatProvider provider)
        {
            return Convert.ToInt32(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a long.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>A long.</returns>
        long IConvertible.ToInt64(IFormatProvider provider)
        {
            return Convert.ToInt64(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a signed byte.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>A signed byte.</returns>
        sbyte IConvertible.ToSByte(IFormatProvider provider)
        {
            return Convert.ToSByte(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a float.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>A float.</returns>
        float IConvertible.ToSingle(IFormatProvider provider)
        {
            return Convert.ToSingle(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a string.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>A string.</returns>
        string IConvertible.ToString(IFormatProvider provider)
        {
            return Convert.ToString(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to an unsigned short.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>An unsigned short.</returns>
        ushort IConvertible.ToUInt16(IFormatProvider provider)
        {
            return Convert.ToUInt16(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to an unsigned int.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>An unsigned int.</returns>
        uint IConvertible.ToUInt32(IFormatProvider provider)
        {
            return Convert.ToUInt32(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to an unsigned long.
        /// </summary>
        /// <param name="provider">The format provider.</param>
        /// <returns>An unsigned long.</returns>
        ulong IConvertible.ToUInt64(IFormatProvider provider)
        {
            return Convert.ToUInt64(this, provider);
        }

        /// <summary>
        /// Converts the current <see cref="AvroDecimal"/> to a string.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="formatProvider">The format provider.</param>
        /// <returns>A string representation of the numeric value.</returns>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return ToString();
        }

        /// <summary>
        /// Compares the value of the current <see cref="AvroDecimal"/> to the value of another object.
        /// </summary>
        /// <param name="obj">The object to compare.</param>
        /// <returns>A value that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(object obj)
        {
            if (obj == null)
                return 1;

            if (!(obj is AvroDecimal))
                throw new ArgumentException("Compare to object must be a BigDecimal", nameof(obj));

            return CompareTo((AvroDecimal)obj);
        }

        /// <summary>
        /// Compares the value of the current <see cref="AvroDecimal"/> to the value of another
        /// <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="other">The <see cref="AvroDecimal"/> to compare.</param>
        /// <returns>A value that indicates the relative order of the <see cref="AvroDecimal"/>
        /// instances being compared.</returns>
        public int CompareTo(AvroDecimal other)
        {
            var unscaledValueCompare = UnscaledValue.CompareTo(other.UnscaledValue);
            var scaleCompare = Scale.CompareTo(other.Scale);

            // if both are the same value, return the value
            if (unscaledValueCompare == scaleCompare)
                return unscaledValueCompare;

            // if the scales are both the same return unscaled value
            if (scaleCompare == 0)
                return unscaledValueCompare;

            var scaledValue = BigInteger.Divide(UnscaledValue, BigInteger.Pow(new BigInteger(10), Scale));
            var otherScaledValue = BigInteger.Divide(other.UnscaledValue, BigInteger.Pow(new BigInteger(10), other.Scale));

            return scaledValue.CompareTo(otherScaledValue);
        }

        /// <summary>
        /// Returns a value that indicates whether the current <see cref="AvroDecimal"/> has the same
        /// value as another <see cref="AvroDecimal"/>.
        /// </summary>
        /// <param name="other">The <see cref="AvroDecimal"/> to compare.</param>
        /// <returns>true if the current <see cref="AvroDecimal"/> has the same value as <paramref name="other"/>;
        /// otherwise false.</returns>
        public bool Equals(AvroDecimal other)
        {
            return Scale == other.Scale && UnscaledValue == other.UnscaledValue;
        }

        private static byte[] GetBytesFromDecimal(decimal d)
        {
            byte[] bytes = new byte[16];

            int[] bits = decimal.GetBits(d);
            int lo = bits[0];
            int mid = bits[1];
            int hi = bits[2];
            int flags = bits[3];

            bytes[0] = (byte)lo;
            bytes[1] = (byte)(lo >> 8);
            bytes[2] = (byte)(lo >> 0x10);
            bytes[3] = (byte)(lo >> 0x18);
            bytes[4] = (byte)mid;
            bytes[5] = (byte)(mid >> 8);
            bytes[6] = (byte)(mid >> 0x10);
            bytes[7] = (byte)(mid >> 0x18);
            bytes[8] = (byte)hi;
            bytes[9] = (byte)(hi >> 8);
            bytes[10] = (byte)(hi >> 0x10);
            bytes[11] = (byte)(hi >> 0x18);
            bytes[12] = (byte)flags;
            bytes[13] = (byte)(flags >> 8);
            bytes[14] = (byte)(flags >> 0x10);
            bytes[15] = (byte)(flags >> 0x18);

            return bytes;
        }
    }
    #pragma warning restore CA2225 // Operator overloads have named alternates
    #pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}

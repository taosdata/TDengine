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
using System.IO;
using System.Text;

namespace Avro.IO
{
    /// <content>
    /// Contains the netstandard2.0 specific functionality for BinaryDecoder.
    /// </content>
    public partial class BinaryDecoder
    {
        /// <summary>
        /// It is hard to find documentation about the real maximum array length in .NET Framework 4.6.1, but this seems to work :-/
        /// </summary>
        private const int MaxDotNetArrayLength = 0x3FFFFFFF;

        /// <summary>
        /// A float is written as 4 bytes.
        /// The float is converted into a 32-bit integer using a method equivalent to
        /// Java's floatToIntBits and then encoded in little-endian format.
        /// </summary>
        /// <returns></returns>
        public float ReadFloat()
        {
            byte[] buffer = read(4);

            if (!BitConverter.IsLittleEndian)
                Array.Reverse(buffer);

            return BitConverter.ToSingle(buffer, 0);

            //int bits = (Stream.ReadByte() & 0xff |
            //(Stream.ReadByte()) & 0xff << 8 |
            //(Stream.ReadByte()) & 0xff << 16 |
            //(Stream.ReadByte()) & 0xff << 24);
            //return intBitsToFloat(bits);
        }

        /// <summary>
        /// A double is written as 8 bytes.
        /// The double is converted into a 64-bit integer using a method equivalent to
        /// Java's doubleToLongBits and then encoded in little-endian format.
        /// </summary>
        /// <returns>A double value.</returns>
        public double ReadDouble()
        {
            long bits = (stream.ReadByte() & 0xffL) |
              (stream.ReadByte() & 0xffL) << 8 |
              (stream.ReadByte() & 0xffL) << 16 |
              (stream.ReadByte() & 0xffL) << 24 |
              (stream.ReadByte() & 0xffL) << 32 |
              (stream.ReadByte() & 0xffL) << 40 |
              (stream.ReadByte() & 0xffL) << 48 |
              (stream.ReadByte() & 0xffL) << 56;
            return BitConverter.Int64BitsToDouble(bits);
        }

        /// <summary>
        /// Reads a string written by <see cref="BinaryEncoder.WriteString(string)"/>.
        /// </summary>
        /// <returns>String read from the stream.</returns>
        public string ReadString()
        {
            int length = ReadInt();

            if (length < 0)
            {
                throw new AvroException("Can not deserialize a string with negative length!");
            }

            if (length > MaxDotNetArrayLength)
            {
                throw new AvroException("String length is not supported!");
            }

            using (var binaryReader = new BinaryReader(stream, Encoding.UTF8, true))
            {
                var bytes = binaryReader.ReadBytes(length);

                if (bytes.Length != length)
                {
                    throw new AvroException("Could not read as many bytes from stream as expected!");
                }

                return Encoding.UTF8.GetString(bytes);
            }
        }

        private void Read(byte[] buffer, int start, int len)
        {
            while (len > 0)
            {
                int n = stream.Read(buffer, start, len);
                if (n <= 0) throw new AvroException("End of stream reached");
                start += n;
                len -= n;
            }
        }
    }
}

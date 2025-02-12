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

namespace Avro.IO
{
    /// <summary>
    /// Decoder for Avro binary format
    /// </summary>
    public partial class BinaryDecoder : Decoder
    {
        private readonly Stream stream;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryDecoder"/> class.
        /// </summary>
        /// <param name="stream">Stream to decode.</param>
        public BinaryDecoder(Stream stream)
        {
            this.stream = stream;
        }

        /// <summary>
        /// null is written as zero bytes
        /// </summary>
        public void ReadNull()
        {
        }

        /// <summary>
        /// a boolean is written as a single byte
        /// whose value is either 0 (false) or 1 (true).
        /// </summary>
        /// <returns></returns>
        public bool ReadBoolean()
        {
            byte b = read();
            if (b == 0) return false;
            if (b == 1) return true;
            throw new AvroException("Not a boolean value in the stream: " + b);
        }

        /// <summary>
        /// int and long values are written using variable-length, zig-zag coding.
        /// </summary>
        /// <returns>An integer value.</returns>
        public int ReadInt()
        {
            return (int)ReadLong();
        }

        /// <summary>
        /// int and long values are written using variable-length, zig-zag coding.
        /// </summary>
        /// <returns>A long value.</returns>
        public long ReadLong()
        {
            byte b = read();
            ulong n = b & 0x7FUL;
            int shift = 7;
            while ((b & 0x80) != 0)
            {
                b = read();
                n |= (b & 0x7FUL) << shift;
                shift += 7;
            }
            long value = (long)n;
            return (-(value & 0x01L)) ^ ((value >> 1) & 0x7fffffffffffffffL);
        }

        /// <summary>
        /// Bytes are encoded as a long followed by that many bytes of data.
        /// </summary>
        /// <returns></returns>
        public byte[] ReadBytes()
        {
            return read(ReadLong());
        }

        /// <summary>
        /// Reads an enumeration.
        /// </summary>
        /// <returns>Ordinal value of the enum.</returns>
        public int ReadEnum()
        {
            return ReadInt();
        }

        /// <summary>
        /// Reads the size of the first block of an array.
        /// </summary>
        /// <returns>Size of the first block of an array.</returns>
        public long ReadArrayStart()
        {
            return doReadItemCount();
        }

        /// <summary>
        /// Processes the next block of an array and returns the number of items in the block and
        /// let's the caller read those items.
        /// </summary>
        /// <returns>Number of items in the next block of an array.</returns>
        public long ReadArrayNext()
        {
            return doReadItemCount();
        }

        /// <summary>
        /// Reads the size of the next block of map-entries.
        /// </summary>
        /// <returns>Size of the next block of map-entries.</returns>
        public long ReadMapStart()
        {
            return doReadItemCount();
        }

        /// <summary>
        /// Processes the next block of map entries and returns the count of them.
        /// </summary>
        /// <returns>Number of entires in the next block of a map.</returns>
        public long ReadMapNext()
        {
            return doReadItemCount();
        }

        /// <summary>
        /// Reads the tag index of a union written by <see cref="BinaryEncoder.WriteUnionIndex(int)"/>.
        /// </summary>
        /// <returns>Tag index of a union.</returns>
        public int ReadUnionIndex()
        {
            return ReadInt();
        }

        /// <summary>
        /// Reads fixed sized binary object.
        /// </summary>
        /// <param name="buffer">Buffer to read the fixed value into.</param>
        public void ReadFixed(byte[] buffer)
        {
            ReadFixed(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Reads fixed sized binary object.
        /// </summary>
        /// <param name="buffer">Buffer to read the fixed value into.</param>
        /// <param name="start">
        /// Position to start writing the fixed value to in the <paramref name="buffer"/>.
        /// </param>
        /// <param name="length">
        /// Number of bytes of the fixed to read.
        /// </param>
        public void ReadFixed(byte[] buffer, int start, int length)
        {
            Read(buffer, start, length);
        }

        /// <summary>
        /// Skips over a null value.
        /// </summary>
        public void SkipNull()
        {
            ReadNull();
        }

        /// <summary>
        /// Skips over a boolean value.
        /// </summary>
        public void SkipBoolean()
        {
            ReadBoolean();
        }

        /// <summary>
        /// Skips over an int value.
        /// </summary>
        public void SkipInt()
        {
            ReadInt();
        }

        /// <summary>
        /// Skips over a long value.
        /// </summary>
        public void SkipLong()
        {
            ReadLong();
        }

        /// <summary>
        /// Skips over a float value.
        /// </summary>
        public void SkipFloat()
        {
            Skip(4);
        }

        /// <summary>
        /// Skips over a double value.
        /// </summary>
        public void SkipDouble()
        {
            Skip(8);
        }

        /// <summary>
        /// Skips a byte-string written by <see cref="BinaryEncoder.WriteBytes(byte[])"/>.
        /// </summary>
        public void SkipBytes()
        {
            Skip(ReadLong());
        }

        /// <summary>
        /// Skips a string written by <see cref="BinaryEncoder.WriteString(string)"/>.
        /// </summary>
        public void SkipString()
        {
            SkipBytes();
        }

        /// <summary>
        /// Skips an enum value.
        /// </summary>
        public void SkipEnum()
        {
            ReadLong();
        }

        /// <summary>
        /// Skips a union tag index.
        /// </summary>
        public void SkipUnionIndex()
        {
            ReadLong();
        }

        /// <summary>
        /// Skips a fixed value of a specified length.
        /// </summary>
        /// <param name="len">Length of the fixed to skip.</param>
        public void SkipFixed(int len)
        {
            Skip(len);
        }

        // Read p bytes into a new byte buffer
        private byte[] read(long p)
        {
            byte[] buffer = new byte[p];
            Read(buffer, 0, buffer.Length);
            return buffer;
        }

        private byte read()
        {
            int n = stream.ReadByte();
            if (n >= 0) return (byte)n;
            throw new AvroException("End of stream reached");
        }

        private long doReadItemCount()
        {
            long result = ReadLong();
            if (result < 0)
            {
                ReadLong(); // Consume byte-count if present
                result = -result;
            }
            return result;
        }

        private void Skip(int p)
        {
            stream.Seek(p, SeekOrigin.Current);
        }

        private void Skip(long p)
        {
            stream.Seek(p, SeekOrigin.Current);
        }
    }
}

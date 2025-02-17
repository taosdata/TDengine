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

namespace Avro.IO
{
    /// <summary>
    /// Decoder is used to decode Avro data on a stream. There are methods to read the Avro types on the stream. There are also
    /// methods to skip items, which are usually more efficient than reading, on the stream.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming",
        "CA1715:Identifiers should have correct prefix", Justification = "Maintain public API")]
    public interface Decoder
    {
        /// <summary>
        /// Reads a null Avro type.
        /// </summary>
        void ReadNull();

        /// <summary>
        /// Read a boolean Avro type
        /// </summary>
        /// <returns>The boolean just read</returns>
        bool ReadBoolean();

        /// <summary>
        /// Reads an int Avro type.
        /// </summary>
        /// <returns>The int just read</returns>
        int ReadInt();

        /// <summary>
        /// Reads a long Avro type.
        /// </summary>
        /// <returns>The long just read</returns>
        long ReadLong();

        /// <summary>
        /// Reads a float Avro type
        /// </summary>
        /// <returns>The float just read</returns>
        float ReadFloat();

        /// <summary>
        /// Reads a double Avro type
        /// </summary>
        /// <returns>The double just read</returns>
        double ReadDouble();

        /// <summary>
        /// Reads the bytes Avro type
        /// </summary>
        /// <returns>The bytes just read</returns>
        byte[] ReadBytes();

        /// <summary>
        /// Reads a string Avro type
        /// </summary>
        /// <returns>The string just read</returns>
        string ReadString();

        /// <summary>
        /// Reads an enum AvroType
        /// </summary>
        /// <returns>The enum just read</returns>
        int ReadEnum();

        /// <summary>
        /// Starts reading the array Avro type. This, together with ReadArrayNext() is used to read the
        /// items from Avro array. This returns the number of entries in the initial chunk. After consuming
        /// the chunk, the client should call ReadArrayNext() to get the number of entries in the next
        /// chunk. The client should repeat the procedure until there are no more entries in the array.
        ///
        /// for (int n = decoder.ReadArrayStart(); n > 0; n = decoder.ReadArrayNext())
        /// {
        ///     // Read one array entry.
        /// }
        /// </summary>
        /// <returns>The number of entries in the initial chunk, 0 if the array is empty.</returns>
        long ReadArrayStart();

        /// <summary>
        /// See ReadArrayStart().
        /// </summary>
        /// <returns>The number of array entries in the next chunk, 0 if there are no more entries.</returns>
        long ReadArrayNext();

        /// <summary>
        /// Starts reading the map Avro type. This, together with ReadMapNext() is used to read the
        /// entries from Avro map. This returns the number of entries in the initial chunk. After consuming
        /// the chunk, the client should call ReadMapNext() to get the number of entriess in the next
        /// chunk. The client should repeat the procedure until there are no more entries in the array.
        /// for (int n = decoder.ReadMapStart(); n > 0; n = decoder.ReadMapNext())
        /// {
        ///     // Read one map entry.
        /// }
        /// </summary>
        /// <returns>The number of entries in the initial chunk, 0 if the map is empty.</returns>
        long ReadMapStart();

        /// <summary>
        /// See ReadMapStart().
        /// </summary>
        /// <returns>The number of map entries in the next chunk, 0 if there are no more entries.</returns>
        long ReadMapNext();

        /// <summary>
        /// Reads the index, which determines the type in an union Avro type.
        /// </summary>
        /// <returns>The index of the type within the union.</returns>
        int ReadUnionIndex();

        /// <summary>
        /// A convenience method for ReadFixed(buffer, 0, buffer.Length);
        /// </summary>
        /// <param name="buffer"> The buffer to read into.</param>
        void ReadFixed(byte[] buffer);

        /// <summary>
        /// Read a Fixed Avro type of length.
        /// </summary>
        /// <param name="buffer">Buffer to read into</param>
        /// <param name="start">Starting position of buffer to read into</param>
        /// <param name="length">Number of bytes to read</param>
        void ReadFixed(byte[] buffer, int start, int length);

        /// <summary>
        /// Skips a null Avro type on the stream.
        /// </summary>
        void SkipNull();

        /// <summary>
        ///  Skips a boolean Avro type on the stream.
        /// </summary>
        void SkipBoolean();

        /// <summary>
        ///  Skips a int Avro type on the stream.
        /// </summary>
        void SkipInt();

        /// <summary>
        ///  Skips a long Avro type on the stream.
        /// </summary>
        void SkipLong();

        /// <summary>
        /// Skips a float Avro type on the stream.
        /// </summary>
        void SkipFloat();

        /// <summary>
        /// Skips a double Avro type on the stream.
        /// </summary>
        void SkipDouble();

        /// <summary>
        /// Skips a bytes Avro type on the stream.
        /// </summary>
        void SkipBytes();

        /// <summary>
        /// Skips a string Avro type on the stream.
        /// </summary>
        void SkipString();

        /// <summary>
        /// Skips an enumeration.
        /// </summary>
        void SkipEnum();

        /// <summary>
        /// Skips a union tag index.
        /// </summary>
        void SkipUnionIndex();

        /// <summary>
        /// Skips a fixed of a specified length.
        /// </summary>
        /// <param name="len">Length of the fixed.</param>
        void SkipFixed(int len);
    }

}

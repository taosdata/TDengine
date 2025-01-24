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
    /// Defines the interface for a class that provies low-level support for serializing Avro
    /// values.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Naming",
        "CA1715:Identifiers should have correct prefix", Justification = "Maintain public API")]
    public interface Encoder
    {
        /// <summary>
        /// Writes a null value.
        /// </summary>
        void WriteNull();

        /// <summary>
        /// Writes a boolean value.
        /// </summary>
        /// <param name="value">Value to write.</param>
        void WriteBoolean(bool value);

        /// <summary>
        /// Writes an int value.
        /// </summary>
        /// <param name="value">Value to write.</param>
        void WriteInt(int value);

        /// <summary>
        /// Writes a long value.
        /// </summary>
        /// <param name="value">Value to write.</param>
        void WriteLong(long value);

        /// <summary>
        /// Writes a float value.
        /// </summary>
        /// <param name="value">Value to write.</param>
        void WriteFloat(float value);

        /// <summary>
        /// Writes a double value.
        /// </summary>
        /// <param name="value">Value to write.</param>
        void WriteDouble(double value);

        /// <summary>
        /// Writes a byte string.
        /// </summary>
        /// <param name="value">Value to write.</param>
        void WriteBytes(byte[] value);

        /// <summary>
        /// Writes a byte string.
        /// </summary>
        /// <param name="value">The byte[] to be read (fully or partially)</param>
        /// <param name="offset">The offset from the beginning of the byte[] to start writing</param>
        /// <param name="length">The length of the data to be read from the byte[].</param>

        void WriteBytes(byte[] value, int offset, int length);

        /// <summary>
        /// Writes an Unicode string.
        /// </summary>
        /// <param name="value">Value to write.</param>
        void WriteString(string value);

        /// <summary>
        /// Writes an enumeration.
        /// </summary>
        /// <param name="value">Value to write.</param>
        void WriteEnum(int value);

        /// <summary>
        /// Call this method before writing a batch of items in an array or a map.
        /// </summary>
        /// <param name="value">Number of <see cref="StartItem"/> calls to follow.</param>
        void SetItemCount(long value);

        /// <summary>
        /// Start a new item of an array or map. See <see cref="WriteArrayStart"/> for usage
        /// information.
        /// </summary>
        void StartItem();

        /// <summary>
        /// Call this method to start writing an array. When starting to serialize an array, call
        /// <see cref="WriteArrayStart"/>. Then, before writing any data for any item call
        /// <see cref="SetItemCount(long)"/> followed by a sequence of <see cref="StartItem"/> and
        /// the item itself. The number of <see cref="StartItem"/> should match the number specified
        /// in <see cref="SetItemCount(long)"/>. When actually writing the data of the item, you can
        /// call any <see cref="Encoder"/> method (e.g., <see cref="WriteLong(long)"/>). When all
        /// items of the array have been written, call <see cref="WriteArrayEnd"/>.
        /// <example>
        /// As an example, let's say you want to write an array of records, the record consisting
        /// of an Long field and a Boolean field. Your code would look something like this:
        /// <code>
        /// out.WriteArrayStart();
        /// out.SetItemCount(list.Count);
        /// foreach (var r in list)
        /// {
        ///     out.StartItem();
        ///     out.WriteLong(r.LongField);
        ///     out.WriteBoolean(r.BoolField);
        /// }
        /// out.WriteArrayEnd();
        /// </code>
        /// </example>
        /// </summary>
        void WriteArrayStart();

        /// <summary>
        /// Call this method to finish writing an array. See <see cref="WriteArrayStart"/> for usage
        /// information.
        /// </summary>
        void WriteArrayEnd();

        /// <summary>
        /// Call this to start a new map. See <see cref="WriteArrayStart"/> for details on usage.
        /// <example>
        /// As an example of usage, let's say you want to write a map of records, the record
        /// consisting of an Long field and a Boolean field. Your code would look something like
        /// this:
        /// <code>
        /// out.WriteMapStart();
        /// out.SetItemCount(dictionary.Count);
        /// foreach (var entry in dictionary)
        /// {
        ///     out.StartItem();
        ///     out.WriteString(entry.Key);
        ///     out.writeLong(entry.Value.LongField);
        ///     out.writeBoolean(entry.Value.BoolField);
        /// }
        /// out.WriteMapEnd();
        /// </code>
        /// </example>
        /// </summary>
        void WriteMapStart();

        /// <summary>
        /// Call this method to terminate the inner-most, currently-opened map. See
        /// <see cref="WriteArrayStart"/> for more details.
        /// </summary>
        void WriteMapEnd();

        /// <summary>
        /// Call this method to write the tag of a union.
        /// <example>
        /// As an example of usage, let's say you want to write a union, whose second branch is a
        /// record consisting of an Long field and a Boolean field. Your code would look something
        /// like this:
        /// <code>
        /// out.WriteIndex(1);
        /// out.WriteLong(record.LongField);
        /// out.WriteBoolean(record.BoolField);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="value"></param>
        void WriteUnionIndex(int value);

        /// <summary>
        /// Writes a fixed value.
        /// </summary>
        /// <param name="data">The contents to write.</param>
        void WriteFixed(byte[] data);

        /// <summary>
        /// Writes a fixed value.
        /// </summary>
        /// <param name="data">Contents to write.</param>
        /// <param name="start">Position within data where the contents start.</param>
        /// <param name="len">Number of bytes to write.</param>
        void WriteFixed(byte[] data, int start, int len);
    }
}

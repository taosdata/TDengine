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
using System.Collections.Generic;
using System.IO;

namespace Avro.IO
{
    /// <summary>
    /// Utility to collect data written to an <see cref="OutputStream"/> in
    /// <see cref="MemoryStream"/>s.
    /// </summary>
    /// <seealso cref="ByteBufferInputStream"/>
    public class ByteBufferOutputStream : OutputStream
    {
        /// <summary>
        /// Size of memory stream buffers.
        /// </summary>
        public const int BUFFER_SIZE = 8192;

        /// <summary>
        /// Initializes a new instance of the <see cref="ByteBufferOutputStream"/> class.
        /// </summary>
        public ByteBufferOutputStream()
        {
            Reset();
        }

        private void Reset()
        {
            _buffers = new List<MemoryStream> {CreateBuffer()};
        }

        private List<MemoryStream> _buffers;

        private static MemoryStream CreateBuffer()
        {
            return new MemoryStream(new byte[BUFFER_SIZE], 0, BUFFER_SIZE, true, true);
        }

        /// <summary>
        /// Prepends a list of <see cref="MemoryStream"/> to this stream.
        /// </summary>
        /// <param name="lists">Memory streams to prepend.</param>
        public void Prepend(List<MemoryStream> lists)
        {
            foreach (var stream in lists)
            {
                stream.Position = stream.Length;
            }

            _buffers.InsertRange(0, lists);
        }

        /// <summary>
        /// Appends a list of <see cref="MemoryStream"/> to this stream.
        /// </summary>
        /// <param name="lists">Memory streams to append.</param>
        public void Append(List<MemoryStream> lists)
        {
            foreach (var stream in lists)
            {
                stream.Position = stream.Length;
            }

            _buffers.AddRange(lists);
        }

        /// <inheritdoc/>
        public override void Write(byte[] b, int off, int len)
        {
            var buffer = _buffers[_buffers.Count -1];
            var remaining = (int) (buffer.Length - buffer.Position);
            while (len > remaining)
            {
                buffer.Write(b, off, remaining);
                len -= remaining;
                off += remaining;

                buffer = CreateBuffer();
                _buffers.Add(buffer);

                remaining = (int) buffer.Length;
            }

            buffer.Write(b, off, len);
        }

        /// <summary>
        /// Returns all data written and resets the stream to be empty.
        /// </summary>
        /// <returns>All memory stream data.</returns>
        public List<MemoryStream> GetBufferList()
        {
            List<MemoryStream> result = _buffers;

            Reset();

            foreach (MemoryStream b in result)
            {
                // Flip()
                b.SetLength(b.Position);
                b.Position = 0;
            }

            return result;
        }

        /// <inheritdoc/>
        public override long Length
        {
            get
            {
                long sum = 0;
                foreach (var buffer in _buffers)
                {
                    sum += buffer.Length;
                }

                return sum;
            }
        }

        /// <inheritdoc/>
        public override void Flush()
        {
        }
    }
}

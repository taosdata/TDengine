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

namespace Avro.File
{
    /// <summary>
    /// Encapsulates a block of data read by the <see cref="DataFileReader{T}"/>.
    /// We will remove this class from the public API in a future version because it is only meant
    /// to be used internally.
    /// </summary>
    [Obsolete("This will be removed from the public API in a future version.")]
    public class DataBlock
    {
        /// <summary>
        /// Raw bytes within this block.
        /// </summary>
        public byte[] Data { get;  set; }

        /// <summary>
        /// Number of entries in this block.
        /// </summary>
        public long NumberOfEntries { get; set; }

        /// <summary>
        /// Size of this block in bytes.
        /// </summary>
        public long BlockSize { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataBlock"/> class.
        /// </summary>
        /// <param name="numberOfEntries">Number of entries in this block.</param>
        /// <param name="blockSize">Size of this block in bytes.</param>
        public DataBlock(long numberOfEntries, long blockSize)
        {
            NumberOfEntries = numberOfEntries;
            BlockSize = blockSize;
            Data = new byte[blockSize];
        }

        internal Stream GetDataAsStream()
        {
            return new MemoryStream(Data);
        }
    }
}

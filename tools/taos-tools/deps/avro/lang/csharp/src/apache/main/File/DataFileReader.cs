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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;

namespace Avro.File
{
    /// <summary>
    /// Provides access to Avro data written using the <see cref="DataFileWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">Type to deserialze data objects to.</typeparam>
    public class DataFileReader<T> : IFileReader<T>
    {
        /// <summary>
        /// Defines the signature for a function that returns a new <see cref="DatumReader{T}"/>
        /// given a writer and reader schema.
        /// </summary>
        /// <param name="writerSchema">Schema used to write the datum.</param>
        /// <param name="readerSchema">Schema used to read the datum.</param>
        /// <returns>A datum reader.</returns>
        public delegate DatumReader<T> CreateDatumReader(Schema writerSchema, Schema readerSchema);

        private DatumReader<T> _reader;
        private Decoder _decoder, _datumDecoder;
        private Header _header;
        private Codec _codec;
        private DataBlock _currentBlock;
        private long _blockRemaining;
        private long _blockSize;
        private bool _availableBlock;
        private byte[] _syncBuffer;
        private long _blockStart;
        private Stream _stream;
        private bool _leaveOpen;
        private Schema _readerSchema;
        private readonly CreateDatumReader _datumReaderFactory;

        /// <summary>
        ///  Open a reader for a file using path
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static IFileReader<T> OpenReader(string path)
        {
            return OpenReader(new FileStream(path, FileMode.Open), null);
        }

        /// <summary>
        ///  Open a reader for a file using path and the reader's schema
        /// </summary>
        /// <param name="path">Path to the file</param>
        /// <param name="readerSchema">Schema used to read data from the file</param>
        /// <returns>A new file reader</returns>
        public static IFileReader<T> OpenReader(string path, Schema readerSchema)
        {
            return OpenReader(new FileStream(path, FileMode.Open), readerSchema);
        }

        /// <summary>
        ///  Open a reader for a stream
        /// </summary>
        /// <param name="inStream"></param>
        /// <returns></returns>
        public static IFileReader<T> OpenReader(Stream inStream)
        {
            return OpenReader(inStream, null);
        }

        /// <summary>
        ///  Open a reader for a stream
        /// </summary>
        /// <param name="inStream"></param>
        /// <param name="leaveOpen">Leave the stream open after disposing the object</param>
        /// <returns></returns>
        public static IFileReader<T> OpenReader(Stream inStream, bool leaveOpen)
        {
            return OpenReader(inStream, null, leaveOpen);
        }

        /// <summary>
        /// Open a reader for a stream using the reader's schema
        /// </summary>
        /// <param name="inStream">Stream containing the file contents</param>
        /// <param name="readerSchema">Schema used to read the file</param>
        /// <returns>A new file reader</returns>
        public static IFileReader<T> OpenReader(Stream inStream, Schema readerSchema)
        {
            return OpenReader(inStream, readerSchema, CreateDefaultReader);
        }

        /// <summary>
        /// Open a reader for a stream using the reader's schema
        /// </summary>
        /// <param name="inStream">Stream containing the file contents</param>
        /// <param name="readerSchema">Schema used to read the file</param>
        /// <param name="leaveOpen">Leave the stream open after disposing the object</param>
        /// <returns>A new file reader</returns>
        public static IFileReader<T> OpenReader(Stream inStream, Schema readerSchema, bool leaveOpen)
        {
            return OpenReader(inStream, readerSchema, CreateDefaultReader, leaveOpen);
        }
       
        /// <summary>
        ///  Open a reader for a stream using the reader's schema and a custom DatumReader
        /// </summary>
        /// <param name="inStream">Stream of file contents</param>
        /// <param name="readerSchema">Schema used to read the file</param>
        /// <param name="datumReaderFactory">Factory to create datum readers given a reader an writer schema</param>
        /// <returns>A new file reader</returns>
        public static IFileReader<T> OpenReader(Stream inStream, Schema readerSchema, CreateDatumReader datumReaderFactory)
        {
            return new DataFileReader<T>(inStream, readerSchema, datumReaderFactory, false);         // (not supporting 1.2 or below, format)
        }

        /// <summary>
        ///  Open a reader for a stream using the reader's schema and a custom DatumReader
        /// </summary>
        /// <param name="inStream">Stream of file contents</param>
        /// <param name="readerSchema">Schema used to read the file</param>
        /// <param name="datumReaderFactory">Factory to create datum readers given a reader an writer schema</param>
        /// <param name="leaveOpen">Leave the stream open after disposing the object</param>
        /// <returns>A new file reader</returns>
        public static IFileReader<T> OpenReader(Stream inStream, Schema readerSchema, CreateDatumReader datumReaderFactory, bool leaveOpen)
        {
            return new DataFileReader<T>(inStream, readerSchema, datumReaderFactory, leaveOpen);         // (not supporting 1.2 or below, format)
        }

        DataFileReader(Stream stream, Schema readerSchema, CreateDatumReader datumReaderFactory, bool leaveOpen)
        {
            _readerSchema = readerSchema;
            _datumReaderFactory = datumReaderFactory;
            _leaveOpen = leaveOpen;
            Init(stream);
            BlockFinished();
        }

        /// <inheritdoc/>
        public Header GetHeader()
        {
            return _header;
        }

        /// <inheritdoc/>
        public Schema GetSchema()
        {
            return _header.Schema;
        }

        /// <inheritdoc/>
        public ICollection<string> GetMetaKeys()
        {
            return _header.MetaData.Keys;
        }

        /// <inheritdoc/>
        public byte[] GetMeta(string key)
        {
            try
            {
                return _header.MetaData[key];
            }
            catch (KeyNotFoundException)
            {
                return null;
            }
        }

        /// <inheritdoc/>
        public long GetMetaLong(string key)
        {
            return long.Parse(GetMetaString(key), CultureInfo.InvariantCulture);
        }

        /// <inheritdoc/>
        public string GetMetaString(string key)
        {
            byte[] value = GetMeta(key);
            if (value == null)
            {
                return null;
            }
            try
            {
                return System.Text.Encoding.UTF8.GetString(value);
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(string.Format(CultureInfo.InvariantCulture,
                    "Error fetching meta data for key: {0}", key), e);
            }
        }

        /// <inheritdoc/>
        public void Seek(long position)
        {
            if (!_stream.CanSeek)
                throw new AvroRuntimeException("Not a valid input stream - must be seekable!");

            _stream.Position = position;
            _decoder = new BinaryDecoder(_stream);
            _datumDecoder = null;
            _blockRemaining = 0;
            _blockStart = position;
        }

        /// <inheritdoc/>
        public void Sync(long position)
        {
            Seek(position);
            // work around an issue where 1.5.4 C stored sync in metadata
            if ((position == 0) && (GetMeta(DataFileConstants.MetaDataSync) != null))
            {
                Init(_stream); // re-init to skip header
                return;
            }

            try
            {
                bool done = false;

                // read until sync mark matched
                do
                {
                    _decoder.ReadFixed(_syncBuffer);
                    if (Enumerable.SequenceEqual(_syncBuffer, _header.SyncData))
                    {
                        done = true;
                    }
                    else
                    {
                        _stream.Position -= DataFileConstants.SyncSize - 1;
                    }
                }
                while (!done);
            }
            catch
            {
                // could not find .. default to EOF
            }

            _blockStart = _stream.Position;
        }

        /// <inheritdoc/>
        public bool PastSync(long position)
        {
            return (_blockStart >= position + DataFileConstants.SyncSize) || (_blockStart >= _stream.Length);
        }

        /// <inheritdoc/>
        public long PreviousSync()
        {
            if (!_stream.CanSeek)
                throw new AvroRuntimeException("Not a valid input stream - must be seekable!");
            return _blockStart;
        }

        /// <inheritdoc/>
        public long Tell()
        {
            return _stream.Position;
        }

        /// <inheritdoc/>
        public IEnumerable<T> NextEntries
        {
            get
            {
                while (HasNext())
                {
                    yield return Next();
                }
            }
        }

        /// <inheritdoc/>
        public bool HasNext()
        {
            try
            {
                if (_blockRemaining == 0)
                {
                    // TODO: Check that the (block) stream is not partially read
                    /*if (_datumDecoder != null)
                    { }*/
                    if (HasNextBlock())
                    {
                        _currentBlock = NextRawBlock(_currentBlock);
                        _currentBlock.Data = _codec.Decompress(_currentBlock.Data);
                        _datumDecoder = new BinaryDecoder(_currentBlock.GetDataAsStream());
                    }
                }
                return _blockRemaining != 0;
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(string.Format(CultureInfo.InvariantCulture,
                    "Error fetching next object from block: {0}", e));
            }
        }

        /// <summary>
        /// Resets this reader.
        /// </summary>
        public void Reset()
        {
            Init(_stream);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases resources associated with this <see cref="DataFileReader{T}"/>.
        /// </summary>
        /// <param name="disposing">
        /// True if called from <see cref="Dispose()"/>; false otherwise.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_leaveOpen)
                _stream.Close();

            if (disposing && !_leaveOpen)
                _stream.Dispose();
        }

        private void Init(Stream stream)
        {
            _stream = stream;
            _header = new Header();
            _decoder = new BinaryDecoder(stream);
            _syncBuffer = new byte[DataFileConstants.SyncSize];

            // read magic
            byte[] firstBytes = new byte[DataFileConstants.Magic.Length];
            try
            {
                _decoder.ReadFixed(firstBytes);
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException("Not a valid data file!", e);
            }
            if (!firstBytes.SequenceEqual(DataFileConstants.Magic))
                throw new AvroRuntimeException("Not a valid data file!");

            // read meta data
            long len = _decoder.ReadMapStart();
            if (len > 0)
            {
                do
                {
                    for (long i = 0; i < len; i++)
                    {
                        string key = _decoder.ReadString();
                        byte[] val = _decoder.ReadBytes();
                        _header.MetaData.Add(key, val);
                    }
                } while ((len = _decoder.ReadMapNext()) != 0);
            }

            // read in sync data
            _decoder.ReadFixed(_header.SyncData);

            // parse schema and set codec
            _header.Schema = Schema.Parse(GetMetaString(DataFileConstants.MetaDataSchema));
            _reader = _datumReaderFactory(_header.Schema, _readerSchema ?? _header.Schema);
            _codec = ResolveCodec();
        }

        private static DatumReader<T> CreateDefaultReader(Schema writerSchema, Schema readerSchema)
        {
            DatumReader<T> reader = null;
            Type type = typeof(T);

            if (typeof(ISpecificRecord).IsAssignableFrom(type))
            {
                reader = new SpecificReader<T>(writerSchema, readerSchema);
            }
            else // generic
            {
                reader = new GenericReader<T>(writerSchema, readerSchema);
            }
            return reader;
        }

        private Codec ResolveCodec()
        {
            return Codec.CreateCodecFromString(GetMetaString(DataFileConstants.MetaDataCodec));
        }

        /// <inheritdoc/>
        public T Next()
        {
            return Next(default(T));
        }

        private T Next(T reuse)
        {
            try
            {
                if (!HasNext())
                    throw new AvroRuntimeException("No more datum objects remaining in block!");

                T result = _reader.Read(reuse, _datumDecoder);
                if (--_blockRemaining == 0)
                {
                    BlockFinished();
                }
                return result;
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(string.Format(CultureInfo.InvariantCulture,
                    "Error fetching next object from block: {0}", e));
            }
        }

        private void BlockFinished()
        {
            if (_stream.CanSeek)
                _blockStart = _stream.Position;
        }

        private DataBlock NextRawBlock(DataBlock reuse)
        {
            if (!HasNextBlock())
                throw new AvroRuntimeException("No data remaining in block!");

            if (reuse == null || reuse.Data.Length < _blockSize)
            {
                reuse = new DataBlock(_blockRemaining, _blockSize);
            }
            else
            {
                reuse.NumberOfEntries = _blockRemaining;
                reuse.BlockSize = _blockSize;
            }

            _decoder.ReadFixed(reuse.Data, 0, (int)reuse.BlockSize);
            _decoder.ReadFixed(_syncBuffer);

            if (!Enumerable.SequenceEqual(_syncBuffer, _header.SyncData))
                throw new AvroRuntimeException("Invalid sync!");

            _availableBlock = false;
            return reuse;
        }

        private bool DataLeft()
        {
            long currentPosition = _stream.Position;
            if (_stream.ReadByte() != -1)
                _stream.Position = currentPosition;
            else
                return false;

            return true;
        }

        private bool HasNextBlock()
        {
            try
            {
                // block currently being read
                if (_availableBlock)
                    return true;

                // check to ensure still data to read
                if (_stream.CanSeek)
                {
                    if (!DataLeft())
                        return false;

                    _blockRemaining = _decoder.ReadLong();      // read block count
                }
                else
                {
                    // when the stream is not seekable, the only way to know if there is still
                    // some data to read is to reach the end and raise an AvroException here.
                    try
                    {
                        _blockRemaining = _decoder.ReadLong();      // read block count
                    }
                    catch(AvroException)
                    {
                        return false;
                    }
                }

                _blockSize = _decoder.ReadLong();           // read block size
                if (_blockSize > System.Int32.MaxValue || _blockSize < 0)
                {
                    throw new AvroRuntimeException("Block size invalid or too large for this " +
                                                   "implementation: " + _blockSize);
                }
                _availableBlock = true;
                return true;
            }
            catch (Exception e)
            {
                throw new AvroRuntimeException(string.Format(CultureInfo.InvariantCulture,
                    "Error ascertaining if data has next block: {0}", e), e);
            }
        }

        /// <summary>
        /// Encapsulates a block of data read by the <see cref="DataFileReader{T}"/>.
        /// </summary>
        private class DataBlock
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
}

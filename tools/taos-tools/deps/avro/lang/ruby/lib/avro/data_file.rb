# frozen_string_literal: true
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'openssl'

module Avro
  module DataFile
    VERSION = 1
    MAGIC = "Obj" + [VERSION].pack('c')
    MAGIC.force_encoding('BINARY') if MAGIC.respond_to?(:force_encoding)
    MAGIC_SIZE = MAGIC.respond_to?(:bytesize) ? MAGIC.bytesize : MAGIC.size
    SYNC_SIZE = 16
    SYNC_INTERVAL = 4000 * SYNC_SIZE
    META_SCHEMA = Schema.parse('{"type": "map", "values": "bytes"}')
    VALID_ENCODINGS = ['binary'].freeze # not used yet

    class DataFileError < AvroError; end

    def self.open(file_path, mode='r', schema=nil, codec=nil)
      schema = Avro::Schema.parse(schema) if schema
      case mode
      when 'w'
        unless schema
          raise DataFileError, "Writing an Avro file requires a schema."
        end
        io = open_writer(File.open(file_path, 'wb'), schema, codec)
      when 'r'
        io = open_reader(File.open(file_path, 'rb'), schema)
      else
        raise DataFileError, "Only modes 'r' and 'w' allowed. You gave #{mode.inspect}."
      end

      yield io if block_given?
      io
    ensure
      io.close if block_given? && io
    end

    def self.codecs
      @codecs
    end

    def self.register_codec(codec)
      @codecs ||= {}
      codec = codec.new if !codec.respond_to?(:codec_name) && codec.is_a?(Class)
      @codecs[codec.codec_name.to_s] = codec
    end

    def self.get_codec(codec)
      codec ||= 'null'
      if codec.respond_to?(:compress) && codec.respond_to?(:decompress)
        codec # it's a codec instance
      elsif codec.is_a?(Class)
        codec.new # it's a codec class
      elsif @codecs.include?(codec.to_s)
        @codecs[codec.to_s] # it's a string or symbol (codec name)
      else
        raise DataFileError, "Unknown codec: #{codec.inspect}"
      end
    end

    class << self
      private
      def open_writer(file, schema, codec=nil)
        writer = Avro::IO::DatumWriter.new(schema)
        Avro::DataFile::Writer.new(file, writer, schema, codec)
      end

      def open_reader(file, schema)
        reader = Avro::IO::DatumReader.new(nil, schema)
        Avro::DataFile::Reader.new(file, reader)
      end
    end

    class Writer
      def self.generate_sync_marker
        OpenSSL::Random.random_bytes(16)
      end

      attr_reader :writer, :encoder, :datum_writer, :buffer_writer, :buffer_encoder, :sync_marker, :meta, :codec
      attr_accessor :block_count

      def initialize(writer, datum_writer, writers_schema=nil, codec=nil, meta={})
        # If writers_schema is not present, presume we're appending
        @writer = writer
        @encoder = IO::BinaryEncoder.new(@writer)
        @datum_writer = datum_writer
        @meta = meta
        @buffer_writer = StringIO.new(+'', 'w')
        @buffer_writer.set_encoding('BINARY') if @buffer_writer.respond_to?(:set_encoding)
        @buffer_encoder = IO::BinaryEncoder.new(@buffer_writer)
        @block_count = 0

        if writers_schema
          @sync_marker = Writer.generate_sync_marker
          @codec = DataFile.get_codec(codec)
          @meta['avro.codec'] = @codec.codec_name.to_s
          @meta['avro.schema'] = writers_schema.to_s
          datum_writer.writers_schema = writers_schema
          write_header
        else
          # open writer for reading to collect metadata
          dfr = Reader.new(writer, Avro::IO::DatumReader.new)

          # FIXME(jmhodges): collect arbitrary metadata
          # collect metadata
          @sync_marker = dfr.sync_marker
          @meta['avro.codec'] = dfr.meta['avro.codec']
          @codec = DataFile.get_codec(meta['avro.codec'])

          # get schema used to write existing file
          schema_from_file = dfr.meta['avro.schema']
          @meta['avro.schema'] = schema_from_file
          datum_writer.writers_schema = Schema.parse(schema_from_file)

          # seek to the end of the file and prepare for writing
          writer.seek(0,2)
        end
      end

      # Append a datum to the file
      def <<(datum)
        datum_writer.write(datum, buffer_encoder)
        self.block_count += 1

        # if the data to write is larger than the sync interval, write
        # the block
        if buffer_writer.tell >= SYNC_INTERVAL
          write_block
        end
      end

      # Return the current position as a value that may be passed to
      # DataFileReader.seek(long). Forces the end of the current block,
      # emitting a synchronization marker.
      def sync
        write_block
        writer.tell
      end

      # Flush the current state of the file, including metadata
      def flush
        write_block
        writer.flush
      end

      def close
        flush
        writer.close
      end

      private

      def write_header
        # write magic
        writer.write(MAGIC)

        # write metadata
        datum_writer.write_data(META_SCHEMA, meta, encoder)

        # write sync marker
        writer.write(sync_marker)
      end

      # TODO(jmhodges): make a schema for blocks and use datum_writer
      # TODO(jmhodges): do we really need the number of items in the block?
      def write_block
        if block_count > 0
          # write number of items in block and block size in bytes
          encoder.write_long(block_count)
          to_write = codec.compress(buffer_writer.string)
          encoder.write_long(to_write.respond_to?(:bytesize) ? to_write.bytesize : to_write.size)

          # write block contents
          writer.write(to_write)

          # write sync marker
          writer.write(sync_marker)

          # reset buffer
          buffer_writer.truncate(0)
          buffer_writer.rewind
          self.block_count = 0
        end
      end
    end

    # Read files written by DataFileWriter
    class Reader
      include ::Enumerable

      # The reader and binary decoder for the raw file stream
      attr_reader :reader, :decoder

      # The binary decoder for the contents of a block (after codec decompression)
      attr_reader :block_decoder

      attr_reader :datum_reader, :sync_marker, :meta, :file_length, :codec
      attr_accessor :block_count # records remaining in current block

      def initialize(reader, datum_reader)
        @reader = reader
        @decoder = IO::BinaryDecoder.new(reader)
        @datum_reader = datum_reader

        # read the header: magic, meta, sync
        read_header

        @codec = DataFile.get_codec(meta['avro.codec'])

        # get ready to read
        @block_count = 0
        datum_reader.writers_schema = Schema.parse meta['avro.schema']
      end

      # Iterates through each datum in this file
      # TODO(jmhodges): handle block of length zero
      def each
        loop do
          if block_count == 0
            case
            when eof?; break
            when skip_sync
              break if eof?
              read_block_header
            else
              read_block_header
            end
          end

          datum = datum_reader.read(block_decoder)
          self.block_count -= 1
          yield(datum)
        end
      end

      def eof?; reader.eof?; end

      def close
        reader.close
      end

      private
      def read_header
        # seek to the beginning of the file to get magic block
        reader.seek(0, 0)

        # check magic number
        magic_in_file = reader.read(MAGIC_SIZE)
        if magic_in_file.size < MAGIC_SIZE
          msg = 'Not an Avro data file: shorter than the Avro magic block'
          raise DataFileError, msg
        elsif magic_in_file != MAGIC
          msg = "Not an Avro data file: #{magic_in_file.inspect} doesn't match #{MAGIC.inspect}"
          raise DataFileError, msg
        end

        # read metadata
        @meta = datum_reader.read_data(META_SCHEMA,
                                       META_SCHEMA,
                                       decoder)
        # read sync marker
        @sync_marker = reader.read(SYNC_SIZE)
      end

      def read_block_header
        self.block_count = decoder.read_long
        block_bytes = decoder.read_long
        data = codec.decompress(reader.read(block_bytes))
        @block_decoder = IO::BinaryDecoder.new(StringIO.new(data))
      end

      # read the length of the sync marker; if it matches the sync
      # marker, return true. Otherwise, seek back to where we started
      # and return false
      def skip_sync
        proposed_sync_marker = reader.read(SYNC_SIZE)
        if proposed_sync_marker != sync_marker
          reader.seek(-SYNC_SIZE, 1)
          false
        else
          true
        end
      end
    end


    class NullCodec
      def codec_name; 'null'; end
      def decompress(data); data; end
      def compress(data); data; end
    end

    class DeflateCodec
      attr_reader :level

      def initialize(level=Zlib::DEFAULT_COMPRESSION)
        @level = level
      end

      def codec_name; 'deflate'; end

      def decompress(compressed)
        # Passing a negative number to Inflate puts it into "raw" RFC1951 mode
        # (without the RFC1950 header & checksum). See the docs for
        # inflateInit2 in https://www.zlib.net/manual.html
        zstream = Zlib::Inflate.new(-Zlib::MAX_WBITS)
        data = zstream.inflate(compressed)
        data << zstream.finish
      ensure
        zstream.close
      end

      def compress(data)
        zstream = Zlib::Deflate.new(level, -Zlib::MAX_WBITS)
        compressed = zstream.deflate(data)
        compressed << zstream.finish
      ensure
        zstream.close
      end
    end

    class SnappyCodec
      def codec_name; 'snappy'; end

      def decompress(data)
        load_snappy!
        crc32 = data.slice(-4..-1).unpack('N').first
        uncompressed = Snappy.inflate(data.slice(0..-5))

        if crc32 == Zlib.crc32(uncompressed)
          uncompressed
        else
          # older versions of avro-ruby didn't write the checksum, so if it
          # doesn't match this must assume that it wasn't there and return
          # the entire payload uncompressed.
          Snappy.inflate(data)
        end
      rescue Snappy::Error
        # older versions of avro-ruby didn't write the checksum, so removing
        # the last 4 bytes may cause Snappy to fail. recover by assuming the
        # payload is from an older file and uncompress the entire buffer.
        Snappy.inflate(data)
      end

      def compress(data)
        load_snappy!
        crc32 = Zlib.crc32(data)
        compressed = Snappy.deflate(data)
        [compressed, crc32].pack('a*N')
      end

      private

      def load_snappy!
        require 'snappy' unless defined?(Snappy)
      rescue LoadError
        raise LoadError, "Snappy compression is not available, please install the `snappy` gem."
      end
    end

    class ZstandardCodec
      def codec_name; 'zstandard'; end

      def decompress(data)
        load_zstandard!
        Zstd.decompress(data)
      end

      def compress(data)
        load_zstandard!
        Zstd.compress(data)
      end

      private

      def load_zstandard!
        require 'zstd-ruby' unless defined?(Zstd)
      rescue LoadError
        raise LoadError, "Zstandard compression is not available, please install the `zstd-ruby` gem."
      end
    end

    DataFile.register_codec NullCodec
    DataFile.register_codec DeflateCodec
    DataFile.register_codec SnappyCodec
    DataFile.register_codec ZstandardCodec

    # TODO this constant won't be updated if you register another codec.
    # Deprecated in favor of Avro::DataFile::codecs
    VALID_CODECS = DataFile.codecs.keys
  end
end

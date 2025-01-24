#!/usr/bin/env python3

##
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

"""
Contains Codecs for Python Avro.

Note that the word "codecs" means "compression/decompression algorithms" in the
Avro world (https://avro.apache.org/docs/current/spec.html#Object+Container+Files),
so don't confuse it with the Python's "codecs", which is a package mainly for
converting charsets (https://docs.python.org/3/library/codecs.html).
"""

import abc
import binascii
import io
import struct
import zlib
from typing import Dict, Tuple, Type

import avro.errors
import avro.io

#
# Constants
#
STRUCT_CRC32 = struct.Struct(">I")  # big-endian unsigned int


def _check_crc32(bytes_: bytes, checksum: bytes) -> None:
    if binascii.crc32(bytes_) & 0xFFFFFFFF != STRUCT_CRC32.unpack(checksum)[0]:
        raise avro.errors.AvroException("Checksum failure")


try:
    import bz2

    has_bzip2 = True
except ImportError:
    has_bzip2 = False
try:
    import snappy

    has_snappy = True
except ImportError:
    has_snappy = False
try:
    import zstandard as zstd

    has_zstandard = True
except ImportError:
    has_zstandard = False


class Codec(abc.ABC):
    """Abstract base class for all Avro codec classes."""

    @staticmethod
    @abc.abstractmethod
    def compress(data: bytes) -> Tuple[bytes, int]:
        """Compress the passed data.

        :param data: a byte string to be compressed
        :type data: str

        :rtype: tuple
        :return: compressed data and its length
        """

    @staticmethod
    @abc.abstractmethod
    def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
        """Read compressed data via the passed BinaryDecoder and decompress it.

        :param readers_decoder: a BinaryDecoder object currently being used for
                                reading an object container file
        :type readers_decoder: avro.io.BinaryDecoder

        :rtype: avro.io.BinaryDecoder
        :return: a newly instantiated BinaryDecoder object that contains the
                 decompressed data which is wrapped by a StringIO
        """


class NullCodec(Codec):
    @staticmethod
    def compress(data: bytes) -> Tuple[bytes, int]:
        return data, len(data)

    @staticmethod
    def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
        readers_decoder.skip_long()
        return readers_decoder


class DeflateCodec(Codec):
    @staticmethod
    def compress(data: bytes) -> Tuple[bytes, int]:
        # The first two characters and last character are zlib
        # wrappers around deflate data.
        compressed_data = zlib.compress(data)[2:-1]
        return compressed_data, len(compressed_data)

    @staticmethod
    def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
        # Compressed data is stored as (length, data), which
        # corresponds to how the "bytes" type is encoded.
        data = readers_decoder.read_bytes()
        # -15 is the log of the window size; negative indicates
        # "raw" (no zlib headers) decompression.  See zlib.h.
        uncompressed = zlib.decompress(data, -15)
        return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


if has_bzip2:

    class BZip2Codec(Codec):
        @staticmethod
        def compress(data: bytes) -> Tuple[bytes, int]:
            compressed_data = bz2.compress(data)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
            length = readers_decoder.read_long()
            data = readers_decoder.read(length)
            uncompressed = bz2.decompress(data)
            return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


if has_snappy:

    class SnappyCodec(Codec):
        @staticmethod
        def compress(data: bytes) -> Tuple[bytes, int]:
            compressed_data = snappy.compress(data)
            # A 4-byte, big-endian CRC32 checksum
            compressed_data += STRUCT_CRC32.pack(binascii.crc32(data) & 0xFFFFFFFF)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
            # Compressed data includes a 4-byte CRC32 checksum
            length = readers_decoder.read_long()
            data = readers_decoder.read(length - 4)
            uncompressed = snappy.decompress(data)
            checksum = readers_decoder.read(4)
            _check_crc32(uncompressed, checksum)
            return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


if has_zstandard:

    class ZstandardCodec(Codec):
        @staticmethod
        def compress(data: bytes) -> Tuple[bytes, int]:
            compressed_data = zstd.ZstdCompressor().compress(data)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
            length = readers_decoder.read_long()
            data = readers_decoder.read(length)
            uncompressed = bytearray()
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(io.BytesIO(data)) as reader:
                while True:
                    chunk = reader.read(16384)
                    if not chunk:
                        break
                    uncompressed.extend(chunk)
            return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


KNOWN_CODECS: Dict[str, Type[Codec]] = {
    name[:-5].lower(): class_
    for name, class_ in globals().items()
    if class_ != Codec and name.endswith("Codec") and isinstance(class_, type) and issubclass(class_, Codec)
}


def get_codec(codec_name: str) -> Type[Codec]:
    try:
        return KNOWN_CODECS[codec_name]
    except KeyError:
        raise avro.errors.UnsupportedCodec(f"Unsupported codec: {codec_name}. (Is it installed?)")

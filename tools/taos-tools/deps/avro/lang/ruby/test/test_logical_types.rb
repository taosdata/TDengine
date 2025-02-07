# -*- coding: utf-8 -*-
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

require 'test_help'
require 'memory_profiler'

class TestLogicalTypes < Test::Unit::TestCase
  def test_int_date
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "int", "logicalType": "date" }
    SCHEMA

    assert_equal 'date', schema.logical_type
    today = Date.today
    assert_encode_and_decode today, schema
    assert_preencoded Avro::LogicalTypes::IntDate.encode(today), schema, today
  end

  def test_int_date_conversion
    type = Avro::LogicalTypes::IntDate

    assert_equal 5, type.encode(Date.new(1970, 1, 6))
    assert_equal 0, type.encode(Date.new(1970, 1, 1))
    assert_equal(-5, type.encode(Date.new(1969, 12, 27)))

    assert_equal Date.new(1970, 1, 6), type.decode(5)
    assert_equal Date.new(1970, 1, 1), type.decode(0)
    assert_equal Date.new(1969, 12, 27), type.decode(-5)
  end

  def test_timestamp_millis_long
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "long", "logicalType": "timestamp-millis" }
    SCHEMA

    # The Time.at format is (seconds, microseconds) since Epoch.
    time = Time.at(628232400, 12000)

    assert_equal 'timestamp-millis', schema.logical_type
    assert_encode_and_decode time, schema
    assert_preencoded Avro::LogicalTypes::TimestampMillis.encode(time), schema, time.utc
  end

  def test_timestamp_millis_long_conversion
    type = Avro::LogicalTypes::TimestampMillis

    now = Time.now.utc
    now_millis = Time.utc(now.year, now.month, now.day, now.hour, now.min, now.sec, now.usec / 1000 * 1000)

    assert_equal now_millis, type.decode(type.encode(now_millis))
    assert_equal 1432849613221, type.encode(Time.utc(2015, 5, 28, 21, 46, 53, 221000))
    assert_equal 1432849613221, type.encode(DateTime.new(2015, 5, 28, 21, 46, 53.221))
    assert_equal Time.utc(2015, 5, 28, 21, 46, 53, 221000), type.decode(1432849613221)
  end

  def test_timestamp_micros_long
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "long", "logicalType": "timestamp-micros" }
    SCHEMA

    # The Time.at format is (seconds, microseconds) since Epoch.
    time = Time.at(628232400, 12345)

    assert_equal 'timestamp-micros', schema.logical_type
    assert_encode_and_decode time, schema
    assert_preencoded Avro::LogicalTypes::TimestampMicros.encode(time), schema, time.utc
  end

  def test_timestamp_micros_long_conversion
    type = Avro::LogicalTypes::TimestampMicros

    now = Time.now.utc

    assert_equal Time.utc(now.year, now.month, now.day, now.hour, now.min, now.sec, now.usec), type.decode(type.encode(now))
    assert_equal 1432849613221843, type.encode(Time.utc(2015, 5, 28, 21, 46, 53, 221843))
    assert_equal 1432849613221843, type.encode(DateTime.new(2015, 5, 28, 21, 46, 53.221843))
    assert_equal Time.utc(2015, 5, 28, 21, 46, 53, 221843), type.decode(1432849613221843)
  end

  def test_parse_fixed_duration
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "fixed", "size": 12, "name": "fixed_dur", "logicalType": "duration" }
    SCHEMA

    assert_equal 'duration', schema.logical_type
  end

  def test_bytes_decimal
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 6 }
    SCHEMA

    assert_equal 'decimal', schema.logical_type
    assert_equal 9, schema.precision
    assert_equal 6, schema.scale

    assert_encode_and_decode BigDecimal('-3.4562'), schema
    assert_encode_and_decode BigDecimal('3.4562'), schema
    assert_encode_and_decode 15.123, schema
    assert_encode_and_decode 15, schema
    assert_encode_and_decode BigDecimal('0.123456'), schema
    assert_encode_and_decode BigDecimal('0'), schema
    assert_encode_and_decode BigDecimal('1'), schema
    assert_encode_and_decode BigDecimal('-1'), schema

    assert_raise ArgumentError do
      type = Avro::LogicalTypes::BytesDecimal.new(schema)
      type.encode('1.23')
    end
  end

  def test_bytes_decimal_range_errors
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2 }
    SCHEMA

    type = Avro::LogicalTypes::BytesDecimal.new(schema)

    assert_raises RangeError do
      type.encode(BigDecimal('345'))
    end

    assert_raises RangeError do
      type.encode(BigDecimal('1.5342'))
    end

    assert_raises RangeError do
      type.encode(BigDecimal('-1.5342'))
    end

    assert_raises RangeError do
      type.encode(BigDecimal('-100.2'))
    end

    assert_raises RangeError do
      type.encode(BigDecimal('-99.991'))
    end
  end

  def test_bytes_decimal_conversion
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 6 }
    SCHEMA

    type = Avro::LogicalTypes::BytesDecimal.new(schema)

    enc = "\xcb\x43\x38".dup.force_encoding('BINARY')
    assert_equal enc, type.encode(BigDecimal('-3.4562'))
    assert_equal BigDecimal('-3.4562'), type.decode(enc)

    assert_equal "\x34\xbc\xc8".dup.force_encoding('BINARY'), type.encode(BigDecimal('3.4562'))
    assert_equal BigDecimal('3.4562'), type.decode("\x34\xbc\xc8".dup.force_encoding('BINARY'))

    assert_equal "\x6a\x33\x0e\x87\x00".dup.force_encoding('BINARY'), type.encode(BigDecimal('456123.123456'))
    assert_equal BigDecimal('456123.123456'), type.decode("\x6a\x33\x0e\x87\x00".dup.force_encoding('BINARY'))
  end

  def test_logical_type_with_schema
    exception = assert_raises(ArgumentError) do
      Avro::LogicalTypes::LogicalTypeWithSchema.new(nil)
    end
    assert_equal exception.to_s, 'schema is required'

    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 6 }
    SCHEMA

    assert_nothing_raised do
      Avro::LogicalTypes::LogicalTypeWithSchema.new(schema)
    end

    assert_raises NotImplementedError do
      Avro::LogicalTypes::LogicalTypeWithSchema.new(schema).encode(BigDecimal('2'))
    end

    assert_raises NotImplementedError do
      Avro::LogicalTypes::LogicalTypeWithSchema.new(schema).decode('foo')
    end
  end

  def test_bytes_decimal_object_allocations_encode
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2 }
    SCHEMA

    type = Avro::LogicalTypes::BytesDecimal.new(schema)

    positive_value = BigDecimal('5.2')
    negative_value = BigDecimal('-5.2')

    [positive_value, negative_value].each do |value|
      report = MemoryProfiler.report do
        type.encode(value)
      end

      assert_equal 5, report.total_allocated
      # Ruby 2.7 does not retain anything. Ruby 2.6 retains 1
      assert_operator 1, :>=, report.total_retained
    end
  end

  def test_bytes_decimal_object_allocations_decode
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2 }
    SCHEMA

    type = Avro::LogicalTypes::BytesDecimal.new(schema)

    positive_enc = "\x02\b".dup.force_encoding('BINARY')
    negative_enc = "\xFD\xF8".dup.force_encoding('BINARY')

    [positive_enc, negative_enc].each do |encoded|
      report = MemoryProfiler.report do
        type.decode(encoded)
      end

      assert_equal 5, report.total_allocated
      # Ruby 2.7 does not retain anything. Ruby 2.6 retains 1
      assert_operator 1, :>=, report.total_retained
    end
  end

  def encode(datum, schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)

    datum_writer = Avro::IO::DatumWriter.new(schema)
    datum_writer.write(datum, encoder)

    buffer.string
  end

  def decode(encoded, schema)
    buffer = StringIO.new(encoded)
    decoder = Avro::IO::BinaryDecoder.new(buffer)

    datum_reader = Avro::IO::DatumReader.new(schema, schema)
    datum_reader.read(decoder)
  end

  def assert_encode_and_decode(datum, schema)
    encoded = encode(datum, schema)
    assert_equal datum, decode(encoded, schema)
  end

  def assert_preencoded(datum, schema, decoded)
    encoded = encode(datum, schema)
    assert_equal decoded, decode(encoded, schema)
  end
end

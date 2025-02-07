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

class TestDataFile < Test::Unit::TestCase
  HERE = File.expand_path File.dirname(__FILE__)
  def setup
    if File.exist?(HERE + '/data.avr')
      File.unlink(HERE + '/data.avr')
    end
  end

  def teardown
    if File.exist?(HERE + '/data.avr')
      File.unlink(HERE + '/data.avr')
    end
  end

  def test_differing_schemas_with_primitives
    writer_schema = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "username", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "verified", "type": "boolean", "default": false}
  ]}
JSON

    data = [{"username" => "john", "age" => 25, "verified" => true},
            {"username" => "ryan", "age" => 23, "verified" => false}]

    Avro::DataFile.open('data.avr', 'w', writer_schema) do |dw|
      data.each{|h| dw << h }
    end

    # extract the username only from the avro serialized file
    reader_schema = <<-JSON
{ "type": "record",
  "name": "User",
  "fields" : [
    {"name": "username", "type": "string"}
 ]}
JSON

    Avro::DataFile.open('data.avr', 'r', reader_schema) do |dr|
      dr.each_with_index do |record, i|
        assert_equal data[i]['username'], record['username']
      end
    end
  end

  def test_differing_schemas_with_complex_objects
    writer_schema = <<-JSON
{ "type": "record",
  "name": "something",
  "fields": [
    {"name": "something_fixed", "type": {"name": "inner_fixed",
                                         "type": "fixed", "size": 3}},
    {"name": "something_enum", "type": {"name": "inner_enum",
                                        "type": "enum",
                                        "symbols": ["hello", "goodbye"]}},
    {"name": "something_array", "type": {"type": "array", "items": "int"}},
    {"name": "something_map", "type": {"type": "map", "values": "int"}},
    {"name": "something_record", "type": {"name": "inner_record",
                                          "type": "record",
                                          "fields": [
                                            {"name": "inner", "type": "int"}
                                          ]}},
    {"name": "username", "type": "string"}
]}
JSON

    data = [{"username" => "john",
              "something_fixed" => "foo",
              "something_enum" => "hello",
              "something_array" => [1,2,3],
              "something_map" => {"a" => 1, "b" => 2},
              "something_record" => {"inner" => 2},
              "something_error" => {"code" => 403}
            },
            {"username" => "ryan",
              "something_fixed" => "bar",
              "something_enum" => "goodbye",
              "something_array" => [1,2,3],
              "something_map" => {"a" => 2, "b" => 6},
              "something_record" => {"inner" => 1},
              "something_error" => {"code" => 401}
            }]

    Avro::DataFile.open('data.avr', 'w', writer_schema) do |dw|
      data.each{|d| dw << d }
    end

    %w[fixed enum record error array map union].each do |s|
      reader = MultiJson.load(writer_schema)
      reader['fields'] = reader['fields'].reject{|f| f['type']['type'] == s}
      Avro::DataFile.open('data.avr', 'r', MultiJson.dump(reader)) do |dr|
        dr.each_with_index do |obj, i|
          reader['fields'].each do |field|
            assert_equal data[i][field['name']], obj[field['name']]
          end
        end
      end
    end
  end

  def test_data_writer_handles_sync_interval
    writer_schema = <<-JSON
{ "type": "record",
  "name": "something",
  "fields": [
    {"name": "something_boolean", "type": "boolean"}
]}
JSON

    data = {"something_boolean" => true }

    Avro::DataFile.open('data.avr', 'w', writer_schema) do |dw|
      while dw.writer.tell < Avro::DataFile::SYNC_INTERVAL
        dw << data
      end
      block_count = dw.block_count
      dw << data
      # ensure we didn't just write another block
      assert_equal(block_count+1, dw.block_count)
    end
  end

  def test_utf8
    datafile = Avro::DataFile::open('data.avr', 'w', '"string"')
    datafile << "家"
    datafile.close

    datafile = Avro::DataFile.open('data.avr')
    datafile.each do |s|
      assert_equal "家", s
    end
    datafile.close
  end

  def test_deflate
    Avro::DataFile.open('data.avr', 'w', '"string"', :deflate) do |writer|
      writer << 'a' * 10_000
    end
    assert(File.size('data.avr') < 500)

    records = []
    Avro::DataFile.open('data.avr') do |reader|
      reader.each {|record| records << record }
    end
    assert_equal records, ['a' * 10_000]
  end

  def test_snappy
    Avro::DataFile.open('data.avr', 'w', '"string"', :snappy) do |writer|
      writer << 'a' * 10_000
    end
    assert(File.size('data.avr') < 600)

    records = []
    Avro::DataFile.open('data.avr') do |reader|
      reader.each {|record| records << record }
    end
    assert_equal records, ['a' * 10_000]
  end

  def test_zstandard
    Avro::DataFile.open('data.avr', 'w', '"string"', :zstandard) do |writer|
      writer << 'a' * 10_000
    end
    assert(File.size('data.avr') < 600)

    records = []
    Avro::DataFile.open('data.avr') do |reader|
      reader.each {|record| records << record }
    end
    assert_equal records, ['a' * 10_000]
  end

  def test_append_to_deflated_file
    schema = Avro::Schema.parse('"string"')
    writer = Avro::IO::DatumWriter.new(schema)
    file = Avro::DataFile::Writer.new(File.open('data.avr', 'wb'), writer, schema, :deflate)
    file << 'a' * 10_000
    file.close

    file = Avro::DataFile::Writer.new(File.open('data.avr', 'a+b'), writer)
    file << 'b' * 10_000
    file.close
    assert(File.size('data.avr') < 1_000)

    records = []
    Avro::DataFile.open('data.avr') do |reader|
      reader.each {|record| records << record }
    end
    assert_equal records, ['a' * 10_000, 'b' * 10_000]
  end

  def test_custom_meta
    meta = { 'x.greeting' => 'yo' }

    schema = Avro::Schema.parse('"string"')
    writer = Avro::IO::DatumWriter.new(schema)
    file = Avro::DataFile::Writer.new(File.open('data.avr', 'wb'), writer, schema, nil, meta)
    file.close

    Avro::DataFile.open('data.avr') do |reader|
      assert_equal 'yo', reader.meta['x.greeting']
    end
  end
end

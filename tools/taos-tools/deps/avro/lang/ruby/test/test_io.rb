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

class TestIO < Test::Unit::TestCase
  DATAFILE = 'tmp/test.rb.avro'
  Schema = Avro::Schema

  def test_null
    check('"null"')
    check_default('"null"', "null", nil)
  end

  def test_boolean
    check('"boolean"')
    check_default('"boolean"', "true", true)
    check_default('"boolean"', "false", false)
  end

  def test_string
    check('"string"')
    check_default('"string"', '"foo"', "foo")
  end

  def test_bytes
    check('"bytes"')
    check_default('"bytes"', '"foo"', "foo")
  end

  def test_int
    check('"int"')
    check_default('"int"', "5", 5)
  end

  def test_long
    check('"long"')
    check_default('"long"', "9", 9)
  end

  def test_float
    check('"float"')
    check_default('"float"', "1.2", 1.2)
  end

  def test_double
    check('"double"')
    check_default('"double"', "1.2", 1.2)
  end

  def test_array
    array_schema = '{"type": "array", "items": "long"}'
    check(array_schema)
    check_default(array_schema, "[1]", [1])
  end

  def test_map
    map_schema = '{"type": "map", "values": "long"}'
    check(map_schema)
    check_default(map_schema, '{"a": 1}', {"a" => 1})
  end

  def test_record
    record_schema = <<EOS
      {"type": "record",
       "name": "Test",
       "fields": [{"name": "f",
                   "type": "long"}]}
EOS
    check(record_schema)
    check_default(record_schema, '{"f": 11}', {"f" => 11})
  end

  def test_record_with_logical_type
    record_schema = <<EOS
      {"type": "record",
       "name": "Test",
       "fields": [{"name": "ts",
                   "type": {"type": "long",
                            "logicalType": "timestamp-micros"}},
                  {"name": "ts2",
                   "type": {"type": "long",
                            "logicalType": "timestamp-millis"}}]}
EOS
    check(record_schema)
  end

  def test_error
    error_schema = <<EOS
      {"type": "error",
       "name": "TestError",
       "fields": [{"name": "message",
                   "type": "string"}]}
EOS
    check(error_schema)
    check_default(error_schema, '{"message": "boom"}', {"message" => "boom"})
  end

  def test_enum
    enum_schema = '{"type": "enum", "name": "Test","symbols": ["A", "B"]}'
    check(enum_schema)
    check_default(enum_schema, '"B"', "B")
  end

  def test_enum_with_default
    enum_schema = '{"type": "enum", "name": "Test", "symbols": ["A", "B"], "default": "A"}'
    check(enum_schema)
    # Field default is used for missing field.
    check_default(enum_schema, '"B"', "B")
  end

  def test_recursive
    recursive_schema = <<EOS
      {"type": "record",
       "name": "Node",
       "fields": [{"name": "label", "type": "string"},
                  {"name": "children",
                   "type": {"type": "array", "items": "Node"}}]}
EOS
    check(recursive_schema)
  end

  def test_union
    union_schema = <<EOS
      ["string",
       {"type": "int", "logicalType": "date"},
       "null",
       "long",
       {"type": "record",
        "name": "Cons",
        "fields": [{"name": "car", "type": "string"},
                   {"name": "cdr", "type": "string"}]}]
EOS
    check(union_schema)
    check_default('["double", "long"]', "1.1", 1.1)
  end

  def test_lisp
    lisp_schema = <<EOS
      {"type": "record",
       "name": "Lisp",
       "fields": [{"name": "value",
                   "type": ["null", "string",
                            {"type": "record",
                             "name": "Cons",
                             "fields": [{"name": "car", "type": "Lisp"},
                                        {"name": "cdr", "type": "Lisp"}]}]}]}
EOS
    check(lisp_schema)
  end

  def test_fixed
    fixed_schema = '{"type": "fixed", "name": "Test", "size": 1}'
    check(fixed_schema)
    check_default(fixed_schema, '"a"', "a")
  end

  def test_record_variable_key_types
    datum = { sym: "foo", "str"=>"bar"}
    ret_val = { "sym"=> "foo", "str"=>"bar"}
    schema = Schema.parse('{"type":"record", "name":"rec", "fields":[{"name":"sym", "type":"string"}, {"name":"str", "type":"string"}]}')

    writer, _encoder, _datum_writer = write_datum(datum, schema)

    ret_datum = read_datum(writer, schema)
    assert_equal ret_datum, ret_val
  end

  def test_record_with_nil
    schema = Avro::Schema.parse('{"type":"record", "name":"rec", "fields":[{"type":"int", "name":"i"}]}')
    assert_raise(Avro::IO::AvroTypeError) do
      write_datum(nil, schema)
    end
  end

  def test_array_with_nil
    schema = Avro::Schema.parse('{"type":"array", "items":"int"}')
    assert_raise(Avro::IO::AvroTypeError) do
      write_datum(nil, schema)
    end
  end

  def test_map_with_nil
    schema = Avro::Schema.parse('{"type":"map", "values":"long"}')
    assert_raise(Avro::IO::AvroTypeError) do
      write_datum(nil, schema)
    end
  end

  def test_enum_with_duplicate
    str = '{"type": "enum", "name": "Test","symbols" : ["AA", "AA"]}'
    assert_raises(Avro::SchemaParseError.new('Duplicate symbol: ["AA", "AA"]')) do
      Avro::Schema.parse str
    end
  end

  BINARY_INT_ENCODINGS = [
    [0, '00'],
    [-1, '01'],
    [1, '02'],
    [-2, '03'],
    [2, '04'],
    [-64, '7f'],
    [64, '80 01'],
    [8192, '80 80 01'],
    [-8193, '81 80 01'],
  ].freeze

  def avro_hexlify(reader)
    bytes = []
    current_byte = reader.read(1)
    bytes << hexlify(current_byte)
    while (current_byte.unpack('C').first & 0x80) != 0
      current_byte = reader.read(1)
      bytes << hexlify(current_byte)
    end
    bytes.join ' '
  end

  def hexlify(msg)
    msg.unpack("H*")
  end

  def test_binary_int_encoding
    for value, hex_encoding in BINARY_INT_ENCODINGS
      # write datum in binary to string buffer
      buffer = StringIO.new
      encoder = Avro::IO::BinaryEncoder.new(buffer)
      datum_writer = Avro::IO::DatumWriter.new(Avro::Schema.parse('"int"'))
      datum_writer.write(value, encoder)

      buffer.seek(0)
      hex_val = avro_hexlify(buffer)

      assert_equal hex_encoding, hex_val
    end
  end

  def test_binary_long_encoding
    for value, hex_encoding in BINARY_INT_ENCODINGS
      buffer = StringIO.new
      encoder = Avro::IO::BinaryEncoder.new(buffer)
      datum_writer = Avro::IO::DatumWriter.new(Avro::Schema.parse('"long"'))
      datum_writer.write(value, encoder)

      # read it out of the buffer and hexlify it
      buffer.seek(0)
      hex_val = avro_hexlify(buffer)

      assert_equal hex_encoding, hex_val
    end
  end

  def test_utf8_string_encoding
    [
      String.new("\xC3", encoding: 'ISO-8859-1'),
      String.new("\xC3\x83", encoding: 'UTF-8')
    ].each do |value|
      output = String.new('', encoding: 'BINARY')
      encoder = Avro::IO::BinaryEncoder.new(StringIO.new(output))
      datum_writer = Avro::IO::DatumWriter.new(Avro::Schema.parse('"string"'))
      datum_writer.write(value, encoder)

      assert_equal String.new("\x04\xc3\x83", encoding: 'BINARY'), output
    end
  end

  def test_bytes_encoding
    [
      String.new("\xC3\x83", encoding: 'BINARY'),
      String.new("\xC3\x83", encoding: 'ISO-8859-1'),
      String.new("\xC3\x83", encoding: 'UTF-8')
    ].each do |value|
      output = String.new('', encoding: 'BINARY')
      encoder = Avro::IO::BinaryEncoder.new(StringIO.new(output))
      datum_writer = Avro::IO::DatumWriter.new(Avro::Schema.parse('"bytes"'))
      datum_writer.write(value, encoder)

      assert_equal String.new("\x04\xc3\x83", encoding: 'BINARY'), output
    end
  end

  def test_fixed_encoding
    [
      String.new("\xC3\x83", encoding: 'BINARY'),
      String.new("\xC3\x83", encoding: 'ISO-8859-1'),
      String.new("\xC3\x83", encoding: 'UTF-8')
    ].each do |value|
      output = String.new('', encoding: 'BINARY')
      encoder = Avro::IO::BinaryEncoder.new(StringIO.new(output))
      schema = '{"type": "fixed", "name": "TwoBytes", "size": 2}'
      datum_writer = Avro::IO::DatumWriter.new(Avro::Schema.parse(schema))
      datum_writer.write(value, encoder)

      assert_equal String.new("\xC3\x83", encoding: 'BINARY'), output
    end
  end

  def test_skip_long
    for value_to_skip, _hex_encoding in BINARY_INT_ENCODINGS
      value_to_read = 6253

      # write some data in binary to string buffer
      writer = StringIO.new
      encoder = Avro::IO::BinaryEncoder.new(writer)
      datum_writer = Avro::IO::DatumWriter.new(Avro::Schema.parse('"long"'))
      datum_writer.write(value_to_skip, encoder)
      datum_writer.write(value_to_read, encoder)

      # skip the value
      reader = StringIO.new(writer.string())
      decoder = Avro::IO::BinaryDecoder.new(reader)
      decoder.skip_long()

      # read data from string buffer
      datum_reader = Avro::IO::DatumReader.new(Avro::Schema.parse('"long"'))
      read_value = datum_reader.read(decoder)

      # check it
      assert_equal value_to_read, read_value
    end
  end

  def test_skip_int
    for value_to_skip, _hex_encoding in BINARY_INT_ENCODINGS
      value_to_read = 6253

      writer = StringIO.new
      encoder = Avro::IO::BinaryEncoder.new(writer)
      datum_writer = Avro::IO::DatumWriter.new(Avro::Schema.parse('"int"'))
      datum_writer.write(value_to_skip, encoder)
      datum_writer.write(value_to_read, encoder)

      reader = StringIO.new(writer.string)
      decoder = Avro::IO::BinaryDecoder.new(reader)
      decoder.skip_int

      datum_reader = Avro::IO::DatumReader.new(Avro::Schema.parse('"int"'))
      read_value = datum_reader.read(decoder)

      assert_equal value_to_read, read_value
    end
  end

  def test_skip_union
    ["hello", -1, 32, nil].each do |value_to_skip|
      value_to_read = 6253

      schema = Avro::Schema.parse('["int", "string", "null"]')
      writer = StringIO.new
      encoder = Avro::IO::BinaryEncoder.new(writer)
      datum_writer = Avro::IO::DatumWriter.new(schema)
      datum_writer.write(value_to_skip, encoder)
      datum_writer.write(value_to_read, encoder)

      reader = StringIO.new(writer.string)
      decoder = Avro::IO::BinaryDecoder.new(reader)
      datum_reader = Avro::IO::DatumReader.new(schema)
      datum_reader.skip_data(schema, decoder)
      read_value = datum_reader.read(decoder)

      assert_equal value_to_read, read_value
    end
  end


  def test_schema_promotion
    promotable_schemas = ['"int"', '"long"', '"float"', '"double"']
    incorrect = 0
    promotable_schemas.each_with_index do |ws, i|
      writers_schema = Avro::Schema.parse(ws)
      datum_to_write = 219
      for rs in promotable_schemas[(i + 1)..-1]
        readers_schema = Avro::Schema.parse(rs)
        writer, _enc, _dw = write_datum(datum_to_write, writers_schema)
        datum_read = read_datum(writer, writers_schema, readers_schema)
        if datum_read != datum_to_write
          incorrect += 1
        end
      end
      assert_equal(incorrect, 0)
    end
  end

  def test_interchangeable_schemas
    interchangeable_schemas = ['"string"', '"bytes"']
    incorrect = 0
    interchangeable_schemas.each_with_index do |ws, i|
      writers_schema = Avro::Schema.parse(ws)
      datum_to_write = 'foo'
      readers_schema = Avro::Schema.parse(interchangeable_schemas[i == 0 ? 1 : 0])
      writer, * = write_datum(datum_to_write, writers_schema)
      datum_read = read_datum(writer, writers_schema, readers_schema)
      if datum_read != datum_to_write
        incorrect += 1
      end
    end
    assert_equal(incorrect, 0)
  end

  def test_unknown_enum_symbol
    writers_schema = Avro::Schema.parse(<<-SCHEMA)
      {
        "type": "enum",
        "name": "test",
        "symbols": ["B", "C"]
      }
    SCHEMA
    readers_schema = Avro::Schema.parse(<<-SCHEMA)
      {
        "type": "enum",
        "name": "test",
        "symbols": ["A", "B"]
      }
    SCHEMA
    datum_to_write = "C"
    writer, * = write_datum(datum_to_write, writers_schema)
    datum_read = read_datum(writer, writers_schema, readers_schema)
    # Ruby implementation did not follow the spec and returns the writer's symbol here
    assert_equal(datum_read, datum_to_write)
  end

  def test_unknown_enum_symbol_with_enum_default
    writers_schema = Avro::Schema.parse(<<-SCHEMA)
      {
        "type": "enum",
        "name": "test",
        "symbols": ["B", "C"]
      }
    SCHEMA
    readers_schema = Avro::Schema.parse(<<-SCHEMA)
      {
        "type": "enum",
        "name": "test",
        "symbols": ["A", "B", "UNKNOWN"],
        "default": "UNKNOWN"
      }
    SCHEMA
    datum_to_write = "C"
    writer, * = write_datum(datum_to_write, writers_schema)
    datum_read = read_datum(writer, writers_schema, readers_schema)
    assert_equal(datum_read, "UNKNOWN")
  end

  def test_array_schema_promotion
    writers_schema = Avro::Schema.parse('{"type":"array", "items":"int"}')
    readers_schema = Avro::Schema.parse('{"type":"array", "items":"long"}')
    datum_to_write = [1, 2]
    writer, * = write_datum(datum_to_write, writers_schema)
    datum_read = read_datum(writer, writers_schema, readers_schema)
    assert_equal(datum_read, datum_to_write)
  end

  def test_map_schema_promotion
    writers_schema = Avro::Schema.parse('{"type":"map", "values":"int"}')
    readers_schema = Avro::Schema.parse('{"type":"map", "values":"long"}')
    datum_to_write = { 'foo' => 1, 'bar' => 2 }
    writer, * = write_datum(datum_to_write, writers_schema)
    datum_read = read_datum(writer, writers_schema, readers_schema)
    assert_equal(datum_read, datum_to_write)
  end

  def test_aliased
    writers_schema = Avro::Schema.parse(<<-SCHEMA)
      {"type":"record", "name":"Rec1", "fields":[
        {"name":"field1", "type":"int"}
      ]}
    SCHEMA
    readers_schema = Avro::Schema.parse(<<-SCHEMA)
      {"type":"record", "name":"Rec2", "aliases":["Rec1"], "fields":[
        {"name":"field2", "aliases":["field1"], "type":"int"}
      ]}
    SCHEMA
    writer, * = write_datum({ 'field1' => 1 }, writers_schema)
    datum_read = read_datum(writer, writers_schema, readers_schema)
    assert_equal(datum_read, { 'field2' => 1 })
  end

  def test_big_decimal_datum_for_float
    writers_schema = Avro::Schema.parse('"float"')
    writer, * = write_datum(BigDecimal('1.2'), writers_schema)
    datum_read = read_datum(writer, writers_schema)
    assert_in_delta(1.2, datum_read)
  end

  def test_big_decimal_datum_for_double
    writers_schema = Avro::Schema.parse('"double"')
    writer, * = write_datum(BigDecimal("1.2"), writers_schema)
    datum_read = read_datum(writer, writers_schema)
    assert_in_delta(1.2, datum_read)
  end

  def test_snappy_backward_compat
    # a snappy-compressed block payload without the checksum
    # this has no back-references, just one literal so the last 9
    # bytes are the uncompressed payload.
    old_snappy_bytes = "\x09\x20\x02\x06\x02\x0a\x67\x72\x65\x65\x6e"
    uncompressed_bytes = "\x02\x06\x02\x0a\x67\x72\x65\x65\x6e"
    snappy = Avro::DataFile::SnappyCodec.new
    assert_equal(uncompressed_bytes, snappy.decompress(old_snappy_bytes))
  end

  private

  def check_no_default(schema_json)
    actual_schema = '{"type": "record", "name": "Foo", "fields": []}'
    actual = Avro::Schema.parse(actual_schema)

    expected_schema = <<EOS
      {"type": "record",
       "name": "Foo",
       "fields": [{"name": "f", "type": #{schema_json}}]}
EOS
    expected = Avro::Schema.parse(expected_schema)

    reader = Avro::IO::DatumReader.new(actual, expected)
    assert_raise Avro::AvroError do
      value = reader.read(Avro::IO::BinaryDecoder.new(StringIO.new))
      assert_not_equal(value, :no_default) # should never return this
    end
  end

  def check_default(schema_json, default_json, default_value)
    actual_schema = '{"type": "record", "name": "Foo", "fields": []}'
    actual = Avro::Schema.parse(actual_schema)

    expected_schema = <<EOS
      {"type": "record",
       "name": "Foo",
       "fields": [{"name": "f", "type": #{schema_json}, "default": #{default_json}}]}
EOS
    expected = Avro::Schema.parse(expected_schema)

    reader = Avro::IO::DatumReader.new(actual, expected)
    record = reader.read(Avro::IO::BinaryDecoder.new(StringIO.new))
    assert_equal default_value, record["f"]
  end

  def check(str)
    # parse schema, then convert back to string
    schema = Avro::Schema.parse str

    parsed_string = schema.to_s

     # test that the round-trip didn't mess up anything
    # NB: I don't think we should do this. Why enforce ordering?
    assert_equal(MultiJson.load(str),
                  MultiJson.load(parsed_string))

    # test __eq__
    assert_equal(schema, Avro::Schema.parse(str))

    # test hashcode doesn't generate infinite recursion
    schema.hash

    # test serialization of random data
    randomdata = RandomData.new(schema)
    9.times { checkser(schema, randomdata) }

    # test writing of data to file
    check_datafile(schema)

    # check that AvroError is raised when there is no default
    check_no_default(str)
  end

  def checkser(schm, randomdata)
    datum = randomdata.next
    assert validate(schm, datum), 'datum is not valid for schema'
    w = Avro::IO::DatumWriter.new(schm)
    writer = StringIO.new(+"", "w")
    w.write(datum, Avro::IO::BinaryEncoder.new(writer))
    r = datum_reader(schm)
    reader = StringIO.new(writer.string)
    ob = r.read(Avro::IO::BinaryDecoder.new(reader))
    assert_equal(datum, ob) # FIXME check on assertdata conditional
  end

  def check_datafile(schm)
    seed = 0
    count = 10
    random_data = RandomData.new(schm, seed)


    f = File.open(DATAFILE, 'wb')
    dw = Avro::DataFile::Writer.new(f, datum_writer(schm), schm)
    count.times{ dw << random_data.next }
    dw.close

    random_data = RandomData.new(schm, seed)


    f = File.open(DATAFILE, 'r+')
    dr = Avro::DataFile::Reader.new(f, datum_reader(schm))

    last_index = nil
    dr.each_with_index do |data, c|
      last_index = c
      # FIXME assertdata conditional
      assert_equal(random_data.next, data)
    end
    dr.close
    assert_equal count, last_index+1
  end

  def validate(schm, datum)
    Avro::Schema.validate(schm, datum)
  end

  def datum_writer(schm)
    Avro::IO::DatumWriter.new(schm)
  end

  def datum_reader(schm)
    Avro::IO::DatumReader.new(schm)
  end

  def write_datum(datum, writers_schema)
    writer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(writer)
    datum_writer = Avro::IO::DatumWriter.new(writers_schema)
    datum_writer.write(datum, encoder)
    [writer, encoder, datum_writer]
  end

  def read_datum(buffer, writers_schema, readers_schema=nil)
    reader = StringIO.new(buffer.string)
    decoder = Avro::IO::BinaryDecoder.new(reader)
    datum_reader = Avro::IO::DatumReader.new(writers_schema, readers_schema)
    datum_reader.read(decoder)
  end
end

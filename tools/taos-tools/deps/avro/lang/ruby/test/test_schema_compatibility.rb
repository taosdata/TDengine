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

class TestSchemaCompatibility < Test::Unit::TestCase

  def test_primitive_schema_compatibility
    Avro::Schema::PRIMITIVE_TYPES.each do |schema_type|
      assert_true(can_read?(send("#{schema_type}_schema"), send("#{schema_type}_schema")))
    end
  end

  def test_compatible_reader_writer_pairs
    cached_schema = a_int_record1_schema
    [
      cached_schema, cached_schema,
      long_schema, int_schema,
      float_schema, int_schema,
      float_schema, long_schema,
      double_schema, long_schema,
      double_schema, int_schema,
      double_schema, float_schema,

      int_array_schema, int_array_schema,
      long_array_schema, int_array_schema,
      int_map_schema, int_map_schema,
      long_map_schema, int_map_schema,

      enum1_ab_schema, enum1_ab_schema,
      enum1_ab_aliased_schema, enum1_ab_schema,
      enum1_abc_schema, enum1_ab_schema,
      enum1_ab_default_schema, enum1_abc_schema,

      fixed1_schema, fixed1_schema,
      fixed1_aliased_schema, fixed1_schema,

      string_schema, bytes_schema,
      bytes_schema, string_schema,

      empty_union_schema, empty_union_schema,
      int_union_schema, int_union_schema,
      int_string_union_schema, string_int_union_schema,
      int_union_schema, empty_union_schema,
      long_union_schema, int_union_schema,

      int_union_schema, int_schema,
      int_schema, int_union_schema,

      empty_record1_schema, empty_record1_schema,
      empty_record1_schema, a_int_record1_schema,
      empty_record1_aliased_schema, empty_record1_schema,

      a_int_record1_schema, a_int_record1_schema,
      a_dint_record1_schema, a_int_record1_schema,
      a_dint_record1_schema, a_dint_record1_schema,
      a_int_record1_schema, a_dint_record1_schema,

      a_long_record1_schema, a_int_record1_schema,

      a_int_record1_schema, a_int_b_int_record1_schema,
      a_dint_record1_schema, a_int_b_int_record1_schema,

      a_int_b_dint_record1_schema, a_int_record1_schema,
      a_dint_b_dint_record1_schema, empty_record1_schema,
      a_dint_b_dint_record1_schema, a_int_record1_schema,
      a_int_b_int_record1_schema, a_dint_b_dint_record1_schema,

      int_list_record_schema, int_list_record_schema,
      long_list_record_schema, long_list_record_schema,
      long_list_record_schema, int_list_record_schema,

      null_schema, null_schema,

      nested_optional_record, nested_record
    ].each_slice(2) do |(reader, writer)|
      assert_true(can_read?(writer, reader), "expecting #{reader} to read #{writer}")
    end
  end

  def test_broken
    assert_false(can_read?(int_string_union_schema, int_union_schema))
  end

  def test_incompatible_reader_writer_pairs
    [
      null_schema, int_schema,
      null_schema, long_schema,

      boolean_schema, int_schema,

      int_schema, null_schema,
      int_schema, boolean_schema,
      int_schema, long_schema,
      int_schema, float_schema,
      int_schema, double_schema,

      long_schema, float_schema,
      long_schema, double_schema,

      float_schema, double_schema,

      string_schema, boolean_schema,
      string_schema, int_schema,

      bytes_schema, null_schema,
      bytes_schema, int_schema,

      int_array_schema, long_array_schema,
      int_map_schema, int_array_schema,
      int_array_schema, int_map_schema,
      int_map_schema, long_map_schema,

      enum1_ab_schema, enum1_abc_schema,
      enum1_ab_schema, enum1_ab_aliased_schema,
      enum1_bc_schema, enum1_abc_schema,

      enum1_ab_schema, enum2_ab_schema,
      int_schema, enum2_ab_schema,
      enum2_ab_schema, int_schema,

      fixed1_schema, fixed2_schema,
      fixed1_schema, fixed1_size3_schema,
      fixed1_schema, fixed1_aliased_schema,

      int_union_schema, int_string_union_schema,
      string_union_schema, int_string_union_schema,

      empty_record2_schema, empty_record1_schema,
      empty_record1_schema, empty_record1_aliased_schema,
      a_int_record1_schema, empty_record1_schema,
      a_int_b_dint_record1_schema, empty_record1_schema,

      int_list_record_schema, long_list_record_schema,

      null_schema, int_schema,

      nested_record, nested_optional_record
    ].each_slice(2) do |(reader, writer)|
      assert_false(can_read?(writer, reader), "expecting #{reader} not to read #{writer}")
    end
  end

  def writer_schema
    Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"},
        {"name":"oldfield2", "type":"string"}
      ]}
    SCHEMA
  end

  def test_missing_field
    reader_schema = Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"}
      ]}
    SCHEMA
    assert_true(can_read?(writer_schema, reader_schema))
    assert_false(can_read?(reader_schema, writer_schema))
  end

  def test_missing_second_field
    reader_schema = Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield2", "type":"string"}
      ]}
    SCHEMA
    assert_true(can_read?(writer_schema, reader_schema))
    assert_false(can_read?(reader_schema, writer_schema))
  end

  def test_aliased_field
    reader_schema = Avro::Schema.parse(<<-SCHEMA)
      {"type":"record", "name":"Record", "fields":[
        {"name":"newname1", "aliases":["oldfield1"], "type":"int"},
        {"name":"oldfield2", "type":"string"}
      ]}
    SCHEMA
    assert_true(can_read?(writer_schema, reader_schema))
    assert_false(can_read?(reader_schema, writer_schema))
  end

  def test_all_fields
    reader_schema = Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"},
        {"name":"oldfield2", "type":"string"}
      ]}
    SCHEMA
    assert_true(can_read?(writer_schema, reader_schema))
    assert_true(can_read?(reader_schema, writer_schema))
  end

  def test_new_field_with_default
    reader_schema = Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"},
        {"name":"newfield1", "type":"int", "default":42}
      ]}
    SCHEMA
    assert_true(can_read?(writer_schema, reader_schema))
    assert_false(can_read?(reader_schema, writer_schema))
  end

  def test_new_field
    reader_schema = Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Record", "fields":[
        {"name":"oldfield1", "type":"int"},
        {"name":"newfield1", "type":"int"}
      ]}
    SCHEMA
    assert_false(can_read?(writer_schema, reader_schema))
    assert_false(can_read?(reader_schema, writer_schema))
  end

  def test_array_writer_schema
    valid_reader = string_array_schema
    invalid_reader = string_map_schema

    assert_true(can_read?(string_array_schema, valid_reader))
    assert_false(can_read?(string_array_schema, invalid_reader))
  end

  def test_primitive_writer_schema
    valid_reader = string_schema
    assert_true(can_read?(string_schema, valid_reader))
    assert_false(can_read?(int_schema, string_schema))
  end

  def test_union_reader_writer_subset_incompatiblity
    # reader union schema must contain all writer union branches
    union_writer = union_schema(int_schema, string_schema)
    union_reader = union_schema(string_schema)

    assert_false(can_read?(union_writer, union_reader))
    assert_true(can_read?(union_reader, union_writer))
  end

  def test_incompatible_record_field
    string_schema = Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
        {"name":"field1", "type":"string"}
      ]}
    SCHEMA
    int_schema = Avro::Schema.parse <<-SCHEMA2
      {"type":"record", "name":"MyRecord", "namespace":"ns", "fields": [
        {"name":"field1", "type":"int"}
      ]}
    SCHEMA2
    assert_false(can_read?(string_schema, int_schema))
  end

  def test_enum_symbols
    enum_schema1 = Avro::Schema.parse <<-SCHEMA
      {"type":"enum", "name":"MyEnum", "symbols":["A","B"]}
    SCHEMA
    enum_schema2 = Avro::Schema.parse <<-SCHEMA
      {"type":"enum", "name":"MyEnum", "symbols":["A","B","C"]}
    SCHEMA
    assert_false(can_read?(enum_schema2, enum_schema1))
    assert_true(can_read?(enum_schema1, enum_schema2))
  end

  def test_crossed_aliases
    writer_schema = Avro::Schema.parse(<<-SCHEMA)
      {"type":"record", "name":"Record", "fields":[
        {"name":"field1", "type": "int"},
        {"name":"field2", "type": "string"}
      ]}
    SCHEMA
    reader_schema = Avro::Schema.parse(<<-SCHEMA)
      {"type":"record", "name":"Record", "fields":[
        {"name":"field1", "aliases":["field2"], "type":"string"},
        {"name":"field2", "aliases":["field1"], "type":"int"}
      ]}
    SCHEMA
    # Not supported; alias is not used if there is a redirect match
    assert_false(can_read?(writer_schema, reader_schema))
  end

  def test_bytes_decimal
    bytes_decimal_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"decimal", "precision":4, "scale":4}')
    bytes2_decimal_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"decimal", "precision":4, "scale":4}')
    bytes_decimal_different_precision_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"decimal", "precision":5, "scale":4}')
    bytes_decimal_no_scale_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"decimal", "precision":4}')
    bytes2_decimal_no_scale_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"decimal", "precision":4}')
    bytes_decimal_zero_scale_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"decimal", "precision":4, "scale":0}')
    bytes_unknown_logical_type_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"unknown"}')

    # decimal bytes and non-decimal bytes can be mixed
    assert_true(can_read?(bytes_schema, bytes_decimal_schema))
    assert_true(can_read?(bytes_decimal_schema, bytes_schema))
    assert_true(can_read?(bytes_decimal_schema, bytes_unknown_logical_type_schema))

    # decimal bytes match even if precision and scale differ
    assert_true(can_read?(bytes_decimal_schema, bytes_decimal_different_precision_schema))
    assert_true(can_read?(bytes_decimal_schema, bytes_decimal_no_scale_schema))
    assert_true(can_read?(bytes_decimal_schema, bytes_decimal_zero_scale_schema))
    # - zero and no scale are equivalent
    assert_true(can_read?(bytes_decimal_zero_scale_schema, bytes_decimal_no_scale_schema))
    # - different schemas with the same attributes match
    assert_true(can_read?(bytes_decimal_schema, bytes2_decimal_schema))
    # - different schemas with the same no scale match
    assert_true(can_read?(bytes2_decimal_no_scale_schema, bytes_decimal_no_scale_schema))
  end

  def test_fixed_decimal
    fixed_decimal_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed1", "logicalType":"decimal", "precision":4, "scale":2}')
    fixed2_decimal_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed2", "logicalType":"decimal", "precision":4, "scale":2}')
    fixed_decimal_different_precision_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed1", "logicalType":"decimal", "precision":3, "scale":2}')
    fixed_decimal_size3_schema = Avro::Schema.
      parse('{"type":"fixed", "size":3, "name":"FixedS3", "logicalType":"decimal", "precision":4, "scale":2}')
    fixed_unknown_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed1", "logicalType":"unknown"}')
    fixed_decimal_zero_scale_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed1", "logicalType":"decimal", "precision":4, "scale":0}')
    fixed_decimal_no_scale_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed1", "logicalType":"decimal", "precision":4}')

    # decimal fixed and non-decimal can be mixed if fixed name matches
    assert_true(can_read?(fixed_decimal_schema, fixed1_schema))
    assert_true(can_read?(fixed1_schema, fixed_decimal_schema))
    assert_false(can_read?(fixed2_schema, fixed_decimal_schema))

    # decimal logical types match even if fixed name differs
    assert_true(can_read?(fixed_decimal_schema, fixed2_decimal_schema))

    # fixed with the same name & size match even if decimal precision and scale differ
    assert_true(can_read?(fixed_decimal_schema, fixed_decimal_different_precision_schema))
    assert_true(can_read?(fixed_decimal_schema, fixed_decimal_size3_schema))
    assert_true(can_read?(fixed_decimal_schema, fixed_unknown_schema))
    # - zero and no scale are equivalent but these match anyway due to same name & size
    assert_true(can_read?(fixed_decimal_no_scale_schema, fixed_decimal_zero_scale_schema))
    # - scale does not match
    assert_true(can_read?(fixed_decimal_schema, fixed_decimal_no_scale_schema))
    assert_true(can_read?(fixed_decimal_schema, fixed_decimal_zero_scale_schema))
  end

  def test_decimal_different_types
    fixed_decimal_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed1", "logicalType":"decimal", "precision":4, "scale":2}')
    fixed_decimal_scale4_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed1", "logicalType":"decimal", "precision":4, "scale":4}')
    bytes_decimal_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"decimal", "precision":4, "scale":2}')
    fixed_decimal_zero_scale_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed1", "logicalType":"decimal", "precision":4, "scale":0}')
    fixed_decimal_no_scale_schema = Avro::Schema.
      parse('{"type":"fixed", "size":2, "name":"Fixed1", "logicalType":"decimal", "precision":4}')
    bytes_decimal_zero_scale_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"decimal", "precision":4, "scale":0}')
    bytes_decimal_no_scale_schema = Avro::Schema.
      parse('{"type":"bytes", "logicalType":"decimal", "precision":4}')

    # decimal logical types can be read
    assert_true(can_read?(fixed_decimal_schema, bytes_decimal_schema))
    assert_true(can_read?(bytes_decimal_schema, fixed_decimal_schema))

    # non-decimal bytes and fixed cannot be mixed
    assert_false(can_read?(fixed_decimal_schema, bytes_schema))
    assert_false(can_read?(bytes_schema, fixed_decimal_schema))
    assert_false(can_read?(fixed1_schema, bytes_decimal_schema))
    assert_false(can_read?(bytes_decimal_schema, fixed1_schema))

    # decimal precision and scale must match
    assert_false(can_read?(fixed_decimal_scale4_schema, bytes_decimal_schema))
    assert_false(can_read?(bytes_decimal_schema, fixed_decimal_scale4_schema))

    # zero scale and no scale are equivalent
    assert_true(can_read?(bytes_decimal_no_scale_schema, fixed_decimal_zero_scale_schema))
    assert_true(can_read?(fixed_decimal_zero_scale_schema, bytes_decimal_no_scale_schema))
    assert_true(can_read?(bytes_decimal_zero_scale_schema, fixed_decimal_no_scale_schema))
    assert_true(can_read?(fixed_decimal_no_scale_schema, bytes_decimal_zero_scale_schema))
  end

  # Tests from lang/java/avro/src/test/java/org/apache/avro/io/parsing/TestResolvingGrammarGenerator2.java

  def point_2d_schema
    Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Point2D", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"}
      ]}
    SCHEMA
  end

  def point_2d_fullname_schema
    Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Point", "namespace":"written", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"}
      ]}
    SCHEMA
  end

  def point_3d_no_default_schema
    Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Point", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double"}
      ]}
    SCHEMA
  end

  def point_3d_schema
    Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Point3D", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double", "default": 0.0}
      ]}
    SCHEMA
  end

  def point_3d_match_name_schema
    Avro::Schema.parse <<-SCHEMA
      {"type":"record", "name":"Point", "fields":[
        {"name":"x", "type":"double"},
        {"name":"y", "type":"double"},
        {"name":"z", "type":"double", "default": 0.0}
      ]}
    SCHEMA
  end

  def test_union_resolution_no_structure_match
    # short name match, but no structure match
    read_schema = union_schema(null_schema, point_3d_no_default_schema)
    assert_false(can_read?(point_2d_fullname_schema, read_schema))
  end

  def test_union_resolution_first_structure_match_2d
    # multiple structure matches with no name matches
    read_schema = union_schema(null_schema, point_3d_no_default_schema, point_2d_schema, point_3d_schema)
    assert_false(can_read?(point_2d_fullname_schema, read_schema))
  end

  def test_union_resolution_first_structure_match_3d
    # multiple structure matches with no name matches
    read_schema = union_schema(null_schema, point_3d_no_default_schema, point_3d_schema, point_2d_schema)
    assert_false(can_read?(point_2d_fullname_schema, read_schema))
  end

  def test_union_resolution_named_structure_match
    # multiple structure matches with a short name match
    read_schema = union_schema(null_schema, point_2d_schema, point_3d_match_name_schema, point_3d_schema)
    assert_false(can_read?(point_2d_fullname_schema, read_schema))
  end

  def test_union_resolution_full_name_match
    # there is a full name match that should be chosen
    read_schema = union_schema(null_schema, point_2d_schema, point_3d_match_name_schema, point_3d_schema, point_2d_fullname_schema)
    assert_true(can_read?(point_2d_fullname_schema, read_schema))
  end

  def can_read?(writer, reader)
    Avro::SchemaCompatibility.can_read?(writer, reader)
  end

  def union_schema(*schemas)
    schemas ||= []
    Avro::Schema.parse("[#{schemas.map(&:to_s).join(',')}]")
  end

  Avro::Schema::PRIMITIVE_TYPES.each do |schema_type|
    define_method("#{schema_type}_schema") do
      Avro::Schema.parse("\"#{schema_type}\"")
    end
  end

  def int_array_schema
    Avro::Schema.parse('{"type":"array", "items":"int"}')
  end

  def long_array_schema
    Avro::Schema.parse('{"type":"array", "items":"long"}')
  end

  def string_array_schema
    Avro::Schema.parse('{"type":"array", "items":"string"}')
  end

  def int_map_schema
    Avro::Schema.parse('{"type":"map", "values":"int"}')
  end

  def long_map_schema
    Avro::Schema.parse('{"type":"map", "values":"long"}')
  end

  def string_map_schema
    Avro::Schema.parse('{"type":"map", "values":"string"}')
  end

  def enum1_ab_schema
    Avro::Schema.parse('{"type":"enum", "name":"Enum1", "symbols":["A","B"]}')
  end

  def enum1_ab_default_schema
    Avro::Schema.parse('{"type":"enum", "name":"Enum1", "symbols":["A","B"], "default":"A"}')
  end

  def enum1_ab_aliased_schema
    Avro::Schema.parse('{"type":"enum", "name":"Enum2", "aliases":["Enum1"], "symbols":["A","B"]}')
  end

  def enum1_abc_schema
    Avro::Schema.parse('{"type":"enum", "name":"Enum1", "symbols":["A","B","C"]}')
  end

  def enum1_bc_schema
    Avro::Schema.parse('{"type":"enum", "name":"Enum1", "symbols":["B","C"]}')
  end

  def enum2_ab_schema
    Avro::Schema.parse('{"type":"enum", "name":"Enum2", "symbols":["A","B"]}')
  end

  def fixed1_schema
    Avro::Schema.parse('{"type":"fixed", "name":"Fixed1", "size": 2}')
  end

  def fixed1_aliased_schema
    Avro::Schema.parse('{"type":"fixed", "name":"Fixed2", "aliases":["Fixed1"], "size": 2}')
  end

  def fixed2_schema
    Avro::Schema.parse('{"type":"fixed", "name":"Fixed2", "size": 2}')
  end

  def fixed1_size3_schema
    Avro::Schema.parse('{"type":"fixed", "name":"Fixed1", "size": 3}')
  end

  def empty_record1_schema
    Avro::Schema.parse('{"type":"record", "name":"Record1"}')
  end

  def empty_record1_aliased_schema
    Avro::Schema.parse('{"type":"record", "name":"Record2", "aliases":["Record1"]}')
  end

  def empty_record2_schema
    Avro::Schema.parse('{"type":"record", "name":"Record2"}')
  end

  def a_int_record1_schema
    Avro::Schema.parse('{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}]}')
  end

  def a_long_record1_schema
    Avro::Schema.parse('{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"long"}]}')
  end

  def a_int_b_int_record1_schema
    Avro::Schema.parse('{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int"}]}')
  end

  def a_dint_record1_schema
    Avro::Schema.parse('{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}]}')
  end

  def a_int_b_dint_record1_schema
    Avro::Schema.parse('{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int"}, {"name":"b", "type":"int", "default":0}]}')
  end

  def a_dint_b_dint_record1_schema
    Avro::Schema.parse('{"type":"record", "name":"Record1", "fields":[{"name":"a", "type":"int", "default":0}, {"name":"b", "type":"int", "default":0}]}')
  end

  def nested_record
    Avro::Schema.parse('{"type":"record","name":"parent","fields":[{"name":"attribute","type":{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}}]}')
  end

  def nested_optional_record
    Avro::Schema.parse('{"type":"record","name":"parent","fields":[{"name":"attribute","type":["null",{"type":"record","name":"child","fields":[{"name":"id","type":"string"}]}],"default":null}]}')
  end

  def int_list_record_schema
    Avro::Schema.parse <<-SCHEMA
      {
        "type":"record", "name":"List", "fields": [
          {"name": "head", "type": "int"},
          {"name": "tail", "type": "List"}
      ]}
    SCHEMA
  end

  def long_list_record_schema
    Avro::Schema.parse <<-SCHEMA
      {
        "type":"record", "name":"List", "fields": [
          {"name": "head", "type": "long"},
          {"name": "tail", "type": "List"}
      ]}
    SCHEMA
  end

  def empty_union_schema
    union_schema
  end

  def null_union_schema
    union_schema(null_schema)
  end

  def int_union_schema
    union_schema(int_schema)
  end

  def long_union_schema
    union_schema(long_schema)
  end

  def string_union_schema
    union_schema(string_schema)
  end

  def int_string_union_schema
    union_schema(int_schema, string_schema)
  end

  def string_int_union_schema
    union_schema(string_schema, int_schema)
  end
end

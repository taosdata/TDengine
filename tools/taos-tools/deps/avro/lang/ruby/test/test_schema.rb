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

class TestSchema < Test::Unit::TestCase
  def hash_to_schema(hash)
    Avro::Schema.parse(hash.to_json)
  end

  def test_default_namespace
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "OuterRecord", "fields": [
        {"name": "field1", "type": {
          "type": "record", "name": "InnerRecord", "fields": []
        }},
        {"name": "field2", "type": "InnerRecord"}
      ]}
    SCHEMA

    assert_equal 'OuterRecord', schema.name
    assert_equal 'OuterRecord', schema.fullname
    assert_nil schema.namespace

    schema.fields.each do |field|
      assert_equal 'InnerRecord', field.type.name
      assert_equal 'InnerRecord', field.type.fullname
      assert_nil field.type.namespace
    end
  end

  def test_inherited_namespace
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "OuterRecord", "namespace": "my.name.space",
       "fields": [
          {"name": "definition", "type": {
            "type": "record", "name": "InnerRecord", "fields": []
          }},
          {"name": "relativeReference", "type": "InnerRecord"},
          {"name": "absoluteReference", "type": "my.name.space.InnerRecord"}
      ]}
    SCHEMA

    assert_equal 'OuterRecord', schema.name
    assert_equal 'my.name.space.OuterRecord', schema.fullname
    assert_equal 'my.name.space', schema.namespace
    schema.fields.each do |field|
      assert_equal 'InnerRecord', field.type.name
      assert_equal 'my.name.space.InnerRecord', field.type.fullname
      assert_equal 'my.name.space', field.type.namespace
    end
  end

  def test_inherited_namespace_from_dotted_name
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "my.name.space.OuterRecord", "fields": [
        {"name": "definition", "type": {
          "type": "enum", "name": "InnerEnum", "symbols": ["HELLO", "WORLD"]
        }},
        {"name": "relativeReference", "type": "InnerEnum"},
        {"name": "absoluteReference", "type": "my.name.space.InnerEnum"}
      ]}
    SCHEMA

    assert_equal 'OuterRecord', schema.name
    assert_equal 'my.name.space.OuterRecord', schema.fullname
    assert_equal 'my.name.space', schema.namespace
    schema.fields.each do |field|
      assert_equal 'InnerEnum', field.type.name
      assert_equal 'my.name.space.InnerEnum', field.type.fullname
      assert_equal 'my.name.space', field.type.namespace
    end
  end

  def test_nested_namespaces
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "outer.OuterRecord", "fields": [
        {"name": "middle", "type": {
          "type": "record", "name": "middle.MiddleRecord", "fields": [
            {"name": "inner", "type": {
              "type": "record", "name": "InnerRecord", "fields": [
                {"name": "recursive", "type": "MiddleRecord"}
              ]
            }}
          ]
        }}
      ]}
    SCHEMA

    assert_equal 'OuterRecord', schema.name
    assert_equal 'outer.OuterRecord', schema.fullname
    assert_equal 'outer', schema.namespace
    middle = schema.fields.first.type
    assert_equal 'MiddleRecord', middle.name
    assert_equal 'middle.MiddleRecord', middle.fullname
    assert_equal 'middle', middle.namespace
    inner = middle.fields.first.type
    assert_equal 'InnerRecord', inner.name
    assert_equal 'middle.InnerRecord', inner.fullname
    assert_equal 'middle', inner.namespace
    assert_equal middle, inner.fields.first.type
  end

  def test_to_avro_includes_namespaces
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "my.name.space.OuterRecord", "fields": [
        {"name": "definition", "type": {
          "type": "fixed", "name": "InnerFixed", "size": 16
        }},
        {"name": "reference", "type": "InnerFixed"}
      ]}
    SCHEMA

    assert_equal({
      'type' => 'record', 'name' => 'OuterRecord', 'namespace' => 'my.name.space',
      'fields' => [
        {'name' => 'definition', 'type' => {
          'type' => 'fixed', 'name' => 'InnerFixed', 'namespace' => 'my.name.space',
          'size' => 16
        }},
        {'name' => 'reference', 'type' => 'my.name.space.InnerFixed'}
      ]
    }, schema.to_avro)
  end

  def test_to_avro_includes_logical_type
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "has_logical", "fields": [
        {"name": "dt", "type": {"type": "int", "logicalType": "date"}}]
      }
    SCHEMA

    assert_equal schema.to_avro, {
      'type' => 'record', 'name' => 'has_logical',
      'fields' => [
        {'name' => 'dt', 'type' => {'type' => 'int', 'logicalType' => 'date'}}
      ]
    }
  end

  def test_to_avro_includes_aliases
    hash = {
      'type' => 'record',
      'name' => 'test_record',
      'aliases' => %w(alt_record),
      'fields' => [
        { 'name' => 'f', 'type' => { 'type' => 'fixed', 'size' => 2, 'name' => 'test_fixed', 'aliases' => %w(alt_fixed) } },
        { 'name' => 'e', 'type' => { 'type' => 'enum', 'symbols' => %w(A B), 'name' => 'test_enum', 'aliases' => %w(alt_enum) } }
      ]
    }
    schema = hash_to_schema(hash)
    assert_equal(schema.to_avro, hash)
  end

  def test_unknown_named_type
    error = assert_raise Avro::UnknownSchemaError do
      Avro::Schema.parse <<-SCHEMA
        {"type": "record", "name": "my.name.space.Record", "fields": [
          {"name": "reference", "type": "MissingType"}
        ]}
      SCHEMA
    end

    assert_equal '"MissingType" is not a schema we know about.', error.message
  end

  def test_invalid_name
    error = assert_raise Avro::SchemaParseError do
      Avro::Schema.parse <<-SCHEMA
        {"type": "record", "name": "my-invalid-name", "fields": [
          {"name": "id", "type": "int"}
        ]}
      SCHEMA
    end

    assert_equal "Name my-invalid-name is invalid for type record!", error.message
  end

  def test_invalid_name_with_two_periods
    error = assert_raise Avro::SchemaParseError do
      Avro::Schema.parse <<-SCHEMA
        {"type": "record", "name": "my..invalid.name", "fields": [
          {"name": "id", "type": "int"}
        ]}
      SCHEMA
    end

    assert_equal "Name my..invalid.name is invalid for type record!", error.message
  end

  def test_invalid_name_with_validation_disabled
    Avro.disable_schema_name_validation = true
    assert_nothing_raised do
      Avro::Schema.parse <<-SCHEMA
        {"type": "record", "name": "my-invalid-name", "fields": [
          {"name": "id", "type": "int"}
        ]}
      SCHEMA
    end
    Avro.disable_schema_name_validation = false
  end

  def test_to_avro_handles_falsey_defaults
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "Record", "namespace": "my.name.space",
        "fields": [
          {"name": "is_usable", "type": "boolean", "default": false}
        ]
      }
    SCHEMA

    assert_equal schema.to_avro, {
      'type' => 'record', 'name' => 'Record', 'namespace' => 'my.name.space',
      'fields' => [
        {'name' => 'is_usable', 'type' => 'boolean', 'default' => false}
      ]
    }
  end

  def test_record_field_doc_attribute
    field_schema_json = Avro::Schema.parse <<-SCHEMA
      {
        "type": "record",
        "name": "Record",
        "namespace": "my.name.space",
        "fields": [
          {
            "name": "name",
            "type": "boolean",
            "doc": "documentation"
          }
        ]
      }
    SCHEMA

    field_schema_hash =
      {
        'type' => 'record',
        'name' => 'Record',
        'namespace' => 'my.name.space',
        'fields' => [
          {
            'name' => 'name',
            'type' => 'boolean',
            'doc' => 'documentation'
          }
        ]
      }

    assert_equal field_schema_hash, field_schema_json.to_avro
  end

  def test_record_doc_attribute
    record_schema_json = Avro::Schema.parse <<-SCHEMA
      {
        "type": "record",
        "name": "Record",
        "namespace": "my.name.space",
        "doc": "documentation",
        "fields": [
          {
            "name": "name",
            "type": "boolean"
          }
        ]
      }
    SCHEMA

    record_schema_hash =
      {
        'type' => 'record',
        'name' => 'Record',
        'namespace' => 'my.name.space',
        'doc' => 'documentation',
        'fields' => [
          {
            'name' => 'name',
            'type' => 'boolean'
          }
        ]
      }

    assert_equal record_schema_hash, record_schema_json.to_avro
  end

  def test_enum_doc_attribute
    enum_schema_json = Avro::Schema.parse <<-SCHEMA
      {
        "type": "enum",
        "name": "Enum",
        "namespace": "my.name.space",
        "doc": "documentation",
        "symbols" : [
          "SPADES",
          "HEARTS",
          "DIAMONDS",
          "CLUBS"
        ]
      }
    SCHEMA

    enum_schema_hash =
      {
        'type' => 'enum',
        'name' => 'Enum',
        'namespace' => 'my.name.space',
        'doc' => 'documentation',
        'symbols' => [
          'SPADES',
          'HEARTS',
          'DIAMONDS',
          'CLUBS'
        ]
      }
    assert_equal enum_schema_hash, enum_schema_json.to_avro
  end

  def test_enum_default_attribute
    enum_schema = Avro::Schema.parse <<-SCHEMA
      {
        "type": "enum",
        "name": "fruit",
        "default": "apples",
        "symbols": ["apples", "oranges"]
      }
    SCHEMA

    enum_schema_hash = {
      'type' => 'enum',
      'name' => 'fruit',
      'default' => 'apples',
      'symbols' => %w(apples oranges)
    }

    assert_equal(enum_schema.default, "apples")
    assert_equal(enum_schema_hash, enum_schema.to_avro)
  end

  def test_validate_enum_default
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'enum',
        name: 'fruit',
        default: 'bananas',
        symbols: %w(apples oranges)
      )
    end
    assert_equal("Default 'bananas' is not a valid symbol for enum fruit",
                 exception.to_s)
  end

  def test_empty_record
    schema = Avro::Schema.parse('{"type":"record", "name":"Empty"}')
    assert_empty(schema.fields)
  end

  def test_empty_union
    schema = Avro::Schema.parse('[]')
    assert_equal(schema.to_s, '[]')
  end

  def test_read
    schema = Avro::Schema.parse('"string"')
    writer_schema = Avro::Schema.parse('"int"')
    assert_false(schema.read?(writer_schema))
    assert_true(schema.read?(schema))
  end

  def test_be_read
    schema = Avro::Schema.parse('"string"')
    writer_schema = Avro::Schema.parse('"int"')
    assert_false(schema.be_read?(writer_schema))
    assert_true(schema.be_read?(schema))
  end

  def test_mutual_read
    schema = Avro::Schema.parse('"string"')
    writer_schema = Avro::Schema.parse('"int"')
    default1 = Avro::Schema.parse('{"type":"record", "name":"Default", "fields":[{"name":"i", "type":"int", "default": 1}]}')
    default2 = Avro::Schema.parse('{"type":"record", "name":"Default", "fields":[{"name:":"s", "type":"string", "default": ""}]}')
    assert_false(schema.mutual_read?(writer_schema))
    assert_true(schema.mutual_read?(schema))
    assert_true(default1.mutual_read?(default2))
  end

  def test_validate_defaults
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          {
            name: 'veggies',
            type: 'string',
            default: nil
          }
        ]
      )
    end
    assert_equal('Error validating default for veggies: at . expected type string, got null',
                 exception.to_s)
  end

  def test_field_default_validation_disabled
    Avro.disable_field_default_validation = true
    assert_nothing_raised do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          {
            name: 'veggies',
            type: 'string',
            default: nil
          }
        ]
      )
    end
  ensure
    Avro.disable_field_default_validation = false
  end

  def test_field_default_validation_disabled_via_env
    Avro.disable_field_default_validation = false
    ENV['AVRO_DISABLE_FIELD_DEFAULT_VALIDATION'] = "1"

    assert_nothing_raised do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          {
            name: 'veggies',
            type: 'string',
            default: nil
          }
        ]
      )
    end
  ensure
    ENV.delete('AVRO_DISABLE_FIELD_DEFAULT_VALIDATION')
    Avro.disable_field_default_validation = false
  end

  def test_validate_record_valid_default
    assert_nothing_raised(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'with_subrecord',
        fields: [
          {
            name: 'sub',
            type: {
              name: 'subrecord',
              type: 'record',
              fields: [
                { type: 'string', name: 'x' }
              ]
            },
            default: {
              x: "y"
            }
          }
        ]
      )
    end
  end

  def test_validate_record_invalid_default
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'with_subrecord',
        fields: [
          {
            name: 'sub',
            type: {
              name: 'subrecord',
              type: 'record',
              fields: [
                { type: 'string', name: 'x' }
              ]
            },
            default: {
              a: 1
            }
          }
        ]
      )
    end
    assert_equal('Error validating default for sub: at .x expected type string, got null',
                 exception.to_s)
  end

  def test_validate_union_defaults
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          {
            name: 'veggies',
            type: %w(string null),
            default: 5
          }
        ]
      )
    end
    assert_equal('Error validating default for veggies: at . expected type string, got int with value 5',
                 exception.to_s)
  end

  def test_validate_union_default_first_type
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          {
            name: 'veggies',
            type: %w(null string),
            default: 'apple'
          }
        ]
      )
    end
    assert_equal('Error validating default for veggies: at . expected type null, got string with value "apple"',
                 exception.to_s)
  end

  def test_fixed_decimal_to_include_precision_scale
    schema = Avro::Schema.parse <<-SCHEMA
      {
        "type": "fixed",
        "name": "aFixed",
        "logicalType": "decimal",
        "size": 4,
        "precision": 9,
        "scale": 2
      }
    SCHEMA

    schema_hash =
      {
        'type' => 'fixed',
        'name' => 'aFixed',
        'logicalType' => 'decimal',
        'size' => 4,
        'precision' => 9,
        'scale' => 2
      }

    assert_equal schema_hash, schema.to_avro
  end

  def test_fixed_decimal_to_include_precision_no_scale
    schema = Avro::Schema.parse <<-SCHEMA
      {
        "type": "fixed",
        "name": "aFixed",
        "logicalType": "decimal",
        "size": 4,
        "precision": 9
      }
    SCHEMA

    schema_hash =
      {
        'type' => 'fixed',
        'name' => 'aFixed',
        'logicalType' => 'decimal',
        'size' => 4,
        'precision' => 9
      }

    assert_equal schema_hash, schema.to_avro
  end

  # Note: this is not valid but validation is not yet implemented
  def test_fixed_decimal_to_without_precision_scale
    schema = Avro::Schema.parse <<-SCHEMA
      {
        "type": "fixed",
        "size": 4,
        "name": "aFixed",
        "logicalType": "decimal"
      }
    SCHEMA

    schema_hash =
      {
        'type' => 'fixed',
        'name' => 'aFixed',
        'logicalType' => 'decimal',
        'size' => 4
      }

    assert_equal schema_hash, schema.to_avro
  end

  def test_bytes_decimal_to_include_precision_scale
    schema = Avro::Schema.parse <<-SCHEMA
      {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 9,
        "scale": 2
      }
    SCHEMA

    schema_hash =
      {
        'type' => 'bytes',
        'logicalType' => 'decimal',
        'precision' => 9,
        'scale' => 2
      }

    assert_equal schema_hash, schema.to_avro
  end

  def test_bytes_decimal_with_string_precision_no_scale
    schema = Avro::Schema.parse <<-SCHEMA
      {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": "7"
      }
    SCHEMA

    schema_hash =
      {
        'type' => 'bytes',
        'logicalType' => 'decimal',
        'precision' => 7
      }

    assert_equal schema_hash, schema.to_avro
  end

  def test_bytes_decimal_without_precision_or_scale
    error = assert_raise Avro::SchemaParseError do
      Avro::Schema.parse <<-SCHEMA
      {
        "type": "bytes",
        "logicalType": "decimal"
      }
      SCHEMA
    end

    assert_equal 'Precision must be positive', error.message
  end

  def test_bytes_decimal_to_negative_precision
    error = assert_raise Avro::SchemaParseError do
      Avro::Schema.parse <<-SCHEMA
      {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": -1
      }
      SCHEMA
    end

    assert_equal 'Precision must be positive', error.message
  end

  def test_bytes_decimal_to_negative_scale
    error = assert_raise Avro::SchemaParseError do
      Avro::Schema.parse <<-SCHEMA
      {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 2,
        "scale": -1
      }
      SCHEMA
    end

    assert_equal 'Scale must be greater than or equal to 0', error.message
  end

  def test_bytes_decimal_with_precision_less_than_scale
    error = assert_raise Avro::SchemaParseError do
      Avro::Schema.parse <<-SCHEMA
      {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 3,
        "scale": 4
      }
      SCHEMA
    end

    assert_equal 'Precision must be greater than scale', error.message
  end

  def test_bytes_schema
    schema = Avro::Schema.parse <<-SCHEMA
      {
        "type": "bytes"
      }
    SCHEMA

    schema_str = 'bytes'
    assert_equal schema_str, schema.to_avro
  end

  def test_validate_duplicate_symbols
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'enum',
        name: 'name',
        symbols: ['erica', 'erica']
      )
    end
    assert_equal(
      'Duplicate symbol: ["erica", "erica"]',
      exception.to_s
    )
  end

  def test_validate_enum_symbols
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'enum',
        name: 'things',
        symbols: ['good_symbol', '_GOOD_SYMBOL_2', '8ad_symbol', 'also-bad-symbol', '>=', '$']
      )
    end

    assert_equal(
      "Invalid symbols for things: 8ad_symbol, also-bad-symbol, >=, $ don't match #{Avro::Schema::EnumSchema::SYMBOL_REGEX.inspect}",
      exception.to_s
    )
  end

  def test_enum_symbol_validation_disabled_via_env
    Avro.disable_enum_symbol_validation = nil
    ENV['AVRO_DISABLE_ENUM_SYMBOL_VALIDATION'] = '1'

    hash_to_schema(
      type: 'enum',
      name: 'things',
      symbols: ['good_symbol', '_GOOD_SYMBOL_2', '8ad_symbol', 'also-bad-symbol', '>=', '$'],
    )
  ensure
    ENV.delete('AVRO_DISABLE_ENUM_SYMBOL_VALIDATION')
    Avro.disable_enum_symbol_validation = nil
  end

  def test_enum_symbol_validation_disabled_via_class_method
    Avro.disable_enum_symbol_validation = true

    hash_to_schema(
      type: 'enum',
      name: 'things',
      symbols: ['good_symbol', '_GOOD_SYMBOL_2', '8ad_symbol', 'also-bad-symbol', '>=', '$'],
    )
  ensure
    Avro.disable_enum_symbol_validation = nil
  end

  def test_validate_field_aliases
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          { name: 'banana', type: 'string', aliases: 'banane' }
        ]
      )
    end

    assert_match(/Invalid aliases value "banane" for "string" banana/, exception.to_s)
  end

  def test_validate_same_alias_multiple_fields
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          { name: 'banana', type: 'string', aliases: %w(yellow) },
          { name: 'lemo', type: 'string', aliases: %w(yellow) }
        ]
      )
    end

    assert_match('Alias ["yellow"] already in use', exception.to_s)
  end

  def test_validate_repeated_aliases
    assert_nothing_raised do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          { name: 'banana', type: 'string', aliases: %w(yellow yellow) },
        ]
      )
    end
  end

  def test_validate_record_aliases
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        aliases: ["foods", 2],
        fields: []
      )
    end

    assert_match(/Invalid aliases value \["foods", 2\] for record fruits/, exception.to_s)
  end

  def test_validate_enum_aliases
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'enum',
        name: 'vowels',
        aliases: [1, 2],
        symbols: %w(A E I O U)
      )
    end

    assert_match(/Invalid aliases value \[1, 2\] for enum vowels/, exception.to_s)
  end

  def test_validate_fixed_aliases
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'fixed',
        name: 'uuid',
        size: 36,
        aliases: "unique_id"
      )
    end

    assert_match(/Invalid aliases value "unique_id" for fixed uuid/, exception.to_s)
  end
end

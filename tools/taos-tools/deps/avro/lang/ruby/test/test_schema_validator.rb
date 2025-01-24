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

class TestSchemaValidator < Test::Unit::TestCase
  def validate!(schema, value, options = {})
    Avro::SchemaValidator.validate!(schema, value, options)
  end

  def validate_simple!(schema, value)
    Avro::SchemaValidator.validate!(schema, value, recursive: false)
  end

  def hash_to_schema(hash)
    Avro::Schema.parse(hash.to_json)
  end

  def assert_failed_validation(messages)
    error = assert_raise(Avro::SchemaValidator::ValidationError) { yield }

    assert_messages = [messages].flatten
    result_errors = error.result.errors
    assert_messages.each do |message|
      assert(result_errors.include?(message), "expected '#{message}' to be in '#{result_errors}'")
    end
    assert_equal(assert_messages.size, result_errors.size)
  end

  def assert_valid_schema(schema, valid, invalid, simple = false)
    valid.each do |value|
      assert_nothing_raised { Avro::SchemaValidator.validate!(schema, value) }
      assert_nothing_raised { Avro::SchemaValidator.validate!(schema, value, recursive: false) } if simple
    end

    invalid.each do |value|
      assert_raise { Avro::SchemaValidator.validate!(schema, value) }
      assert_raise { Avro::SchemaValidator.validate!(schema, value, recursive: false) } if simple
      assert_nothing_raised { Avro::SchemaValidator.validate!(schema, value, recursive: false) } unless simple
    end
  end

  def test_validate_nil
    schema = hash_to_schema(type: 'null', name: 'name')

    assert_nothing_raised { validate!(schema, nil) }
    assert_nothing_raised { validate_simple!(schema, nil) }

    assert_failed_validation('at . expected type null, got int with value 1') do
      validate!(schema, 1)
    end

    assert_failed_validation('at . expected type null, got int with value 1') do
      validate_simple!(schema, 1)
    end
  end

  def test_validate_boolean
    schema = hash_to_schema(type: 'boolean', name: 'name')

    assert_nothing_raised { validate!(schema, true) }
    assert_nothing_raised { validate!(schema, false) }
    assert_nothing_raised { validate_simple!(schema, true) }
    assert_nothing_raised { validate_simple!(schema, false) }

    assert_failed_validation('at . expected type boolean, got int with value 1') do
      validate!(schema, 1)
    end
    assert_failed_validation('at . expected type boolean, got int with value 1') do
      validate_simple!(schema, 1)
    end

    assert_failed_validation('at . expected type boolean, got null') do
      validate!(schema, nil)
    end
    assert_failed_validation('at . expected type boolean, got null') do
      validate_simple!(schema, nil)
    end
  end

  def test_fixed_size_string
    schema = hash_to_schema(type: 'fixed', name: 'some', size: 3)

    assert_nothing_raised { validate!(schema, 'baf') }
    assert_nothing_raised { validate_simple!(schema, 'baf') }

    assert_failed_validation('at . expected fixed with size 3, got "some" with size 4') do
      validate!(schema, 'some')
    end
    assert_failed_validation('at . expected fixed with size 3, got "some" with size 4') do
      validate_simple!(schema, 'some')
    end

    assert_failed_validation('at . expected fixed with size 3, got null') do
      validate!(schema, nil)
    end
    assert_failed_validation('at . expected fixed with size 3, got null') do
      validate_simple!(schema, nil)
    end

    assert_failed_validation("at . expected fixed with size 3, got \"a\u2014b\" with size 5") do
      validate!(schema, "a\u2014b")
    end
    assert_failed_validation("at . expected fixed with size 3, got \"a\u2014b\" with size 5") do
      validate_simple!(schema, "a\u2014b")
    end
  end

  def test_original_validate_nil
    schema = hash_to_schema(type: 'null', name: 'name')

    assert_valid_schema(schema, [nil], ['something'], true)
  end

  def test_original_validate_boolean
    schema = hash_to_schema(type: 'boolean', name: 'name')

    assert_valid_schema(schema, [true, false], [nil, 1], true)
  end

  def test_validate_string
    schema = hash_to_schema(type: 'string', name: 'name')

    assert_valid_schema(schema, ['string'], [nil, 1], true)
  end

  def test_validate_bytes
    schema = hash_to_schema(type: 'bytes', name: 'name')

    assert_valid_schema(schema, ['string'], [nil, 1], true)
  end

  def test_validate_int
    schema = hash_to_schema(type: 'int', name: 'name')

    assert_valid_schema(
      schema,
      [Avro::Schema::INT_MIN_VALUE, Avro::Schema::INT_MAX_VALUE, 1],
      [Avro::Schema::LONG_MIN_VALUE, Avro::Schema::LONG_MAX_VALUE, 'string'],
      true
    )
    assert_failed_validation('at . out of bound value 9223372036854775807') do
      validate!(schema, Avro::Schema::LONG_MAX_VALUE)
    end
    assert_failed_validation('at . out of bound value 9223372036854775807') do
      validate_simple!(schema, Avro::Schema::LONG_MAX_VALUE)
    end
  end

  def test_validate_long
    schema = hash_to_schema(type: 'long', name: 'name')

    assert_valid_schema(schema, [Avro::Schema::LONG_MIN_VALUE, Avro::Schema::LONG_MAX_VALUE, 1], [1.1, 'string'], true)
  end

  def test_validate_float
    schema = hash_to_schema(type: 'float', name: 'name')

    assert_valid_schema(schema, [1.1, 1, BigDecimal('1.1'), Avro::Schema::LONG_MAX_VALUE], ['string'], true)
  end

  def test_validate_double
    schema = hash_to_schema(type: 'double', name: 'name')

    assert_valid_schema(schema, [1.1, 1, BigDecimal('1.1'), Avro::Schema::LONG_MAX_VALUE], ['string'], true)
  end

  def test_validate_fixed
    schema = hash_to_schema(type: 'fixed', name: 'name', size: 3)

    assert_valid_schema(schema, ['abc'], ['ab', 1, 1.1, true], true)
  end

  def test_validate_original_num
    schema = hash_to_schema(type: 'enum', name: 'name', symbols: %w(a b))

    assert_valid_schema(schema, ['a', 'b'], ['c'], true)
  end

  def test_validate_record
    schema = hash_to_schema(type: 'record', name: 'name', fields: [{ type: 'null', name: 'sub' }])

    assert_valid_schema(schema, [{ 'sub' => nil }], [{ 'sub' => 1 }])
  end

  def test_validate_record_with_symbol_keys
    schema = hash_to_schema(type: 'record', name: 'name', fields: [{ type: 'int', name: 'sub' }])

    assert_valid_schema(schema, [{ sub: 1 }], [{ sub: '1' }])
  end

  def test_validate_shallow_record
    schema = hash_to_schema(
      type: 'record', name: 'name', fields: [{ type: 'int', name: 'sub' }]
    )

    assert_nothing_raised { validate!(schema, 'sub' => 1) }
    assert_nothing_raised { validate_simple!(schema, 'sub' => 1) }

    assert_failed_validation('at .sub expected type int, got null') do
      validate!(schema, {})
    end
    assert_nothing_raised { validate_simple!(schema, {}) }

    assert_failed_validation('at . expected type record, got float with value 1.2') do
      validate!(schema, 1.2)
    end
    assert_nothing_raised { validate_simple!(schema, 1.2) }

    assert_failed_validation('at .sub expected type int, got float with value 1.2') do
      validate!(schema, 'sub' => 1.2)
    end
    assert_nothing_raised { validate_simple!(schema, 'sub' => 1.2) }
  end

  def test_validate_array
    schema = hash_to_schema(type: 'array',
                            name: 'person',
                            items: [{ type: 'int', name: 'height' }])

    assert_nothing_raised { validate!(schema, []) }
    assert_nothing_raised { validate_simple!(schema, []) }

    assert_failed_validation 'at . expected type array, got null' do
      validate!(schema, nil)
    end
    assert_nothing_raised { validate_simple!(schema, nil) }

    assert_failed_validation('at .[0] expected type int, got null') do
      validate!(schema, [nil])
    end
    assert_nothing_raised { validate_simple!(schema, [nil]) }

    assert_failed_validation('at .[3] expected type int, got string with value "so wrong"') do
      validate!(schema, [1, 3, 9, 'so wrong'])
    end
    assert_nothing_raised { validate_simple!(schema, [1, 3, 9, 'so wrong']) }
  end

  def test_validate_enum
    schema = hash_to_schema(type: 'enum',
                            name: 'person',
                            symbols: %w(one two three))

    assert_nothing_raised { validate!(schema, 'one') }
    assert_nothing_raised { validate_simple!(schema, 'one') }

    assert_failed_validation('at . expected enum with values ["one", "two", "three"], got string with value "five"') do
      validate!(schema, 'five')
    end
    assert_failed_validation('at . expected enum with values ["one", "two", "three"], got string with value "five"') do
      validate_simple!(schema, 'five')
    end
  end

  def test_validate_union_on_primitive_types
    schema = hash_to_schema(
      name: 'should_not_matter',
      type: 'record',
      fields: [
        { name: 'what_ever', type: %w(long string) }
      ]
    )

    assert_failed_validation('at .what_ever expected union of [\'long\', \'string\'], got null') {
      validate!(schema, 'what_ever' => nil)
    }
    assert_nothing_raised { validate_simple!(schema, 'what_ever' => nil) }
  end

  def test_validate_union_of_nil_and_record_inside_array
    schema = hash_to_schema(
      name: 'this_does_not_matter',
      type: 'record',
      fields: [
        {
          name: 'person',
          type: {
            name: 'person_entry',
            type: 'record',
            fields: [
              {
                name: 'houses',
                type: [
                  'null',
                  {
                    name: 'houses_entry',
                    type: 'array',
                    items: [
                      {
                        name: 'house_entry',
                        type: 'record',
                        fields: [
                          { name: 'number_of_rooms', type: 'long' }
                        ]
                      }
                    ]
                  }
                ],
              }
            ]
          }
        }
      ]
    )

    assert_failed_validation('at .person expected type record, got null') {
      validate!(schema, 'not at all' => nil)
    }
    assert_nothing_raised { validate_simple!(schema, 'person' => {}) }

    assert_nothing_raised { validate!(schema, 'person' => {}) }
    assert_nothing_raised { validate!(schema, 'person' => { houses: [] }) }
    assert_nothing_raised { validate!(schema, 'person' => { 'houses' => [{ 'number_of_rooms' => 1 }] }) }

    assert_nothing_raised { validate_simple!(schema, 'person' => {}) }
    assert_nothing_raised { validate_simple!(schema, 'person' => { houses: [] }) }
    assert_nothing_raised { validate_simple!(schema, 'person' => { 'houses' => [{ 'number_of_rooms' => 1 }] }) }

    message = 'at .person.houses[1].number_of_rooms expected type long, got string with value "not valid at all"'
    datum = {
      'person' => {
        'houses' => [
          { 'number_of_rooms' => 2 },
          { 'number_of_rooms' => 'not valid at all' }
        ]
      }
    }
    assert_failed_validation(message) { validate!(schema, datum) }
    assert_nothing_raised { validate_simple!(schema, datum) }
  end

  def test_validate_map
    schema = hash_to_schema(type: 'map',
                            name: 'numbers',
                            values: [
                              { name: 'some', type: 'int' }
                            ])

    assert_nothing_raised { validate!(schema, 'some' => 1) }
    assert_nothing_raised { validate_simple!(schema, 'some' => 1) }

    assert_failed_validation('at .some expected type int, got string with value "nope"') do
      validate!(schema, 'some' => 'nope')
    end
    assert_nothing_raised { validate_simple!(schema, 'some' => 'nope')}

    assert_failed_validation("at . unexpected key type 'Symbol' in map") do
      validate!(schema, some: 1)
    end
    assert_nothing_raised { validate_simple!(schema, some: 1) }

    assert_failed_validation('at . expected type map, got null') do
      validate!(schema, nil)
    end
    assert_nothing_raised { validate_simple!(schema, nil) }
  end

  def test_validate_deep_record
    schema = hash_to_schema(type: 'record',
                            name: 'person',
                            fields: [
                              {
                                name: 'head',
                                type: {
                                  name: 'head',
                                  type: 'record',
                                  fields: [
                                    {
                                      name: 'hair',
                                      type: {
                                        name: 'hair',
                                        type: 'record',
                                        fields: [
                                          {
                                            name: 'color',
                                            type: 'string'
                                          }
                                        ]
                                      }
                                    }
                                  ]
                                }
                              }
                            ])

    assert_nothing_raised { validate!(schema, 'head' => { 'hair' => { 'color' => 'black' } }) }
    assert_nothing_raised { validate_simple!(schema, 'head' => { 'hair' => { 'color' => 'black' } }) }

    assert_failed_validation('at .head.hair.color expected type string, got null') do
      validate!(schema, 'head' => { 'hair' => { 'color' => nil } })
    end
    assert_nothing_raised { validate_simple!(schema, 'head' => { 'hair' => { 'color' => nil } }) }

    assert_failed_validation('at .head.hair.color expected type string, got null') do
      validate!(schema, 'head' => { 'hair' => {} })
    end
    assert_nothing_raised { validate_simple!(schema, 'head' => { 'hair' => {} }) }

    assert_failed_validation('at .head.hair expected type record, got null') do
      validate!(schema, 'head' => {})
    end
    assert_nothing_raised { validate_simple!(schema, 'head' => {}) }

    assert_failed_validation('at . expected type record, got null') do
      validate!(schema, nil)
    end
    assert_nothing_raised { validate_simple!(schema, nil) }
  end

  def test_validate_deep_record_with_array
    schema = hash_to_schema(type: 'record',
                            name: 'fruits',
                            fields: [
                              {
                                name: 'fruits',
                                type: {
                                  name: 'fruits',
                                  type: 'array',
                                  items: [
                                    {
                                      name: 'fruit',
                                      type: 'record',
                                      fields: [
                                        { name: 'name', type: 'string' },
                                        { name: 'weight', type: 'float' }
                                      ]
                                    }
                                  ]
                                }
                              }
                            ])
    assert_nothing_raised { validate!(schema, 'fruits' => [{ 'name' => 'apple', 'weight' => 30.2 }]) }
    assert_nothing_raised { validate_simple!(schema, 'fruits' => [{ 'name' => 'apple', 'weight' => 30.2 }]) }

    assert_failed_validation('at .fruits[0].name expected type string, got null') do
      validate!(schema, 'fruits' => [{ 'name' => nil, 'weight' => 30.2 }])
    end
    assert_nothing_raised { validate_simple!(schema, 'fruits' => [{ 'name' => nil, 'weight' => 30.2 }]) }

    assert_failed_validation('at .fruits expected type array, got int with value 1') do
      validate!(schema, 'fruits' => 1)
    end
    assert_nothing_raised { validate_simple!(schema, 'fruits' => 1) }
  end

  def test_validate_multiple_errors
    schema = hash_to_schema(type: 'array',
                            name: 'ages',
                            items: [
                              { type: 'int', name: 'age' }
                            ])

    exception = assert_raise(Avro::SchemaValidator::ValidationError) do
      validate!(schema, [nil, 'e'])
    end
    assert_nothing_raised { validate_simple!(schema, [nil, 'e']) }
    assert_equal 2, exception.result.errors.size
    assert_equal(
      "at .[0] expected type int, got null\nat .[1] expected type int, got string with value \"e\"",
      exception.to_s
    )
  end

  def test_validate_extra_fields
    schema = hash_to_schema(
               type: 'record',
               name: 'fruits',
               fields: [
                 {
                   name: 'veggies',
                   type: 'string'
                 }
                     ]
    )
    exception = assert_raise(Avro::SchemaValidator::ValidationError) do
      validate!(schema, {'veggies' => 'tomato', 'bread' => 'rye'}, fail_on_extra_fields: true)
    end
    assert_equal(1, exception.result.errors.size)
    assert_equal("at . extra field 'bread' - not in schema",
                 exception.to_s)
  end

  def test_validate_subrecord_extra_fields
   schema = hash_to_schema(type: 'record',
                           name: 'top',
                           fields: [
                             {
                                name: 'fruit',
                                type: {
                                  name: 'fruit',
                                  type: 'record',
                                  fields: [{ name: 'name', type: 'string' }]
                                }
                             }
                           ])
    exception = assert_raise(Avro::SchemaValidator::ValidationError) do
      validate!(schema, { 'fruit' => { 'name' => 'orange', 'color' => 'orange' } }, fail_on_extra_fields: true)
    end
    assert_equal(1, exception.result.errors.size)
    assert_equal("at .fruit extra field 'color' - not in schema", exception.to_s)
  end

  def test_validate_array_extra_fields
    schema = hash_to_schema(type: 'array',
                            items: {
                              name: 'fruit',
                              type: 'record',
                              fields: [{ name: 'name', type: 'string' }]
                            })
    exception = assert_raise(Avro::SchemaValidator::ValidationError) do
      validate!(schema, [{ 'name' => 'orange', 'color' => 'orange' }], fail_on_extra_fields: true)
    end
    assert_equal(1, exception.result.errors.size)
    assert_equal("at .[0] extra field 'color' - not in schema", exception.to_s)
  end

  def test_validate_map_extra_fields
    schema = hash_to_schema(type: 'map',
                            values: {
                              name: 'fruit',
                              type: 'record',
                              fields: [{ name: 'color', type: 'string' }]
                            })
    exception = assert_raise(Avro::SchemaValidator::ValidationError) do
      validate!(schema, { 'apple' => { 'color' => 'green', 'extra' => 1 } }, fail_on_extra_fields: true)
    end
    assert_equal(1, exception.result.errors.size)
    assert_equal("at .apple extra field 'extra' - not in schema", exception.to_s)
  end

  def test_validate_union_extra_fields
    schema = hash_to_schema([
                              'null',
                              {
                                type: 'record',
                                name: 'fruit',
                                fields: [{ name: 'name', type: 'string' }]
                              }
                            ])
    exception = assert_raise(Avro::SchemaValidator::ValidationError) do
      validate!(schema, { 'name' => 'apple', 'color' => 'green' }, fail_on_extra_fields: true)
    end
    assert_equal(1, exception.result.errors.size)
    assert_equal("at . extra field 'color' - not in schema", exception.to_s)
  end

  def test_validate_bytes_decimal
    schema = hash_to_schema(type: 'bytes', logicalType: 'decimal', precision: 4, scale: 2)
    assert_valid_schema(schema, [BigDecimal('1.23'), 4.2, 1], ['4.2', BigDecimal('233.2')], true)

    schema = hash_to_schema(type: 'bytes', logicalType: 'decimal', precision: 4, scale: 4)
    assert_valid_schema(schema, [BigDecimal('0.2345'), 0.2, 0.1], ['4.2', BigDecimal('233.2')], true)

    schema = hash_to_schema(type: 'bytes', logicalType: 'decimal', precision: 4, scale: 0)
    assert_valid_schema(schema, [BigDecimal('123'), 2], ['4.2', BigDecimal('233.2')], true)

    schema = hash_to_schema(type: 'bytes', logicalType: 'decimal', precision: 4)
    assert_valid_schema(schema, [BigDecimal('123'), 2], ['4.2', BigDecimal('233.2')], true)
  end
end

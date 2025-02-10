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
require 'case_finder'

class TestSchemaNormalization < Test::Unit::TestCase
  def test_primitives
    %w[null boolean string bytes int long float double].each do |type|
      schema = Avro::Schema.parse(<<-JSON)
        { "type": "#{type}" }
      JSON

      canonical_form = Avro::SchemaNormalization.to_parsing_form(schema)

      assert_equal %("#{type}"), canonical_form
    end
  end

  def test_records
    schema = Avro::Schema.parse(<<-JSON)
      {
        "type": "record",
        "name": "test",
        "namespace": "random",
        "doc": "some record",
        "fields": [
          { "name": "height", "type": "int", "doc": "the height" }
        ]
      }
    JSON

    expected_type = <<-JSON.strip
      {"name":"random.test","type":"record","fields":[{"name":"height","type":"int"}]}
    JSON

    canonical_form = Avro::SchemaNormalization.to_parsing_form(schema)

    assert_equal expected_type, canonical_form
  end

  def test_recursive_records
    schema = Avro::Schema.parse(<<-JSON)
      {
        "type": "record",
        "name": "item",
        "fields": [
          { "name": "next", "type": "item" }
        ]
      }
    JSON

    expected_type = <<-JSON.strip
      {"name":"item","type":"record","fields":[{"name":"next","type":"item"}]}
    JSON

    canonical_form = Avro::SchemaNormalization.to_parsing_form(schema)

    assert_equal expected_type, canonical_form
  end

  def test_enums
    schema = Avro::Schema.parse(<<-JSON)
      {
        "type": "enum",
        "name": "suit",
        "namespace": "cards",
        "doc": "the different suits of cards",
        "symbols": ["club", "hearts", "diamond", "spades"]
      }
    JSON

    expected_type = <<-JSON.strip
      {"name":"cards.suit","type":"enum","symbols":["club","hearts","diamond","spades"]}
    JSON

    canonical_form = Avro::SchemaNormalization.to_parsing_form(schema)

    assert_equal expected_type, canonical_form
  end

  def test_fixed
    schema = Avro::Schema.parse(<<-JSON)
      {
        "type": "fixed",
        "name": "id",
        "namespace": "db",
        "size": 64
      }
    JSON

    expected_type = <<-JSON.strip
      {"name":"db.id","type":"fixed","size":64}
    JSON

    canonical_form = Avro::SchemaNormalization.to_parsing_form(schema)

    assert_equal expected_type, canonical_form
  end

  def test_arrays
    schema = Avro::Schema.parse(<<-JSON)
      {
        "type": "array",
        "doc": "the items",
        "items": "int"
      }
    JSON

    expected_type = <<-JSON.strip
      {"type":"array","items":"int"}
    JSON

    canonical_form = Avro::SchemaNormalization.to_parsing_form(schema)

    assert_equal expected_type, canonical_form
  end

  def test_maps
    schema = Avro::Schema.parse(<<-JSON)
      {
        "type": "map",
        "doc": "the items",
        "values": "int"
      }
    JSON

    expected_type = <<-JSON.strip
      {"type":"map","values":"int"}
    JSON

    canonical_form = Avro::SchemaNormalization.to_parsing_form(schema)

    assert_equal expected_type, canonical_form
  end

  def test_unions
    schema = Avro::Schema.parse(<<-JSON)
      ["int", "string"]
    JSON

    expected_type = <<-JSON.strip
      ["int","string"]
    JSON

    canonical_form = Avro::SchemaNormalization.to_parsing_form(schema)

    assert_equal expected_type, canonical_form
  end

  def test_shared_dataset
    CaseFinder.cases.each do |test_case|
      schema = Avro::Schema.parse(test_case.input)
      assert_equal test_case.canonical, Avro::SchemaNormalization.to_parsing_form(schema)
      assert_equal test_case.fingerprint, schema.crc_64_avro_fingerprint
    end
  end
end

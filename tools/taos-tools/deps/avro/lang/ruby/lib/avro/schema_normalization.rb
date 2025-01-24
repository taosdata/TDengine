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

module Avro
  class SchemaNormalization
    def self.to_parsing_form(schema)
      new.to_parsing_form(schema)
    end

    def initialize
      @processed_names = []
    end

    def to_parsing_form(schema)
      MultiJson.dump(normalize_schema(schema))
    end

    private

    def normalize_schema(schema)
      type = schema.type_sym.to_s

      if Schema::NAMED_TYPES.include?(type)
        if @processed_names.include?(schema.name)
          return schema.name
        else
          @processed_names << schema.name
        end
      end

      case type
      when *Schema::PRIMITIVE_TYPES
        type
      when "record"
        fields = schema.fields.map {|field| normalize_field(field) }

        normalize_named_type(schema, fields: fields)
      when "enum"
        normalize_named_type(schema, symbols: schema.symbols)
      when "fixed"
        normalize_named_type(schema, size: schema.size)
      when "array"
        { type: type, items: normalize_schema(schema.items) }
      when "map"
        { type: type, values: normalize_schema(schema.values) }
      when "union"
        if schema.schemas.nil?
          []
        else
          schema.schemas.map {|s| normalize_schema(s) }
        end
      else
        raise "unknown type #{type}"
      end
    end

    def normalize_field(field)
      {
        name: field.name,
        type: normalize_schema(field.type)
      }
    end

    def normalize_named_type(schema, attributes = {})
      name = Name.make_fullname(schema.name, schema.namespace)

      { name: name, type: schema.type_sym.to_s }.merge(attributes)
    end
  end
end

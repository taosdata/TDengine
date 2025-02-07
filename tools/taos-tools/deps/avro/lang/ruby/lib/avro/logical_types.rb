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

require 'date'
require 'bigdecimal'
require 'bigdecimal/util'

module Avro
  module LogicalTypes
    ##
    # Base class for logical types requiring a schema to be present
    class LogicalTypeWithSchema
      ##
      # @return [Avro::Schema] The schema this logical type is dealing with
      attr_reader :schema

      ##
      # Build a new instance of a logical type using the provided schema
      #
      # @param schema [Avro::Schema]
      #     The schema to use with this instance
      #
      # @raise [ArgumentError]
      #     If the provided schema is nil
      def initialize(schema)
        raise ArgumentError, 'schema is required' if schema.nil?

        @schema = schema
      end

      ##
      # Encode the provided datum
      #
      # @param datum [Object] The datum to encode
      #
      # @raise [NotImplementedError]
      #     Subclass will need to override this method
      def encode(datum)
        raise NotImplementedError
      end

      ##
      # Decode the provided datum
      #
      # @param datum [Object] The datum to decode
      #
      # @raise [NotImplementedError]
      #     Subclass will need to override this method
      def decode(datum)
        raise NotImplementedError
      end
    end

    ##
    # Logical type to handle arbitrary-precision decimals using byte array.
    #
    # The byte array contains the two's-complement representation of the unscaled integer
    # value in big-endian byte order.
    class BytesDecimal < LogicalTypeWithSchema
      # Messages for exceptions
      ERROR_INSUFFICIENT_PRECISION = 'Precision is too small'
      ERROR_ROUNDING_NECESSARY     = 'Rounding necessary'
      ERROR_VALUE_MUST_BE_NUMERIC  = 'value must be numeric'

      # The pattern used to pack up the byte array (8 bit unsigned integer/char)
      PACK_UNSIGNED_CHARS = 'C*'

      # The number 10 as BigDecimal
      TEN = BigDecimal(10).freeze

      ##
      # @return [Integer] The number of total digits supported by the decimal
      attr_reader :precision

      ##
      # @return [Integer] The number of fractional digits
      attr_reader :scale

      ##
      # Build a new decimal logical type
      #
      # @param schema [Avro::Schema]
      #     The schema defining precision and scale for the conversion
      def initialize(schema)
        super

        @scale     = schema.scale.to_i
        @precision = schema.precision.to_i
        @factor    = TEN ** @scale
      end

      ##
      # Encode the provided value into a byte array
      #
      # @param value [BigDecimal, Float, Integer]
      #     The numeric value to encode
      #
      # @raise [ArgumentError]
      #     If the provided value is not a numeric type
      #
      # @raise [RangeError]
      #     If the provided value has a scale higher than the schema permits,
      #     or does not fit into the schema's precision
      def encode(value)
        raise ArgumentError, ERROR_VALUE_MUST_BE_NUMERIC unless value.is_a?(Numeric)

        to_byte_array(unscaled_value(value.to_d)).pack(PACK_UNSIGNED_CHARS).freeze
      end

      ##
      # Decode a byte array (in form of a string) into a BigDecimal of the
      # given precision and scale
      #
      # @param stream [String]
      #     The byte array to decode
      #
      # @return [BigDecimal]
      def decode(stream)
        from_byte_array(stream) / @factor
      end

      private

      ##
      # Convert the provided stream of bytes into the unscaled value
      #
      # @param stream [String]
      #     The stream of bytes to convert
      #
      # @return [Integer]
      def from_byte_array(stream)
        bytes    = stream.bytes
        positive = bytes.first[7].zero?
        total    = 0

        bytes.each_with_index do |value, ix|
          total += (positive ? value : (value ^ 0xff)) << (bytes.length - ix - 1) * 8
        end

        return total if positive

        -(total + 1)
      end

      ##
      # Convert the provided number into its two's complement representation
      # in network order (big endian).
      #
      # @param number [Integer]
      #     The number to convert
      #
      # @return [Array<Integer>]
      #     The byte array in network order
      def to_byte_array(number)
        [].tap do |result|
          loop do
            result.unshift(number & 0xff)
            number >>= 8

            break if (number == 0 || number == -1) && (result.first[7] == number[7])
          end
        end
      end

      ##
      # Get the unscaled value from a BigDecimal considering the schema's scale
      #
      # @param decimal [BigDecimal]
      #     The decimal to get the unscaled value from
      #
      # @return [Integer]
      def unscaled_value(decimal)
        details = decimal.split
        length  = details[1].length

        fractional_part = length - details[3]
        raise RangeError, ERROR_ROUNDING_NECESSARY if fractional_part > scale

        if length > precision || (length - fractional_part) > (precision - scale)
          raise RangeError, ERROR_INSUFFICIENT_PRECISION
        end

        (decimal * @factor).to_i
      end
    end

    module IntDate
      EPOCH_START = Date.new(1970, 1, 1)

      def self.encode(date)
        return date.to_i if date.is_a?(Numeric)

        (date - EPOCH_START).to_i
      end

      def self.decode(int)
        EPOCH_START + int
      end
    end

    module TimestampMillis
      def self.encode(value)
        return value.to_i if value.is_a?(Numeric)

        time = value.to_time
        time.to_i * 1000 + time.usec / 1000
      end

      def self.decode(int)
        s, ms = int / 1000, int % 1000
        Time.at(s, ms * 1000).utc
      end
    end

    module TimestampMicros
      def self.encode(value)
        return value.to_i if value.is_a?(Numeric)

        time = value.to_time
        time.to_i * 1000_000 + time.usec
      end

      def self.decode(int)
        s, us = int / 1000_000, int % 1000_000
        Time.at(s, us).utc
      end
    end

    module Identity
      def self.encode(datum)
        datum
      end

      def self.decode(datum)
        datum
      end
    end

    TYPES = {
      "bytes" => {
        "decimal" => BytesDecimal
      },
      "int" => {
        "date" => IntDate
      },
      "long" => {
        "timestamp-millis" => TimestampMillis,
        "timestamp-micros" => TimestampMicros
      },
    }.freeze

    def self.type_adapter(type, logical_type, schema = nil)
      return unless logical_type

      adapter = TYPES.fetch(type, {}.freeze).fetch(logical_type, Identity)
      adapter.is_a?(Class) ? adapter.new(schema) : adapter
    end
  end
end

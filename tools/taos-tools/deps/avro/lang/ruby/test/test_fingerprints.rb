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

class TestFingerprints < Test::Unit::TestCase
  def test_md5_fingerprint
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "int" }
    SCHEMA

    assert_equal 318112854175969537208795771590915775282,
      schema.md5_fingerprint
  end

  def test_sha256_fingerprint
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "int" }
    SCHEMA

    assert_equal 28572620203319713300323544804233350633246234624932075150020181448463213378117,
      schema.sha256_fingerprint
  end

  def test_crc_64_avro_fingerprint
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "int" }
    SCHEMA

    assert_equal 8247732601305521295, # hex: 0x7275d51a3f395c8f
      schema.crc_64_avro_fingerprint
  end

  # This definitely belongs somewhere else
  def test_single_object_encoding_header
    schema = Avro::Schema.parse <<-SCHEMA
      { "type": "int" }
    SCHEMA

    assert_equal ["c3", "01", "8f", "5c", "39", "3f", "1a", "D5", "75", "72"].map{|e| e.to_i(16) },
      schema.single_object_encoding_header
  end
end

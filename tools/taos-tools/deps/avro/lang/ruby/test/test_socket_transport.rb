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

class TestSocketTransport < Test::Unit::TestCase
  def test_buffer_writing
    io = StringIO.new
    st = Avro::IPC::SocketTransport.new(io)
    buffer_length = "\000\000\000\006"  # 6 in big-endian
    message = 'abcdef'
    null_ending = "\000\000\000\000" # 0 in big-endian
    full = buffer_length + message + null_ending
    st.write_framed_message('abcdef')
    assert_equal full, io.string
  end

  def test_buffer_reading
    buffer_length = "\000\000\000\005" # 5 in big-endian
    message = "hello"
    null_ending = "\000\000\000\000" # 0 in big-endian
    full = buffer_length + message + null_ending
    io = StringIO.new(full)
    st = Avro::IPC::SocketTransport.new(io)
    assert_equal 'hello', st.read_framed_message
  end
end

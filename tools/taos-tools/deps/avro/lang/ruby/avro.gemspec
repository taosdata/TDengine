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

Gem::Specification.new do |s|
  s.name = "avro"
  s.version = File.read("lib/avro/VERSION.txt")
  s.authors = ["Apache Software Foundation"]
  s.email = "dev@avro.apache.org"

  s.summary = "Apache Avro for Ruby"
  s.description = "Avro is a data serialization and RPC format"
  s.homepage = "https://avro.apache.org/"
  s.license = "Apache-2.0"
  s.required_ruby_version = ">= 2.6"

  s.metadata["homepage_uri"] = s.homepage
  s.metadata["bug_tracker_uri"] = "https://issues.apache.org/jira/browse/AVRO"
  s.metadata["source_code_uri"] = "https://github.com/apache/avro"
  s.metadata["documentation_uri"] = "https://avro.apache.org/docs/#{s.version}/"

  files = File.read("Manifest").split("\n")
  s.files = files.reject { |f| f.start_with?("test/") }
  s.rdoc_options = ["--line-numbers", "--title", "Avro"]
  s.test_files = files.select { |f| f.start_with?("test/") }
  s.require_paths = ["lib"]

  s.add_dependency("multi_json", "~> 1.0")
end

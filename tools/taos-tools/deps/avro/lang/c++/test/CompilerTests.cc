/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sstream>

#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/unit_test.hpp>

#include "Compiler.hh"
#include "ValidSchema.hh"

// Assert that empty defaults don't make json schema compilation violate bounds
// checks, as they did in AVRO-1853. Please note that on Linux bounds are only
// checked in Debug builds (CMAKE_BUILD_TYPE=Debug).
void testEmptyBytesDefault() {
    std::string input = "{\n\
    \"type\": \"record\",\n\
    \"name\": \"testrecord\",\n\
    \"fields\": [\n\
        {\n\
            \"name\": \"testbytes\",\n\
            \"type\": \"bytes\",\n\
            \"default\": \"\"\n\
        }\n\
    ]\n\
}\n\
";
    std::string expected = "{\n\
    \"type\": \"record\",\n\
    \"name\": \"testrecord\",\n\
    \"fields\": [\n\
        {\n\
            \"name\": \"testbytes\",\n\
            \"type\": \"bytes\",\n\
            \"default\": \"\"\n\
        }\n\
    ]\n\
}\n\
";

    avro::ValidSchema schema = avro::compileJsonSchemaFromString(input);
    std::ostringstream actual;
    schema.toJson(actual);
    BOOST_CHECK_EQUAL(expected, actual.str());
}

void test2dArray() {
    std::string input = "{\n\
    \"type\": \"array\",\n\
    \"items\": {\n\
        \"type\": \"array\",\n\
        \"items\": \"double\"\n\
    }\n\
}\n";

    std::string expected = "{\n\
    \"type\": \"array\",\n\
    \"items\": {\n\
        \"type\": \"array\",\n\
        \"items\": \"double\"\n\
    }\n\
}\n\
";
    avro::ValidSchema schema = avro::compileJsonSchemaFromString(input);
    std::ostringstream actual;
    schema.toJson(actual);
    BOOST_CHECK_EQUAL(expected, actual.str());
}

boost::unit_test::test_suite *
init_unit_test_suite(int /*argc*/, char * /*argv*/[]) {
    using namespace boost::unit_test;

    auto *ts = BOOST_TEST_SUITE("Avro C++ unit tests for Compiler.cc");
    ts->add(BOOST_TEST_CASE(&testEmptyBytesDefault));
    ts->add(BOOST_TEST_CASE(&test2dArray));
    return ts;
}

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

#include "Compiler.hh"
#include "Decoder.hh"
#include "ValidSchema.hh"
#include <fstream>

#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/parameterized_test.hpp>

void testLargeSchema() {
    std::ifstream in("jsonschemas/large_schema.avsc");
    avro::ValidSchema vs;
    avro::compileJsonSchema(in, vs);
    avro::DecoderPtr d = avro::binaryDecoder();
    avro::DecoderPtr vd = avro::validatingDecoder(vs, d);
    avro::DecoderPtr rd = avro::resolvingDecoder(vs, vs, d);
}

boost::unit_test::test_suite *
init_unit_test_suite(int /*argc*/, char * /*argv*/[]) {
    using namespace boost::unit_test;

    auto *ts = BOOST_TEST_SUITE("Avro C++ unit tests for schemas");
    ts->add(BOOST_TEST_CASE(&testLargeSchema));
    return ts;
}

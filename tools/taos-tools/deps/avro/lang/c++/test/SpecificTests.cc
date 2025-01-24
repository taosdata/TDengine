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

#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/unit_test.hpp>

#include "Specific.hh"
#include "Stream.hh"

using std::array;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

namespace avro {

class C {
    int32_t i_;
    int64_t l_;

public:
    C() : i_(0), l_(0) {}
    C(int32_t i, int64_t l) : i_(i), l_(l) {}
    int32_t i() const { return i_; }
    int64_t l() const { return l_; }
    void i(int32_t ii) { i_ = ii; }
    void l(int64_t ll) { l_ = ll; }

    bool operator==(const C &oth) const {
        return i_ == oth.i_ && l_ == oth.l_;
    }
};

template<>
struct codec_traits<C> {
    static void encode(Encoder &e, const C &c) {
        e.encodeInt(c.i());
        e.encodeLong(c.l());
    }

    static void decode(Decoder &d, C &c) {
        c.i(d.decodeInt());
        c.l(d.decodeLong());
    }
};

namespace specific {

class Test {
    unique_ptr<OutputStream> os;
    EncoderPtr e;
    DecoderPtr d;

public:
    Test() : os(memoryOutputStream()), e(binaryEncoder()), d(binaryDecoder()) {
        e->init(*os);
    }

    template<typename T>
    void encode(const T &t) {
        avro::encode(*e, t);
        e->flush();
    }

    template<typename T>
    void decode(T &t) {
        unique_ptr<InputStream> is = memoryInputStream(*os);
        d->init(*is);
        avro::decode(*d, t);
    }
};

template<typename T>
T encodeAndDecode(const T &t) {
    Test tst;

    tst.encode(t);

    T actual = T();

    tst.decode(actual);
    return actual;
}

void testBool() {
    bool b = encodeAndDecode(true);
    BOOST_CHECK_EQUAL(b, true);
}

void testInt() {
    int32_t n = 10;
    int32_t b = encodeAndDecode(n);
    BOOST_CHECK_EQUAL(b, n);
}

void testLong() {
    int64_t n = -109;
    int64_t b = encodeAndDecode(n);
    BOOST_CHECK_EQUAL(b, n);
}

void testFloat() {
    float n = 10.19f;
    float b = encodeAndDecode(n);
    BOOST_CHECK_CLOSE(b, n, 0.00001f);
}

void testDouble() {
    double n = 10.00001;
    double b = encodeAndDecode(n);
    BOOST_CHECK_CLOSE(b, n, 0.00000001);
}

void testString() {
    string n = "abc";
    string b = encodeAndDecode(n);
    BOOST_CHECK_EQUAL(b, n);
}

void testBytes() {
    uint8_t values[] = {1, 7, 23, 47, 83};
    vector<uint8_t> n(values, values + 5);
    vector<uint8_t> b = encodeAndDecode(n);
    BOOST_CHECK_EQUAL_COLLECTIONS(b.begin(), b.end(), n.begin(), n.end());
}

void testFixed() {
    array<uint8_t, 5> n = {{1, 7, 23, 47, 83}};
    array<uint8_t, 5> b = encodeAndDecode(n);
    BOOST_CHECK_EQUAL_COLLECTIONS(b.begin(), b.end(), n.begin(), n.end());
}

void testArray() {
    int32_t values[] = {101, 709, 409, 34};
    vector<int32_t> n(values, values + 4);
    vector<int32_t> b = encodeAndDecode(n);

    BOOST_CHECK_EQUAL_COLLECTIONS(b.begin(), b.end(), n.begin(), n.end());
}

void testBoolArray() {
    bool values[] = {true, false, true, false};
    vector<bool> n(values, values + 4);
    vector<bool> b = encodeAndDecode(n);

    BOOST_CHECK_EQUAL_COLLECTIONS(b.begin(), b.end(), n.begin(), n.end());
}

void testMap() {
    map<string, int32_t> n;
    n["a"] = 1;
    n["b"] = 101;

    map<string, int32_t> b = encodeAndDecode(n);

    BOOST_CHECK(b == n);
}

void testCustom() {
    C n(10, 1023);
    C b = encodeAndDecode(n);
    BOOST_CHECK(b == n);
}

} // namespace specific
} // namespace avro

boost::unit_test::test_suite *
init_unit_test_suite(int /*argc*/, char * /*argv*/[]) {
    using namespace boost::unit_test;

    auto *ts = BOOST_TEST_SUITE("Specific tests");
    ts->add(BOOST_TEST_CASE(avro::specific::testBool));
    ts->add(BOOST_TEST_CASE(avro::specific::testInt));
    ts->add(BOOST_TEST_CASE(avro::specific::testLong));
    ts->add(BOOST_TEST_CASE(avro::specific::testFloat));
    ts->add(BOOST_TEST_CASE(avro::specific::testDouble));
    ts->add(BOOST_TEST_CASE(avro::specific::testString));
    ts->add(BOOST_TEST_CASE(avro::specific::testBytes));
    ts->add(BOOST_TEST_CASE(avro::specific::testFixed));
    ts->add(BOOST_TEST_CASE(avro::specific::testArray));
    ts->add(BOOST_TEST_CASE(avro::specific::testBoolArray));
    ts->add(BOOST_TEST_CASE(avro::specific::testMap));
    ts->add(BOOST_TEST_CASE(avro::specific::testCustom));
    return ts;
}

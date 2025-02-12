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

#include <iostream>

#include "Compiler.hh"
#include "Decoder.hh"
#include "Encoder.hh"
#include "Generic.hh"
#include "Specific.hh"
#include "ValidSchema.hh"

#include <boost/bind.hpp>
#include <cstdint>
#include <functional>
#include <stack>
#include <string>
#include <vector>

#include <boost/math/special_functions/fpclassify.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/parameterized_test.hpp>
#include <boost/test/unit_test.hpp>

namespace avro {

/*
void dump(const OutputStream& os)
{
    std::unique_ptr<InputStream> in = memoryInputStream(os);
    const char *b;
    size_t n;
    std::cout << os.byteCount() << std::endl;
    while (in->next(reinterpret_cast<const uint8_t**>(&b), &n)) {
        std::cout << std::string(b, n);
    }
    std::cout << std::endl;
}
*/

namespace parsing {

static const unsigned int count = 10;

/**
 * A bunch of tests that share quite a lot of infrastructure between them.
 * The basic idea is to generate avro data according to a schema and
 * then read back and compare the data with the original. But quite a few
 * variations are possible:
 * 1. While reading back, one can skip different data elements
 * 2. While reading resolve against a reader's schema. The resolver may
 * promote data type, convert from union to plain data type and vice versa,
 * insert or remove fields in records or reorder fields in a record.
 *
 * To test Json encoder and decoder, we use the same technqiue with only
 * one difference - we use JsonEncoder and JsonDecoder.
 *
 * We also use the same infrastructure to test GenericReader and GenericWriter.
 * In this case, avro binary is generated in the standard way. It is read
 * into a GenericDatum, which in turn is written out. This newly serialized
 * data is decoded in the standard way to check that it is what is written. The
 * last step won't work if there is schema for reading is different from
 * that for writing. This is because any reordering of fields would have
 * got fixed by the GenericDatum's decoding and encoding step.
 *
 * For most tests, the data is generated at random.
 */

using std::back_inserter;
using std::copy;
using std::istringstream;
using std::make_pair;
using std::ostringstream;
using std::pair;
using std::stack;
using std::string;
using std::unique_ptr;
using std::vector;

template<typename T>
T from_string(const std::string &s) {
    istringstream iss(s);
    T result;
    iss >> result;
    return result;
}

template<>
vector<uint8_t> from_string(const std::string &s) {
    vector<uint8_t> result;
    result.reserve(s.size());
    copy(s.begin(), s.end(), back_inserter(result));
    return result;
}

template<typename T>
std::string to_string(const T &t) {
    ostringstream oss;
    oss << t;
    return oss.str();
}

template<>
std::string to_string(const vector<uint8_t> &t) {
    string result;
    copy(t.begin(), t.end(), back_inserter(result));
    return result;
}

class Scanner {
    const char *p;
    const char *const end;

public:
    explicit Scanner(const char *calls) : p(calls), end(calls + strlen(calls)) {}
    Scanner(const char *calls, size_t len) : p(calls), end(calls + len) {}
    char advance() {
        return *p++;
    }

    int extractInt() {
        int result = 0;
        while (p < end) {
            if (isdigit(*p)) {
                result *= 10;
                result += *p++ - '0';
            } else {
                break;
            }
        }
        return result;
    }

    bool isDone() const { return p == end; }
};

boost::mt19937 rnd;

static string randomString(size_t len) {
    std::string result;
    result.reserve(len + 1);
    for (size_t i = 0; i < len; ++i) {
        auto c = static_cast<char>(rnd()) & 0x7f;
        if (c == '\0') {
            c = '\x7f';
        }
        result.push_back(c);
    }
    return result;
}

static vector<uint8_t> randomBytes(size_t len) {
    vector<uint8_t> result;
    result.reserve(len);
    for (size_t i = 0; i < len; ++i) {
        result.push_back(rnd());
    }
    return result;
}

static vector<string> randomValues(const char *calls) {
    Scanner sc(calls);
    vector<string> result;
    while (!sc.isDone()) {
        char c = sc.advance();
        switch (c) {
            case 'B': {
                result.push_back(to_string(rnd() % 2 == 0));
                break;
            }
            case 'I': {
                result.push_back(to_string(static_cast<int32_t>(rnd())));
                break;
            }
            case 'L': {
                result.push_back(to_string(rnd() | static_cast<int64_t>(rnd()) << 32));
                break;
            }
            case 'F': {
                result.push_back(
                    to_string(static_cast<float>(rnd()) / static_cast<float>(rnd())));
                break;
            }
            case 'D': {
                result.push_back(
                    to_string(static_cast<double>(rnd()) / static_cast<double>(rnd())));
                break;
            }
            case 'S':
            case 'K': {
                result.push_back(to_string(randomString(sc.extractInt())));
                break;
            }
            case 'b':
            case 'f': {
                result.push_back(to_string(randomBytes(sc.extractInt())));
                break;
            }
            case 'e':
            case 'c':
            case 'U': {
                sc.extractInt();
                break;
            }
            case 'N':
            case '[':
            case ']':
            case '{':
            case '}':
            case 's': {
                break;
            }
            default: {
                BOOST_FAIL("Unknown mnemonic: " << c);
            }
        }
    }
    return result;
}

static unique_ptr<OutputStream> generate(Encoder &e, const char *calls,
                                         const vector<string> &values) {
    Scanner sc(calls);
    auto it = values.begin();
    unique_ptr<OutputStream> ob = memoryOutputStream();
    e.init(*ob);

    while (!sc.isDone()) {
        char c = sc.advance();

        switch (c) {
            case 'N': {
                e.encodeNull();
                break;
            }
            case 'B': {
                e.encodeBool(from_string<bool>(*it++));
                break;
            }
            case 'I': {
                e.encodeInt(from_string<int32_t>(*it++));
                break;
            }
            case 'L': {
                e.encodeLong(from_string<int64_t>(*it++));
                break;
            }
            case 'F': {
                e.encodeFloat(from_string<float>(*it++));
                break;
            }
            case 'D': {
                e.encodeDouble(from_string<double>(*it++));
                break;
            }
            case 'S':
            case 'K': {
                sc.extractInt();
                e.encodeString(from_string<string>(*it++));
                break;
            }
            case 'b': {
                sc.extractInt();
                e.encodeBytes(from_string<vector<uint8_t>>(*it++));
                break;
            }
            case 'f': {
                sc.extractInt();
                e.encodeFixed(from_string<vector<uint8_t>>(*it++));
                break;
            }
            case 'e': {
                e.encodeEnum(sc.extractInt());
                break;
            }
            case '[': {
                e.arrayStart();
                break;
            }
            case ']': {
                e.arrayEnd();
                break;
            }
            case '{': {
                e.mapStart();
                break;
            }
            case '}': {
                e.mapEnd();
                break;
            }
            case 'c': {
                e.setItemCount(sc.extractInt());
                break;
            }
            case 's': {
                e.startItem();
                break;
            }
            case 'U': {
                e.encodeUnionIndex(sc.extractInt());
                break;
            }
            default: {
                BOOST_FAIL("Unknown mnemonic: " << c);
            }
        }
    }
    e.flush();
    return ob;
}

namespace {
struct StackElement {
    size_t size;
    size_t count;
    bool isArray;
    StackElement(size_t s, bool a) : size(s), count(0), isArray(a) {}
};
} // namespace

static vector<string>::const_iterator skipCalls(Scanner &sc, Decoder &d,
                                                vector<string>::const_iterator it, bool isArray) {
    char end = isArray ? ']' : '}';
    int level = 0;
    while (!sc.isDone()) {
        char c = sc.advance();
        switch (c) {
            case '[':
            case '{':
                ++level;
                break;
            case ']':
            case '}':
                if (c == end && level == 0) {
                    return it;
                }
                --level;
                break;
            case 'B':
            case 'I':
            case 'L':
            case 'F':
            case 'D':
                ++it;
                break;
            case 'S':
            case 'K':
            case 'b':
            case 'f':
            case 'e': ++it; // Fall through.
            case 'c':
            case 'U':
                sc.extractInt();
                break;
            case 's':
            case 'N':
            case 'R': break;
            default: BOOST_FAIL("Don't know how to skip: " << c);
        }
    }
    BOOST_FAIL("End reached while trying to skip");
    throw 0; // Just to keep the compiler happy.
}

static void check(Decoder &d, unsigned int skipLevel,
                  const char *calls, const vector<string> &values) {
    const size_t zero = 0;
    Scanner sc(calls);
    stack<StackElement> containerStack;
    auto it = values.begin();
    while (!sc.isDone()) {
        char c = sc.advance();
        switch (c) {
            case 'N':
                d.decodeNull();
                break;
            case 'B': {
                bool b1 = d.decodeBool();
                bool b2 = from_string<bool>(*it++);
                BOOST_CHECK_EQUAL(b1, b2);
            } break;
            case 'I': {
                int32_t b1 = d.decodeInt();
                auto b2 = from_string<int32_t>(*it++);
                BOOST_CHECK_EQUAL(b1, b2);
            } break;
            case 'L': {
                int64_t b1 = d.decodeLong();
                auto b2 = from_string<int64_t>(*it++);
                BOOST_CHECK_EQUAL(b1, b2);
            } break;
            case 'F': {
                float b1 = d.decodeFloat();
                auto b2 = from_string<float>(*it++);
                BOOST_CHECK_CLOSE(b1, b2, 0.001f);
            } break;
            case 'D': {
                double b1 = d.decodeDouble();
                auto b2 = from_string<double>(*it++);
                BOOST_CHECK_CLOSE(b1, b2, 0.001f);
            } break;
            case 'S':
            case 'K':
                sc.extractInt();
                if (containerStack.size() >= skipLevel) {
                    d.skipString();
                } else {
                    string b1 = d.decodeString();
                    string b2 = from_string<string>(*it);
                    BOOST_CHECK_EQUAL(b1, b2);
                }
                ++it;
                break;
            case 'b':
                sc.extractInt();
                if (containerStack.size() >= skipLevel) {
                    d.skipBytes();
                } else {
                    vector<uint8_t> b1 = d.decodeBytes();
                    vector<uint8_t> b2 = from_string<vector<uint8_t>>(*it);
                    BOOST_CHECK_EQUAL_COLLECTIONS(b1.begin(), b1.end(),
                                                  b2.begin(), b2.end());
                }
                ++it;
                break;
            case 'f': {
                size_t len = sc.extractInt();
                if (containerStack.size() >= skipLevel) {
                    d.skipFixed(len);
                } else {
                    vector<uint8_t> b1 = d.decodeFixed(len);
                    vector<uint8_t> b2 = from_string<vector<uint8_t>>(*it);
                    BOOST_CHECK_EQUAL_COLLECTIONS(b1.begin(), b1.end(),
                                                  b2.begin(), b2.end());
                }
            }
                ++it;
                break;
            case 'e': {
                size_t b1 = d.decodeEnum();
                size_t b2 = sc.extractInt();
                BOOST_CHECK_EQUAL(b1, b2);
            } break;
            case '[':
                if (containerStack.size() >= skipLevel) {
                    size_t n = d.skipArray();
                    if (n == 0) {
                        it = skipCalls(sc, d, it, true);
                    } else {
                        containerStack.push(StackElement(n, true));
                    }
                } else {
                    containerStack.push(StackElement(d.arrayStart(), true));
                }
                break;
            case '{':
                if (containerStack.size() >= skipLevel) {
                    size_t n = d.skipMap();
                    if (n == 0) {
                        it = skipCalls(sc, d, it, false);
                    } else {
                        containerStack.push(StackElement(n, false));
                    }
                } else {
                    containerStack.push(StackElement(d.mapStart(), false));
                }
                break;
            case ']': {
                const StackElement &se = containerStack.top();
                BOOST_CHECK_EQUAL(se.size, se.count);
                if (se.size != 0) {
                    BOOST_CHECK_EQUAL(zero, d.arrayNext());
                }
                containerStack.pop();
            } break;
            case '}': {
                const StackElement &se = containerStack.top();
                BOOST_CHECK_EQUAL(se.size, se.count);
                if (se.size != 0) {
                    BOOST_CHECK_EQUAL(zero, d.mapNext());
                }
                containerStack.pop();
            } break;
            case 's': {
                StackElement &se = containerStack.top();
                if (se.size == se.count) {
                    se.size += (se.isArray ? d.arrayNext() : d.mapNext());
                }
                ++se.count;
            } break;
            case 'c':
                sc.extractInt();
                break;
            case 'U': {
                size_t idx = sc.extractInt();
                BOOST_CHECK_EQUAL(idx, d.decodeUnionIndex());
            } break;
            case 'R':
                static_cast<ResolvingDecoder &>(d).fieldOrder();
                continue;
            default: BOOST_FAIL("Unknown mnemonic: " << c);
        }
    }
    BOOST_CHECK(it == values.end());
}

ValidSchema makeValidSchema(const char *schema) {
    istringstream iss(schema);
    ValidSchema vs;
    compileJsonSchema(iss, vs);
    return ValidSchema(vs);
}

void testEncoder(const EncoderPtr &e, const char *writerCalls,
                 vector<string> &v, unique_ptr<OutputStream> &p) {
    v = randomValues(writerCalls);
    p = generate(*e, writerCalls, v);
}

static void testDecoder(const DecoderPtr &d,
                        const vector<string> &values, InputStream &data,
                        const char *readerCalls, unsigned int skipLevel) {
    d->init(data);
    check(*d, skipLevel, readerCalls, values);
}

/**
 * The first member is a schema.
 * The second one is a sequence of (single character) mnemonics:
 * N  null
 * B  boolean
 * I  int
 * L  long
 * F  float
 * D  double
 * K followed by integer - key-name (and its length) in a map
 * S followed by integer - string and its length
 * b followed by integer - bytes and length
 * f followed by integer - fixed and length
 * c  Number of items to follow in an array/map.
 * U followed by integer - Union and its branch
 * e followed by integer - Enum and its value
 * [  Start array
 * ]  End array
 * {  Start map
 * }  End map
 * s  start item
 * R  Start of record in resolving situations. Client may call fieldOrder()
 */

struct TestData {
    const char *schema;
    const char *calls;
    unsigned int depth;
};

struct TestData2 {
    const char *schema;
    const char *correctCalls;
    const char *incorrectCalls;
    unsigned int depth;
};

struct TestData3 {
    const char *writerSchema;
    const char *writerCalls;
    const char *readerSchema;
    const char *readerCalls;
    unsigned int depth;
};

struct TestData4 {
    const char *writerSchema;
    const char *writerCalls;
    const char *writerValues[100];
    const char *readerSchema;
    const char *readerCalls;
    const char *readerValues[100];
    unsigned int depth;
    size_t recordCount;
};

void appendSentinel(OutputStream &os) {
    uint8_t *buf;
    size_t len;
    os.next(&buf, &len);
    *buf = '~';
    os.backup(len - 1);
}

void assertSentinel(InputStream &is) {
    const uint8_t *buf;
    size_t len;
    is.next(&buf, &len);
    if (len > 1) {
        for (size_t i = 0; i < len; i++) {
            std::cout << static_cast<int>(buf[i]) << std::endl;
        }
    }
    BOOST_REQUIRE_EQUAL(len, 1);
    BOOST_CHECK_EQUAL(*buf, '~');
}

template<typename CodecFactory>
void testCodec(const TestData &td) {
    static int testNo = 0;
    testNo++;

    ValidSchema vs = makeValidSchema(td.schema);

    for (unsigned int i = 0; i < count; ++i) {
        vector<string> v;
        unique_ptr<OutputStream> p;
        testEncoder(CodecFactory::newEncoder(vs), td.calls, v, p);
        appendSentinel(*p);

        // dump(*p);

        for (unsigned int j = 0; j <= td.depth; ++j) {
            unsigned int skipLevel = td.depth - j;
            BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                           << " schema: " << td.schema
                                           << " calls: " << td.calls
                                           << " skip-level: " << skipLevel);
            unique_ptr<InputStream> in = memoryInputStream(*p);
            DecoderPtr d = CodecFactory::newDecoder(vs);
            testDecoder(d, v, *in, td.calls, skipLevel);
            d->drain();
            assertSentinel(*in);
        }
    }
}

template<typename CodecFactory>
void testCodecResolving(const TestData3 &td) {
    static int testNo = 0;
    testNo++;

    BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                   << " writer schema: " << td.writerSchema
                                   << " writer calls: " << td.writerCalls
                                   << " reader schema: " << td.readerSchema
                                   << " reader calls: " << td.readerCalls);

    ValidSchema vs = makeValidSchema(td.writerSchema);

    for (unsigned int i = 0; i < count; ++i) {
        vector<string> v;
        unique_ptr<OutputStream> p;
        testEncoder(CodecFactory::newEncoder(vs), td.writerCalls, v, p);
        appendSentinel(*p);
        // dump(*p);

        ValidSchema rvs = makeValidSchema(td.readerSchema);
        for (unsigned int j = 0; j <= td.depth; ++j) {
            unsigned int skipLevel = td.depth - j;
            BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                           << " writer schema: " << td.writerSchema
                                           << " writer calls: " << td.writerCalls
                                           << " reader schema: " << td.readerSchema
                                           << " reader calls: " << td.readerCalls
                                           << " skip-level: " << skipLevel);
            unique_ptr<InputStream> in = memoryInputStream(*p);
            DecoderPtr d = CodecFactory::newDecoder(vs, rvs);
            testDecoder(d, v, *in, td.readerCalls, skipLevel);
            d->drain();
            assertSentinel(*in);
        }
    }
}

static vector<string> mkValues(const char *const values[]) {
    vector<string> result;
    for (const char *const *p = values; *p; ++p) {
        result.emplace_back(*p);
    }
    return result;
}

template<typename CodecFactory>
void testCodecResolving2(const TestData4 &td) {
    static int testNo = 0;
    testNo++;

    BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                   << " writer schema: " << td.writerSchema
                                   << " writer calls: " << td.writerCalls
                                   << " reader schema: " << td.readerSchema
                                   << " reader calls: " << td.readerCalls);

    ValidSchema vs = makeValidSchema(td.writerSchema);

    vector<string> wd = mkValues(td.writerValues);
    unique_ptr<OutputStream> p =
        generate(*CodecFactory::newEncoder(vs), td.writerCalls, wd);
    appendSentinel(*p);
    // dump(*p);

    ValidSchema rvs = makeValidSchema(td.readerSchema);
    vector<string> rd = mkValues(td.readerValues);
    for (unsigned int i = 0; i <= td.depth; ++i) {
        unsigned int skipLevel = td.depth - i;
        BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                       << " writer schema: " << td.writerSchema
                                       << " writer calls: " << td.writerCalls
                                       << " reader schema: " << td.readerSchema
                                       << " reader calls: " << td.readerCalls
                                       << " skip-level: " << skipLevel);
        unique_ptr<InputStream> in = memoryInputStream(*p);
        DecoderPtr d = CodecFactory::newDecoder(vs, rvs);
        testDecoder(d, rd, *in, td.readerCalls, skipLevel);
        d->drain();
        assertSentinel(*in);
    }
}

template<typename CodecFactory>
void testReaderFail(const TestData2 &td) {
    static int testNo = 0;
    testNo++;
    BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                   << " schema: " << td.schema
                                   << " correctCalls: " << td.correctCalls
                                   << " incorrectCalls: " << td.incorrectCalls
                                   << " skip-level: " << td.depth);
    ValidSchema vs = makeValidSchema(td.schema);

    vector<string> v;
    unique_ptr<OutputStream> p;
    testEncoder(CodecFactory::newEncoder(vs), td.correctCalls, v, p);
    appendSentinel(*p);
    unique_ptr<InputStream> in = memoryInputStream(*p);
    DecoderPtr d = CodecFactory::newDecoder(vs);
    BOOST_CHECK_THROW(
        testDecoder(d, v, *in, td.incorrectCalls, td.depth), Exception);
}

template<typename CodecFactory>
void testWriterFail(const TestData2 &td) {
    static int testNo = 0;
    testNo++;
    BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                   << " schema: " << td.schema
                                   << " incorrectCalls: " << td.incorrectCalls);
    ValidSchema vs = makeValidSchema(td.schema);

    vector<string> v;
    unique_ptr<OutputStream> p;
    BOOST_CHECK_THROW(testEncoder(CodecFactory::newEncoder(vs),
                                  td.incorrectCalls, v, p),
                      Exception);
}

template<typename CodecFactory>
void testGeneric(const TestData &td) {
    static int testNo = 0;
    testNo++;
    BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                   << " schema: " << td.schema
                                   << " calls: " << td.calls);

    ValidSchema vs = makeValidSchema(td.schema);

    for (unsigned int i = 0; i < count; ++i) {
        vector<string> v;
        unique_ptr<OutputStream> p;
        testEncoder(CodecFactory::newEncoder(vs), td.calls, v, p);
        appendSentinel(*p);
        // dump(*p);
        DecoderPtr d1 = CodecFactory::newDecoder(vs);
        unique_ptr<InputStream> in1 = memoryInputStream(*p);
        d1->init(*in1);
        GenericDatum datum(vs);
        avro::decode(*d1, datum);
        d1->drain();
        assertSentinel(*in1);

        EncoderPtr e2 = CodecFactory::newEncoder(vs);
        unique_ptr<OutputStream> ob = memoryOutputStream();
        e2->init(*ob);

        avro::encode(*e2, datum);
        e2->flush();
        appendSentinel(*ob);

        BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                       << " schema: " << td.schema
                                       << " calls: " << td.calls);
        unique_ptr<InputStream> in2 = memoryInputStream(*ob);
        DecoderPtr d2 = CodecFactory::newDecoder(vs);
        testDecoder(d2, v, *in2, td.calls, td.depth);
        d2->drain();
        assertSentinel(*in2);
    }
}

template<typename CodecFactory>
void testGenericResolving(const TestData3 &td) {
    static int testNo = 0;
    testNo++;

    BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                   << " writer schema: " << td.writerSchema
                                   << " writer calls: " << td.writerCalls
                                   << " reader schema: " << td.readerSchema
                                   << " reader calls: " << td.readerCalls);

    ValidSchema wvs = makeValidSchema(td.writerSchema);
    ValidSchema rvs = makeValidSchema(td.readerSchema);

    for (unsigned int i = 0; i < count; ++i) {
        vector<string> v;
        unique_ptr<OutputStream> p;
        testEncoder(CodecFactory::newEncoder(wvs), td.writerCalls, v, p);
        appendSentinel(*p);
        // dump(*p);
        DecoderPtr d1 = CodecFactory::newDecoder(wvs);
        unique_ptr<InputStream> in1 = memoryInputStream(*p);
        d1->init(*in1);

        GenericReader gr(wvs, rvs, d1);
        GenericDatum datum;
        gr.read(datum);
        d1->drain();
        assertSentinel(*in1);

        EncoderPtr e2 = CodecFactory::newEncoder(rvs);
        unique_ptr<OutputStream> ob = memoryOutputStream();
        e2->init(*ob);
        avro::encode(*e2, datum);
        e2->flush();
        appendSentinel(*ob);

        BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                       << " writer-schemai " << td.writerSchema
                                       << " writer-calls: " << td.writerCalls
                                       << " writer-calls: " << td.writerCalls
                                       << " reader-schema: " << td.readerSchema
                                       << " calls: " << td.readerCalls);
        unique_ptr<InputStream> in2 = memoryInputStream(*ob);
        testDecoder(CodecFactory::newDecoder(rvs), v, *in2,
                    td.readerCalls, td.depth);
    }
}

template<typename CodecFactory>
void testGenericResolving2(const TestData4 &td) {
    static int testNo = 0;
    testNo++;

    BOOST_TEST_CHECKPOINT("Test: " << testNo << ' '
                                   << " writer schema: " << td.writerSchema
                                   << " writer calls: " << td.writerCalls
                                   << " reader schema: " << td.readerSchema
                                   << " reader calls: " << td.readerCalls);

    ValidSchema wvs = makeValidSchema(td.writerSchema);
    ValidSchema rvs = makeValidSchema(td.readerSchema);

    const vector<string> wd = mkValues(td.writerValues);

    unique_ptr<OutputStream> p = generate(*CodecFactory::newEncoder(wvs),
                                          td.writerCalls, wd);
    // dump(*p);
    DecoderPtr d1 = CodecFactory::newDecoder(wvs);
    unique_ptr<InputStream> in1 = memoryInputStream(*p);
    d1->init(*in1);

    GenericReader gr(wvs, rvs, d1);
    GenericDatum datum;
    gr.read(datum);

    EncoderPtr e2 = CodecFactory::newEncoder(rvs);
    unique_ptr<OutputStream> ob = memoryOutputStream();
    e2->init(*ob);
    avro::encode(*e2, datum);
    e2->flush();
    // We cannot verify with the reader calls because they are for
    // the resolving decoder and hence could be in a different order than
    // the "normal" data.
}

static const TestData data[] = {
    {"\"null\"", "N", 1},
    {"\"boolean\"", "B", 1},
    {"\"int\"", "I", 1},
    {"\"long\"", "L", 1},
    {"\"float\"", "F", 1},
    {"\"double\"", "D", 1},
    {"\"string\"", "S0", 1},
    {"\"string\"", "S10", 1},
    {"\"bytes\"", "b0", 1},
    {"\"bytes\"", "b10", 1},

    {R"({"type":"fixed", "name":"fi", "size": 1})", "f1", 1},
    {R"({"type":"fixed", "name":"fi", "size": 10})", "f10", 1},
    {R"({"type":"enum", "name":"en", "symbols":["v1", "v2"]})",
     "e1", 1},

    {R"({"type":"array", "items": "boolean"})", "[]", 2},
    {R"({"type":"array", "items": "int"})", "[]", 2},
    {R"({"type":"array", "items": "long"})", "[]", 2},
    {R"({"type":"array", "items": "float"})", "[]", 2},
    {R"({"type":"array", "items": "double"})", "[]", 2},
    {R"({"type":"array", "items": "string"})", "[]", 2},
    {R"({"type":"array", "items": "bytes"})", "[]", 2},
    {"{\"type\":\"array\", \"items\":{\"type\":\"fixed\", "
     "\"name\":\"fi\", \"size\": 10}}",
     "[]", 2},

    {R"({"type":"array", "items": "boolean"})", "[c1sB]", 2},
    {R"({"type":"array", "items": "int"})", "[c1sI]", 2},
    {R"({"type":"array", "items": "long"})", "[c1sL]", 2},
    {R"({"type":"array", "items": "float"})", "[c1sF]", 2},
    {R"({"type":"array", "items": "double"})", "[c1sD]", 2},
    {R"({"type":"array", "items": "string"})", "[c1sS10]", 2},
    {R"({"type":"array", "items": "bytes"})", "[c1sb10]", 2},
    {R"({"type":"array", "items": "int"})", "[c1sIc1sI]", 2},
    {R"({"type":"array", "items": "int"})", "[c2sIsI]", 2},
    {"{\"type\":\"array\", \"items\":{\"type\":\"fixed\", "
     "\"name\":\"fi\", \"size\": 10}}",
     "[c2sf10sf10]", 2},

    {R"({"type":"map", "values": "boolean"})", "{}", 2},
    {R"({"type":"map", "values": "int"})", "{}", 2},
    {R"({"type":"map", "values": "long"})", "{}", 2},
    {R"({"type":"map", "values": "float"})", "{}", 2},
    {R"({"type":"map", "values": "double"})", "{}", 2},
    {R"({"type":"map", "values": "string"})", "{}", 2},
    {R"({"type":"map", "values": "bytes"})", "{}", 2},
    {"{\"type\":\"map\", \"values\": "
     "{\"type\":\"array\", \"items\":\"int\"}}",
     "{}", 2},

    {R"({"type":"map", "values": "boolean"})", "{c1sK5B}", 2},
    {R"({"type":"map", "values": "int"})", "{c1sK5I}", 2},
    {R"({"type":"map", "values": "long"})", "{c1sK5L}", 2},
    {R"({"type":"map", "values": "float"})", "{c1sK5F}", 2},
    {R"({"type":"map", "values": "double"})", "{c1sK5D}", 2},
    {R"({"type":"map", "values": "string"})", "{c1sK5S10}", 2},
    {R"({"type":"map", "values": "bytes"})", "{c1sK5b10}", 2},
    {"{\"type\":\"map\", \"values\": "
     "{\"type\":\"array\", \"items\":\"int\"}}",
     "{c1sK5[c3sIsIsI]}", 2},

    {R"({"type":"map", "values": "boolean"})",
     "{c1sK5Bc2sK5BsK5B}", 2},

    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"boolean\"}]}",
     "B", 1},
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"int\"}]}",
     "I", 1},
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"long\"}]}",
     "L", 1},
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"float\"}]}",
     "F", 1},
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"double\"}]}",
     "D", 1},
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"string\"}]}",
     "S10", 1},
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"bytes\"}]}",
     "b10", 1},

    // multi-field records
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"int\"},"
     "{\"name\":\"f2\", \"type\":\"double\"},"
     "{\"name\":\"f3\", \"type\":\"string\"}]}",
     "IDS10", 1},
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f0\", \"type\":\"null\"},"
     "{\"name\":\"f1\", \"type\":\"boolean\"},"
     "{\"name\":\"f2\", \"type\":\"int\"},"
     "{\"name\":\"f3\", \"type\":\"long\"},"
     "{\"name\":\"f4\", \"type\":\"float\"},"
     "{\"name\":\"f5\", \"type\":\"double\"},"
     "{\"name\":\"f6\", \"type\":\"string\"},"
     "{\"name\":\"f7\", \"type\":\"bytes\"}]}",
     "NBILFDS10b25", 1},
    // record of records
    {"{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
     "{\"name\":\"f1\", \"type\":{\"type\":\"record\", "
     "\"name\":\"inner\", \"fields\":["
     "{\"name\":\"g1\", \"type\":\"int\"}, {\"name\":\"g2\", "
     "\"type\":\"double\"}]}},"
     "{\"name\":\"f2\", \"type\":\"string\"},"
     "{\"name\":\"f3\", \"type\":\"inner\"}]}",
     "IDS10ID", 1},

    // record with name references
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":{\"type\":\"fixed\", "
     "\"name\":\"f\", \"size\":10 }},"
     "{\"name\":\"f2\", \"type\":\"f\"},"
     "{\"name\":\"f3\", \"type\":\"f\"}]}",
     "f10f10f10", 1},
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":{\"type\":\"enum\", "
     "\"name\": \"e\", \"symbols\":[\"s1\", \"s2\"] }},"
     "{\"name\":\"f2\", \"type\":\"e\"},"
     "{\"name\":\"f3\", \"type\":\"e\"}]}",
     "e1e0e1", 1},

    // record with array
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\"},"
     "{\"name\":\"f2\", "
     "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}",
     "L[c1sI]", 2},

    // record with map
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\"},"
     "{\"name\":\"f2\", "
     "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}",
     "L{c1sK5I}", 2},

    // array of records
    {"{\"type\":\"array\", \"items\":"
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\"},"
     "{\"name\":\"f2\", \"type\":\"null\"}]}}",
     "[c2sLNsLN]", 2},

    {"{\"type\":\"array\", \"items\":"
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\"},"
     "{\"name\":\"f2\", "
     "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}}",
     "[c2sL[c1sI]sL[c2sIsI]]", 3},
    {"{\"type\":\"array\", \"items\":"
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\"},"
     "{\"name\":\"f2\", "
     "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}}",
     "[c2sL{c1sK5I}sL{c2sK5IsK5I}]", 3},
    {"{\"type\":\"array\", \"items\":"
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\"},"
     "{\"name\":\"f2\", "
     "\"type\":[\"null\", \"int\"]}]}}",
     "[c2sLU0NsLU1I]", 2},

    {R"(["boolean", "null" ])", "U0B", 1},
    {R"(["int", "null" ])", "U0I", 1},
    {R"(["long", "null" ])", "U0L", 1},
    {R"(["float", "null" ])", "U0F", 1},
    {R"(["double", "null" ])", "U0D", 1},
    {R"(["string", "null" ])", "U0S10", 1},
    {R"(["bytes", "null" ])", "U0b10", 1},

    {R"(["null", "int"])", "U0N", 1},
    {R"(["boolean", "int"])", "U0B", 1},
    {R"(["boolean", "int"])", "U1I", 1},
    {R"(["boolean", {"type":"array", "items":"int"} ])",
     "U0B", 1},

    {R"(["boolean", {"type":"array", "items":"int"} ])",
     "U1[c1sI]", 2},

    // Recursion
    {"{\"type\": \"record\", \"name\": \"Node\", \"fields\": ["
     "{\"name\":\"label\", \"type\":\"string\"},"
     "{\"name\":\"children\", \"type\":"
     "{\"type\": \"array\", \"items\": \"Node\" }}]}",
     "S10[c1sS10[]]", 3},

    {"{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
     "{\"name\":\"value\", \"type\":[\"null\", \"string\","
     "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
     "{\"name\":\"car\", \"type\":\"Lisp\"},"
     "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}",
     "U0N", 1},
    {"{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
     "{\"name\":\"value\", \"type\":[\"null\", \"string\","
     "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
     "{\"name\":\"car\", \"type\":\"Lisp\"},"
     "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}",
     "U1S10", 1},
    {"{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
     "{\"name\":\"value\", \"type\":[\"null\", \"string\","
     "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
     "{\"name\":\"car\", \"type\":\"Lisp\"},"
     "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}",
     "U2U1S10U0N", 1},
    {"{" // https://issues.apache.org/jira/browse/AVRO-1635
     "  \"name\": \"Container\","
     "  \"type\": \"record\","
     "  \"fields\": [{"
     "    \"name\": \"field\","
     "    \"type\": {"
     "      \"name\": \"Object\","
     "      \"type\": \"record\","
     "      \"fields\": [{"
     "        \"name\": \"value\","
     "        \"type\": ["
     "          \"string\","
     "          {\"type\": \"map\", \"values\": \"Object\"}"
     "        ]"
     "      }]"
     "    }"
     "  }]"
     "}",
     "U1{c1sK1U0S1c2sK2U1{c2sK4U1{c1sK6U1{}}sK5U1{}}sK3U0S3}", 5},
    {"{"
     "  \"name\": \"Container\","
     "  \"type\": \"record\","
     "  \"fields\": [{"
     "    \"name\": \"tree_A\","
     "    \"type\": {"
     "      \"name\": \"ArrayTree\","
     "      \"type\": \"record\","
     "      \"fields\": [{"
     "        \"name\": \"label\","
     "        \"type\": \"long\""
     "      }, {"
     "        \"name\": \"children\","
     "        \"type\": {"
     "          \"type\": \"array\","
     "          \"items\": {"
     "            \"name\": \"MapTree\","
     "            \"type\": \"record\","
     "            \"fields\": [{"
     "              \"name\": \"label\","
     "              \"type\": \"int\""
     "            }, {"
     "              \"name\": \"children\","
     "              \"type\": {"
     "                \"type\": \"map\","
     "                \"values\": \"ArrayTree\""
     "              }"
     "            }]"
     "          }"
     "        }"
     "      }]"
     "    }"
     "  }, {"
     "    \"name\": \"tree_B\","
     "    \"type\": \"MapTree\""
     "  }]"
     "}",
     "L[c1sI{c1sK3L[]c2sK4L[c1sI{c1sK6L[c2sI{}sI{}]}]sK5L[]}]I{c2sK1L[]sK2L[]}",
     7},
};

static const TestData2 data2[] = {
    {"\"int\"", "I", "B", 1},
    {"\"boolean\"", "B", "I", 1},
    {"\"boolean\"", "B", "L", 1},
    {"\"boolean\"", "B", "F", 1},
    {"\"boolean\"", "B", "D", 1},
    {"\"boolean\"", "B", "S10", 1},
    {"\"boolean\"", "B", "b10", 1},
    {"\"boolean\"", "B", "[]", 1},
    {"\"boolean\"", "B", "{}", 1},
    {"\"boolean\"", "B", "U0", 1},
    {R"({"type":"fixed", "name":"fi", "size": 1})", "f1", "f2", 1},
};

static const TestData3 data3[] = {
    {"\"int\"", "I", "\"float\"", "F", 1},
    {"\"int\"", "I", "\"double\"", "D", 1},
    {"\"int\"", "I", "\"long\"", "L", 1},
    {"\"long\"", "L", "\"float\"", "F", 1},
    {"\"long\"", "L", "\"double\"", "D", 1},
    {"\"float\"", "F", "\"double\"", "D", 1},

    {R"({"type":"array", "items": "int"})", "[]",
     R"({"type":"array", "items": "long"})", "[]", 2},
    {R"({"type":"array", "items": "int"})", "[]",
     R"({"type":"array", "items": "double"})", "[]", 2},
    {R"({"type":"array", "items": "long"})", "[]",
     R"({"type":"array", "items": "double"})", "[]", 2},
    {R"({"type":"array", "items": "float"})", "[]",
     R"({"type":"array", "items": "double"})", "[]", 2},

    {R"({"type":"array", "items": "int"})", "[c1sI]",
     R"({"type":"array", "items": "long"})", "[c1sL]", 2},
    {R"({"type":"array", "items": "int"})", "[c1sI]",
     R"({"type":"array", "items": "double"})", "[c1sD]", 2},
    {R"({"type":"array", "items": "long"})", "[c1sL]",
     R"({"type":"array", "items": "double"})", "[c1sD]", 2},
    {R"({"type":"array", "items": "float"})", "[c1sF]",
     R"({"type":"array", "items": "double"})", "[c1sD]", 2},

    {R"({"type":"map", "values": "int"})", "{}",
     R"({"type":"map", "values": "long"})", "{}", 2},
    {R"({"type":"map", "values": "int"})", "{}",
     R"({"type":"map", "values": "double"})", "{}", 2},
    {R"({"type":"map", "values": "long"})", "{}",
     R"({"type":"map", "values": "double"})", "{}", 2},
    {R"({"type":"map", "values": "float"})", "{}",
     R"({"type":"map", "values": "double"})", "{}", 2},

    {R"({"type":"map", "values": "int"})", "{c1sK5I}",
     R"({"type":"map", "values": "long"})", "{c1sK5L}", 2},
    {R"({"type":"map", "values": "int"})", "{c1sK5I}",
     R"({"type":"map", "values": "double"})", "{c1sK5D}", 2},
    {R"({"type":"map", "values": "long"})", "{c1sK5L}",
     R"({"type":"map", "values": "double"})", "{c1sK5D}", 2},
    {R"({"type":"map", "values": "float"})", "{c1sK5F}",
     R"({"type":"map", "values": "double"})", "{c1sK5D}", 2},

    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"int\"}]}",
     "I",
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"long\"}]}",
     "L", 1},
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"int\"}]}",
     "I",
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"double\"}]}",
     "D", 1},

    // multi-field record with promotions
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f0\", \"type\":\"boolean\"},"
     "{\"name\":\"f1\", \"type\":\"int\"},"
     "{\"name\":\"f2\", \"type\":\"float\"},"
     "{\"name\":\"f3\", \"type\":\"string\"}]}",
     "BIFS",
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f0\", \"type\":\"boolean\"},"
     "{\"name\":\"f1\", \"type\":\"long\"},"
     "{\"name\":\"f2\", \"type\":\"double\"},"
     "{\"name\":\"f3\", \"type\":\"string\"}]}",
     "BLDS", 1},

    {R"(["int", "long"])", "U0I", R"(["long", "string"])", "U0L", 1},
    {R"(["int", "long"])", "U0I", R"(["double", "string"])", "U0D", 1},
    {R"(["long", "double"])", "U0L", R"(["double", "string"])", "U0D", 1},
    {R"(["float", "double"])", "U0F", R"(["double", "string"])", "U0D", 1},

    {"\"int\"", "I", R"(["int", "string"])", "U0I", 1},

    {R"(["int", "double"])", "U0I", "\"int\"", "I", 1},
    {R"(["int", "double"])", "U0I", "\"long\"", "L", 1},

    {R"(["boolean", "int"])", "U1I", R"(["boolean", "long"])", "U1L", 1},
    {R"(["boolean", "int"])", "U1I", R"(["long", "boolean"])", "U0L", 1},
};

static const TestData4 data4[] = {
    // Projection
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"string\"},"
     "{\"name\":\"f2\", \"type\":\"string\"},"
     "{\"name\":\"f3\", \"type\":\"int\"}]}",
     "S10S10IS10S10I",
     {"s1", "s2", "100", "t1", "t2", "200", nullptr},
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"string\" },"
     "{\"name\":\"f2\", \"type\":\"string\"}]}",
     "RS10S10RS10S10",
     {"s1", "s2", "t1", "t2", nullptr},
     1,
     2},

    // Reordered fields
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"int\"},"
     "{\"name\":\"f2\", \"type\":\"string\"}]}",
     "IS10",
     {"10", "hello", nullptr},
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f2\", \"type\":\"string\" },"
     "{\"name\":\"f1\", \"type\":\"long\"}]}",
     "RLS10",
     {"10", "hello", nullptr},
     1,
     1},

    // Default values
    {R"({"type":"record","name":"r","fields":[]})", "", {nullptr}, "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                                                                   "{\"name\":\"f\", \"type\":\"int\", \"default\": 100}]}",
     "RI",
     {"100", nullptr},
     1,
     1},

    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f2\", \"type\":\"int\"}]}",
     "I",
     {"10", nullptr},
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"int\", \"default\": 101},"
     "{\"name\":\"f2\", \"type\":\"int\"}]}",
     "RII",
     {"10", "101", nullptr},
     1,
     1},

    {"{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
     "{\"name\": \"g1\", "
     "\"type\":{\"type\":\"record\",\"name\":\"inner\",\"fields\":["
     "{\"name\":\"f2\", \"type\":\"int\"}]}}, "
     "{\"name\": \"g2\", \"type\": \"long\"}]}",
     "IL",
     {"10", "11", nullptr},
     "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
     "{\"name\": \"g1\", "
     "\"type\":{\"type\":\"record\",\"name\":\"inner\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"int\", \"default\": 101},"
     "{\"name\":\"f2\", \"type\":\"int\"}]}}, "
     "{\"name\": \"g2\", \"type\": \"long\"}]}}",
     "RRIIL",
     {"10", "101", "11", nullptr},
     1,
     1},

    // Default value for a record.
    {"{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
     "{\"name\": \"g1\", "
     "\"type\":{\"type\":\"record\",\"name\":\"inner1\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\" },"
     "{\"name\":\"f2\", \"type\":\"int\"}] } }, "
     "{\"name\": \"g2\", \"type\": \"long\"}]}",
     "LIL",
     {"10", "12", "13", nullptr},
     "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
     "{\"name\": \"g1\", "
     "\"type\":{\"type\":\"record\",\"name\":\"inner1\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\" },"
     "{\"name\":\"f2\", \"type\":\"int\"}] } }, "
     "{\"name\": \"g2\", \"type\": \"long\"},"
     "{\"name\": \"g3\", "
     "\"type\":{\"type\":\"record\",\"name\":\"inner2\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\" },"
     "{\"name\":\"f2\", \"type\":\"int\"}] }, "
     "\"default\": { \"f1\": 15, \"f2\": 101 } }] } ",
     "RRLILRLI",
     {"10", "12", "13", "15", "101", nullptr},
     1,
     1},

    {"{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
     "{\"name\": \"g1\", "
     "\"type\":{\"type\":\"record\",\"name\":\"inner1\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\" },"
     "{\"name\":\"f2\", \"type\":\"int\"}] } }, "
     "{\"name\": \"g2\", \"type\": \"long\"}]}",
     "LIL",
     {"10", "12", "13", nullptr},
     "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
     "{\"name\": \"g1\", "
     "\"type\":{\"type\":\"record\",\"name\":\"inner1\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"long\" },"
     "{\"name\":\"f2\", \"type\":\"int\"}] } }, "
     "{\"name\": \"g2\", \"type\": \"long\"},"
     "{\"name\": \"g3\", "
     "\"type\":\"inner1\", "
     "\"default\": { \"f1\": 15, \"f2\": 101 } }] } ",
     "RRLILRLI",
     {"10", "12", "13", "15", "101", nullptr},
     1,
     1},

    {R"({"type":"record","name":"r","fields":[]})", "", {nullptr}, "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                                                                   "{\"name\":\"f\", \"type\":{ \"type\": \"array\", \"items\": \"int\" },"
                                                                   "\"default\": [100]}]}",
     "[c1sI]",
     {"100", nullptr},
     1,
     1},

    {"{ \"type\": \"array\", \"items\": {\"type\":\"record\","
     "\"name\":\"r\",\"fields\":["
     "{\"name\":\"f0\", \"type\": \"int\"}]} }",
     "[c1sI]",
     {"99", nullptr},
     "{ \"type\": \"array\", \"items\": {\"type\":\"record\","
     "\"name\":\"r\",\"fields\":["
     "{\"name\":\"f\", \"type\":\"int\", \"default\": 100}]} }",
     "[Rc1sI]",
     {"100", nullptr},
     1,
     1},

    // Record of array of record with deleted field as last field
    {"{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
     "{\"name\": \"g1\","
     "\"type\":{\"type\":\"array\",\"items\":{"
     "\"name\":\"item\",\"type\":\"record\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"int\"},"
     "{\"name\":\"f2\", \"type\": \"long\", \"default\": 0}]}}}]}",
     "[c1sIL]",
     {"10", "11", nullptr},
     "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
     "{\"name\": \"g1\","
     "\"type\":{\"type\":\"array\",\"items\":{"
     "\"name\":\"item\",\"type\":\"record\",\"fields\":["
     "{\"name\":\"f1\", \"type\":\"int\"}]}}}]}",
     "R[c1sI]",
     {"10", nullptr},
     2,
     1},

    // Enum resolution
    {R"({"type":"enum","name":"e","symbols":["x","y","z"]})",
     "e2",
     {nullptr},
     R"({"type":"enum","name":"e","symbols":[ "y", "z" ]})",
     "e1",
     {nullptr},
     1,
     1},

    {R"({"type":"enum","name":"e","symbols":[ "x", "y" ]})",
     "e1",
     {nullptr},
     R"({"type":"enum","name":"e","symbols":[ "y", "z" ]})",
     "e0",
     {nullptr},
     1,
     1},

    // Union
    {"\"int\"", "I", {"100", nullptr}, R"([ "long", "int"])", "U1I", {"100", nullptr}, 1, 1},

    {R"([ "long", "int"])", "U1I", {"100", nullptr}, "\"int\"", "I", {"100", nullptr}, 1, 1},

    // Arrray of unions
    {R"({"type":"array", "items":[ "long", "int"]})",
     "[c2sU1IsU1I]",
     {"100", "100", nullptr},
     R"({"type":"array", "items": "int"})",
     "[c2sIsI]",
     {"100", "100", nullptr},
     2,
     1},

    // Map of unions
    {R"({"type":"map", "values":[ "long", "int"]})",
     "{c2sS10U1IsS10U1I}",
     {"k1", "100", "k2", "100", nullptr},
     R"({"type":"map", "values": "int"})",
     "{c2sS10IsS10I}",
     {"k1", "100", "k2", "100", nullptr},
     2,
     1},

    // Union + promotion
    {"\"int\"", "I", {"100", nullptr}, R"([ "long", "string"])", "U0L", {"100", nullptr}, 1, 1},

    {R"([ "int", "string"])", "U0I", {"100", nullptr}, "\"long\"", "L", {"100", nullptr}, 1, 1},

    // Record where union field is skipped.
    {"{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f0\", \"type\":\"boolean\"},"
     "{\"name\":\"f1\", \"type\":\"int\"},"
     "{\"name\":\"f2\", \"type\":[\"int\", \"long\"]},"
     "{\"name\":\"f3\", \"type\":\"float\"}"
     "]}",
     "BIU0IF",
     {"1", "100", "121", "10.75", nullptr},
     "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
     "{\"name\":\"f0\", \"type\":\"boolean\"},"
     "{\"name\":\"f1\", \"type\":\"long\"},"
     "{\"name\":\"f3\", \"type\":\"double\"}]}",
     "BLD",
     {"1", "100", "10.75", nullptr},
     1,
     1},
};

static const TestData4 data4BinaryOnly[] = {
    // Arrray of unions
    {R"({"type":"array", "items":[ "long", "int"]})",
     "[c1sU1Ic1sU1I]",
     {"100", "100", nullptr},
     R"({"type":"array", "items": "int"})",
     "[c1sIc1sI]",
     {"100", "100", nullptr},
     2},

    // Map of unions
    {R"({"type":"map", "values":[ "long", "int"]})",
     "{c1sS10U1Ic1sS10U1I}",
     {"k1", "100", "k2", "100", nullptr},
     R"({"type":"map", "values": "int"})",
     "{c1sS10Ic1sS10I}",
     {"k1", "100", "k2", "100", nullptr},
     2},
};

#define COUNTOF(x) sizeof(x) / sizeof(x[0])
#define ENDOF(x) (x) + COUNTOF(x)

// Boost 1.67 and later expects test cases to have unique names. This dummy
// helper functions leads to names which compose 'testFunc', 'Factory', and
// 'data'.
template<typename Test, typename Data>
Test testWithData(const Test &test, const Data &) {
    return test;
}
#define ADD_TESTS(testSuite, Factory, testFunc, data) \
    testSuite.add(BOOST_PARAM_TEST_CASE(              \
        testWithData(&testFunc<Factory>, data), data, data + COUNTOF(data)))

struct BinaryEncoderFactory {
    static EncoderPtr newEncoder(const ValidSchema &schema) {
        return binaryEncoder();
    }
};

struct BinaryDecoderFactory {
    static DecoderPtr newDecoder(const ValidSchema &schema) {
        return binaryDecoder();
    }
};

struct BinaryCodecFactory : public BinaryEncoderFactory,
                            public BinaryDecoderFactory {
};

struct ValidatingEncoderFactory {
    static EncoderPtr newEncoder(const ValidSchema &schema) {
        return validatingEncoder(schema, binaryEncoder());
    }
};

struct ValidatingDecoderFactory {
    static DecoderPtr newDecoder(const ValidSchema &schema) {
        return validatingDecoder(schema, binaryDecoder());
    }
};

struct ValidatingCodecFactory : public ValidatingEncoderFactory,
                                public ValidatingDecoderFactory {
};

struct JsonCodec {
    static EncoderPtr newEncoder(const ValidSchema &schema) {
        return jsonEncoder(schema);
    }
    static DecoderPtr newDecoder(const ValidSchema &schema) {
        return jsonDecoder(schema);
    }
};

struct JsonPrettyCodec {
    static EncoderPtr newEncoder(const ValidSchema &schema) {
        return jsonPrettyEncoder(schema);
    }
    static DecoderPtr newDecoder(const ValidSchema &schema) {
        return jsonDecoder(schema);
    }
};

struct BinaryEncoderResolvingDecoderFactory : public BinaryEncoderFactory {
    static DecoderPtr newDecoder(const ValidSchema &schema) {
        return resolvingDecoder(schema, schema, binaryDecoder());
    }

    static DecoderPtr newDecoder(const ValidSchema &writer,
                                 const ValidSchema &reader) {
        return resolvingDecoder(writer, reader, binaryDecoder());
    }
};

struct JsonEncoderResolvingDecoderFactory {
    static EncoderPtr newEncoder(const ValidSchema &schema) {
        return jsonEncoder(schema);
    }

    static DecoderPtr newDecoder(const ValidSchema &schema) {
        return resolvingDecoder(schema, schema, jsonDecoder(schema));
    }

    static DecoderPtr newDecoder(const ValidSchema &writer,
                                 const ValidSchema &reader) {
        return resolvingDecoder(writer, reader, jsonDecoder(writer));
    }
};

struct ValidatingEncoderResolvingDecoderFactory : public ValidatingEncoderFactory {
    static DecoderPtr newDecoder(const ValidSchema &schema) {
        return resolvingDecoder(schema, schema,
                                validatingDecoder(schema, binaryDecoder()));
    }

    static DecoderPtr newDecoder(const ValidSchema &writer,
                                 const ValidSchema &reader) {
        return resolvingDecoder(writer, reader,
                                validatingDecoder(writer, binaryDecoder()));
    }
};

void add_tests(boost::unit_test::test_suite &ts) {
    ADD_TESTS(ts, BinaryCodecFactory, testCodec, data);
    ADD_TESTS(ts, ValidatingCodecFactory, testCodec, data);
    ADD_TESTS(ts, JsonCodec, testCodec, data);
    ADD_TESTS(ts, JsonPrettyCodec, testCodec, data);
    ADD_TESTS(ts, BinaryEncoderResolvingDecoderFactory, testCodec, data);
    ADD_TESTS(ts, JsonEncoderResolvingDecoderFactory, testCodec, data);
    ADD_TESTS(ts, ValidatingCodecFactory, testReaderFail, data2);
    ADD_TESTS(ts, ValidatingCodecFactory, testWriterFail, data2);
    ADD_TESTS(ts, BinaryEncoderResolvingDecoderFactory,
              testCodecResolving, data3);
    ADD_TESTS(ts, JsonEncoderResolvingDecoderFactory,
              testCodecResolving, data3);
    ADD_TESTS(ts, BinaryEncoderResolvingDecoderFactory,
              testCodecResolving2, data4);
    ADD_TESTS(ts, JsonEncoderResolvingDecoderFactory,
              testCodecResolving2, data4);
    ADD_TESTS(ts, ValidatingEncoderResolvingDecoderFactory,
              testCodecResolving2, data4);
    ADD_TESTS(ts, BinaryEncoderResolvingDecoderFactory,
              testCodecResolving2, data4BinaryOnly);

    ADD_TESTS(ts, ValidatingCodecFactory, testGeneric, data);
    ADD_TESTS(ts, ValidatingCodecFactory, testGenericResolving, data3);
    ADD_TESTS(ts, ValidatingCodecFactory, testGenericResolving2, data4);
}

} // namespace parsing

static void testStreamLifetimes() {
    EncoderPtr e = binaryEncoder();
    {
        std::unique_ptr<OutputStream> s1 = memoryOutputStream();
        e->init(*s1);
        e->encodeInt(100);
        e->encodeDouble(4.73);
        e->flush();
    }

    {
        std::unique_ptr<OutputStream> s2 = memoryOutputStream();
        e->init(*s2);
        e->encodeDouble(3.14);
        e->flush();
    }
}

static void testLimits(const EncoderPtr &e, const DecoderPtr &d) {
    std::unique_ptr<OutputStream> s1 = memoryOutputStream();
    {
        e->init(*s1);
        e->encodeDouble(std::numeric_limits<double>::infinity());
        e->encodeDouble(-std::numeric_limits<double>::infinity());
        e->encodeDouble(std::numeric_limits<double>::quiet_NaN());
        e->encodeDouble(std::numeric_limits<double>::max());
        e->encodeDouble(std::numeric_limits<double>::min());
        e->encodeFloat(std::numeric_limits<float>::infinity());
        e->encodeFloat(-std::numeric_limits<float>::infinity());
        e->encodeFloat(std::numeric_limits<float>::quiet_NaN());
        e->encodeFloat(std::numeric_limits<float>::max());
        e->encodeFloat(std::numeric_limits<float>::min());
        e->flush();
    }

    {
        std::unique_ptr<InputStream> s2 = memoryInputStream(*s1);
        d->init(*s2);
        BOOST_CHECK_EQUAL(d->decodeDouble(),
                          std::numeric_limits<double>::infinity());
        BOOST_CHECK_EQUAL(d->decodeDouble(),
                          -std::numeric_limits<double>::infinity());
        BOOST_CHECK(boost::math::isnan(d->decodeDouble()));
        BOOST_CHECK(d->decodeDouble() == std::numeric_limits<double>::max());
        BOOST_CHECK(d->decodeDouble() == std::numeric_limits<double>::min());
        BOOST_CHECK_EQUAL(d->decodeFloat(),
                          std::numeric_limits<float>::infinity());
        BOOST_CHECK_EQUAL(d->decodeFloat(),
                          -std::numeric_limits<float>::infinity());
        BOOST_CHECK(boost::math::isnan(d->decodeFloat()));
        BOOST_CHECK_CLOSE(d->decodeFloat(), std::numeric_limits<float>::max(), 0.00011);
        BOOST_CHECK_CLOSE(d->decodeFloat(), std::numeric_limits<float>::min(), 0.00011);
    }
}

static void testLimitsBinaryCodec() {
    testLimits(binaryEncoder(), binaryDecoder());
}

static void testLimitsJsonCodec() {
    const char *s = "{ \"type\": \"record\", \"name\": \"r\", \"fields\": ["
                    "{ \"name\": \"d1\", \"type\": \"double\" },"
                    "{ \"name\": \"d2\", \"type\": \"double\" },"
                    "{ \"name\": \"d3\", \"type\": \"double\" },"
                    "{ \"name\": \"d4\", \"type\": \"double\" },"
                    "{ \"name\": \"d5\", \"type\": \"double\" },"
                    "{ \"name\": \"f1\", \"type\": \"float\" },"
                    "{ \"name\": \"f2\", \"type\": \"float\" },"
                    "{ \"name\": \"f3\", \"type\": \"float\" },"
                    "{ \"name\": \"f4\", \"type\": \"float\" },"
                    "{ \"name\": \"f5\", \"type\": \"float\" }"
                    "]}";
    ValidSchema schema = parsing::makeValidSchema(s);
    testLimits(jsonEncoder(schema), jsonDecoder(schema));
    testLimits(jsonPrettyEncoder(schema), jsonDecoder(schema));
}

struct JsonData {
    const char *schema;
    const char *json;
    const char *calls;
    int depth;
};

const JsonData jsonData[] = {
    {R"({"type": "double"})", " 10 ", "D", 1},
    {R"({"type": "double"})", " 10.0 ", "D", 1},
    {R"({"type": "double"})", " \"Infinity\"", "D", 1},
    {R"({"type": "double"})", " \"-Infinity\"", "D", 1},
    {R"({"type": "double"})", " \"NaN\"", "D", 1},
    {R"({"type": "long"})", " 10 ", "L", 1},
};

static void testJson(const JsonData &data) {
    ValidSchema schema = parsing::makeValidSchema(data.schema);
    EncoderPtr e = jsonEncoder(schema);
}

static void testJsonCodecReinit() {
    const char *schemaStr = "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
                            "{\"name\":\"f1\", \"type\":\"boolean\"},"
                            "{\"name\":\"f2\", \"type\":\"long\"}"
                            "]}";
    ValidSchema schema = parsing::makeValidSchema(schemaStr);
    OutputStreamPtr os1 = memoryOutputStream();
    OutputStreamPtr os2 = memoryOutputStream();
    {
        EncoderPtr e = jsonEncoder(schema);
        e->init(*os1);
        e->encodeBool(false);
        e->encodeLong(100);
        e->flush();

        e->init(*os2);
        e->encodeBool(true);
        e->encodeLong(200);
        e->flush();
    }

    InputStreamPtr is1 = memoryInputStream(*os1);
    InputStreamPtr is2 = memoryInputStream(*os2);
    DecoderPtr d = jsonDecoder(schema);
    {
        d->init(*is1);
        BOOST_CHECK_EQUAL(d->decodeBool(), false);
        BOOST_CHECK_EQUAL(d->decodeLong(), 100);
    }

    // Reinit
    {
        d->init(*is2);
        BOOST_CHECK_EQUAL(d->decodeBool(), true);
        BOOST_CHECK_EQUAL(d->decodeLong(), 200);
    }
}

static void testByteCount() {
    OutputStreamPtr os1 = memoryOutputStream();
    EncoderPtr e1 = binaryEncoder();
    e1->init(*os1);
    e1->encodeBool(true);
    e1->encodeLong(1000);
    e1->flush();
    BOOST_CHECK_EQUAL(e1->byteCount(), 3);
    BOOST_CHECK_EQUAL(os1->byteCount(), 3);
}

} // namespace avro

boost::unit_test::test_suite *
init_unit_test_suite(int argc, char *argv[]) {
    using namespace boost::unit_test;

    auto *ts = BOOST_TEST_SUITE("Avro C++ unit tests for codecs");
    avro::parsing::add_tests(*ts);
    ts->add(BOOST_TEST_CASE(avro::testStreamLifetimes));
    ts->add(BOOST_TEST_CASE(avro::testLimitsBinaryCodec));
    ts->add(BOOST_TEST_CASE(avro::testLimitsJsonCodec));
    ts->add(BOOST_PARAM_TEST_CASE(&avro::testJson, avro::jsonData,
                                  ENDOF(avro::jsonData)));
    ts->add(BOOST_TEST_CASE(avro::testJsonCodecReinit));
    ts->add(BOOST_TEST_CASE(avro::testByteCount));

    return ts;
}

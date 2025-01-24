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

#include <boost/filesystem.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <thread>

#include <sstream>

#include "Compiler.hh"
#include "DataFile.hh"
#include "Generic.hh"
#include "Stream.hh"

using std::array;
using std::istringstream;
using std::map;
using std::ostringstream;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

using boost::shared_ptr;
using boost::unit_test::test_suite;

using avro::GenericDatum;
using avro::GenericRecord;
using avro::NodePtr;
using avro::ValidSchema;

const int DEFAULT_COUNT = 1000;

template<typename T>
struct Complex {
    T re;
    T im;
    Complex() : re(0), im(0) {}
    Complex(T r, T i) : re(r), im(i) {}
};

struct Integer {
    int64_t re;
    Integer() : re(0) {}
    explicit Integer(int64_t r) : re(r) {}
};

using ComplexInteger = Complex<int64_t>;
using ComplexDouble = Complex<double>;

struct Double {
    double re;
    Double() : re(0) {}
    explicit Double(double r) : re(r) {}
};

namespace avro {

template<typename T>
struct codec_traits<Complex<T>> {
    static void encode(Encoder &e, const Complex<T> &c) {
        avro::encode(e, c.re);
        avro::encode(e, c.im);
    }

    static void decode(Decoder &d, Complex<T> &c) {
        avro::decode(d, c.re);
        avro::decode(d, c.im);
    }
};

template<>
struct codec_traits<Integer> {
    static void decode(Decoder &d, Integer &c) {
        avro::decode(d, c.re);
    }
};

template<>
struct codec_traits<Double> {
    static void decode(Decoder &d, Double &c) {
        avro::decode(d, c.re);
    }
};

template<>
struct codec_traits<uint32_t> {
    static void encode(Encoder &e, const uint32_t &v) {
        e.encodeFixed((uint8_t *) &v, sizeof(uint32_t));
    }

    static void decode(Decoder &d, uint32_t &v) {
        std::vector<uint8_t> value;
        d.decodeFixed(sizeof(uint32_t), value);
        memcpy(&v, &(value[0]), sizeof(uint32_t));
    }
};

} // namespace avro

static ValidSchema makeValidSchema(const char *schema) {
    istringstream iss(schema);
    ValidSchema vs;
    compileJsonSchema(iss, vs);
    return ValidSchema(vs);
}

static const char sch[] = "{\"type\": \"record\","
                          "\"name\":\"ComplexInteger\", \"fields\": ["
                          "{\"name\":\"re\", \"type\":\"long\"},"
                          "{\"name\":\"im\", \"type\":\"long\"}"
                          "]}";
static const char isch[] = "{\"type\": \"record\","
                           "\"name\":\"ComplexInteger\", \"fields\": ["
                           "{\"name\":\"re\", \"type\":\"long\"}"
                           "]}";
static const char dsch[] = "{\"type\": \"record\","
                           "\"name\":\"ComplexDouble\", \"fields\": ["
                           "{\"name\":\"re\", \"type\":\"double\"},"
                           "{\"name\":\"im\", \"type\":\"double\"}"
                           "]}";
static const char dblsch[] =
    "{\"type\": \"record\","
    "\"name\":\"ComplexDouble\", "
    "\"doc\": \"\\\"Quoted_doc_string\\\"\", "
    "\"fields\": ["
    "{\"name\":\"re\", \"type\":\"double\"}"
    "]}";
static const char fsch[] = "{\"type\": \"fixed\","
                           "\"name\":\"Fixed_32\", \"size\":4}";
static const char ischWithDoc[] =
    "{\"type\": \"record\","
    "\"name\":\"ComplexInteger\", "
    "\"doc\": \"record_doc\", "
    "\"fields\": ["
    "{\"name\":\"re1\", \"type\":\"long\", \"doc\": \"field_doc\"},"
    "{\"name\":\"re2\", \"type\":\"long\"},"
    "{\"name\":\"re3\", \"type\":\"long\", \"doc\": \"\"},"
    "{\"name\":\"re4\", \"type\":\"long\", "
    "\"doc\": \"A_\\\"quoted_doc\\\"\"},"
    "{\"name\":\"re5\", \"type\":\"long\", \"doc\": \"doc with\nspaces\"},"
    "{\"name\":\"re6\", \"type\":\"long\", "
    "\"doc\": \"extra slashes\\\\\\\\\"}"
    "]}";

static const char schemaWithIdAndString[] = R"({
       "type":"record",
       "name":"R",
       "fields":[
          {
             "name":"s1",
             "type":"string"
          },
          {
             "name":"id",
             "type":"long"
          }
       ]
    })";

string toString(const ValidSchema &s) {
    ostringstream oss;
    s.toJson(oss);
    return oss.str();
}

class DataFileTest {
    const char *filename;
    const ValidSchema writerSchema;
    const ValidSchema readerSchema;
    const int count;

public:
    DataFileTest(const char *f, const char *wsch, const char *rsch,
                 int count = DEFAULT_COUNT) : filename(f), writerSchema(makeValidSchema(wsch)),
                                              readerSchema(makeValidSchema(rsch)), count(count) {}

    using Pair = pair<ValidSchema, GenericDatum>;

    void testCleanup() {
        BOOST_CHECK(boost::filesystem::remove(filename));
    }

    void testWrite() {
        testWriteWithCodec(avro::NULL_CODEC);
    }

    void testWriteWithDeflateCodec() {
        testWriteWithCodec(avro::DEFLATE_CODEC);
    }

#ifdef SNAPPY_CODEC_AVAILABLE
    void testWriteWithSnappyCodec() {
        testWriteWithCodec(avro::SNAPPY_CODEC);
    }
#endif

    void testWriteWithCodec(avro::Codec codec) {
        avro::DataFileWriter<ComplexInteger> df(filename, writerSchema, 100);
        int64_t re = 3;
        int64_t im = 5;
        for (int i = 0; i < count; ++i, re *= im, im += 3) {
            ComplexInteger c(re, im);
            df.write(c);
        }
        // Simulate writing an empty block.
        df.flush();
        df.close();
    }

    void testWriteGeneric() {
        avro::DataFileWriter<Pair> df(filename, writerSchema, 100);
        int64_t re = 3;
        int64_t im = 5;
        Pair p(writerSchema, GenericDatum());

        GenericDatum &c = p.second;
        c = GenericDatum(writerSchema.root());
        auto &r = c.value<GenericRecord>();

        for (int i = 0; i < count; ++i, re *= im, im += 3) {
            r.fieldAt(0) = re;
            r.fieldAt(1) = im;
            df.write(p);
        }
        df.close();
    }

    void testWriteGenericByName() {
        avro::DataFileWriter<Pair> df(filename, writerSchema, 100);
        int64_t re = 3;
        int64_t im = 5;
        Pair p(writerSchema, GenericDatum());

        GenericDatum &c = p.second;
        c = GenericDatum(writerSchema.root());
        auto &r = c.value<GenericRecord>();

        for (int i = 0; i < count; ++i, re *= im, im += 3) {
            r.field("re") = re;
            r.field("im") = im;
            df.write(p);
        }
        df.close();
    }

    void testWriteDouble() {
        avro::DataFileWriter<ComplexDouble> df(filename, writerSchema, 100);
        double re = 3.0;
        double im = 5.0;
        for (int i = 0; i < count; ++i, re += im - 0.7, im += 3.1) {
            ComplexDouble c(re, im);
            df.write(c);
        }
        df.close();
    }

    void testTruncate() {
        testWriteDouble();
        uintmax_t size = boost::filesystem::file_size(filename);
        {
            avro::DataFileWriter<Pair> df(filename, writerSchema, 100);
            df.close();
        }
        uintmax_t new_size = boost::filesystem::file_size(filename);
        BOOST_CHECK(size > new_size);
    }

    void testReadFull() {
        avro::DataFileReader<ComplexInteger> df(filename, writerSchema);
        int i = 0;
        ComplexInteger ci;
        int64_t re = 3;
        int64_t im = 5;
        while (df.read(ci)) {
            BOOST_CHECK_EQUAL(ci.re, re);
            BOOST_CHECK_EQUAL(ci.im, im);
            re *= im;
            im += 3;
            ++i;
        }
        BOOST_CHECK_EQUAL(i, count);
    }

    void testReadProjection() {
        avro::DataFileReader<Integer> df(filename, readerSchema);
        int i = 0;
        Integer integer;
        int64_t re = 3;
        int64_t im = 5;
        while (df.read(integer)) {
            BOOST_CHECK_EQUAL(integer.re, re);
            re *= im;
            im += 3;
            ++i;
        }
        BOOST_CHECK_EQUAL(i, count);
    }

    void testReaderGeneric() {
        avro::DataFileReader<Pair> df(filename, writerSchema);
        int i = 0;
        Pair p(writerSchema, GenericDatum());
        int64_t re = 3;
        int64_t im = 5;

        const GenericDatum &ci = p.second;
        while (df.read(p)) {
            BOOST_REQUIRE_EQUAL(ci.type(), avro::AVRO_RECORD);
            const auto &r = ci.value<GenericRecord>();
            const size_t n = 2;
            BOOST_REQUIRE_EQUAL(r.fieldCount(), n);
            const GenericDatum &f0 = r.fieldAt(0);
            BOOST_REQUIRE_EQUAL(f0.type(), avro::AVRO_LONG);
            BOOST_CHECK_EQUAL(f0.value<int64_t>(), re);

            const GenericDatum &f1 = r.fieldAt(1);
            BOOST_REQUIRE_EQUAL(f1.type(), avro::AVRO_LONG);
            BOOST_CHECK_EQUAL(f1.value<int64_t>(), im);
            re *= im;
            im += 3;
            ++i;
        }
        BOOST_CHECK_EQUAL(i, count);
    }

    void testReaderGenericByName() {
        avro::DataFileReader<Pair> df(filename, writerSchema);
        int i = 0;
        Pair p(writerSchema, GenericDatum());
        int64_t re = 3;
        int64_t im = 5;

        const GenericDatum &ci = p.second;
        while (df.read(p)) {
            BOOST_REQUIRE_EQUAL(ci.type(), avro::AVRO_RECORD);
            const auto &r = ci.value<GenericRecord>();
            const size_t n = 2;
            BOOST_REQUIRE_EQUAL(r.fieldCount(), n);
            const GenericDatum &f0 = r.field("re");
            BOOST_REQUIRE_EQUAL(f0.type(), avro::AVRO_LONG);
            BOOST_CHECK_EQUAL(f0.value<int64_t>(), re);

            const GenericDatum &f1 = r.field("im");
            BOOST_REQUIRE_EQUAL(f1.type(), avro::AVRO_LONG);
            BOOST_CHECK_EQUAL(f1.value<int64_t>(), im);
            re *= im;
            im += 3;
            ++i;
        }
        BOOST_CHECK_EQUAL(i, count);
    }

    void testReaderGenericProjection() {
        avro::DataFileReader<Pair> df(filename, readerSchema);
        int i = 0;
        Pair p(readerSchema, GenericDatum());
        int64_t re = 3;
        int64_t im = 5;

        const GenericDatum &ci = p.second;
        while (df.read(p)) {
            BOOST_REQUIRE_EQUAL(ci.type(), avro::AVRO_RECORD);
            const auto &r = ci.value<GenericRecord>();
            const size_t n = 1;
            BOOST_REQUIRE_EQUAL(r.fieldCount(), n);
            const GenericDatum &f0 = r.fieldAt(0);
            BOOST_REQUIRE_EQUAL(f0.type(), avro::AVRO_LONG);
            BOOST_CHECK_EQUAL(f0.value<int64_t>(), re);

            re *= im;
            im += 3;
            ++i;
        }
        BOOST_CHECK_EQUAL(i, count);
    }

    void testReaderSyncSeek() {
        std::vector<int64_t> sync_points;
        avro::DataFileReader<ComplexInteger> df(filename, writerSchema);
        for (int64_t prev = 0; prev != df.previousSync(); df.sync(prev)) {
            prev = df.previousSync();
            sync_points.push_back(prev);
        }
        std::set<pair<int64_t, int64_t>> actual;
        int num = 0;
        for (int i = sync_points.size() - 2; i >= 0; --i) {
            df.seek(sync_points[i]);
            ComplexInteger ci;
            // Subtract avro::SyncSize here because sync and pastSync
            // expect a point *at or before* the sync marker, whereas seek
            // expects the point right *after* the sync marker.
            while (!df.pastSync(sync_points[i + 1] - avro::SyncSize)) {
                BOOST_CHECK(df.read(ci));
                ++num;
                actual.insert(std::make_pair(ci.re, ci.im));
            }
        }
        df.close();
        // We read 'count' total objects.
        BOOST_CHECK_EQUAL(num, count);
        // We read 'count' distinct objects.
        BOOST_CHECK_EQUAL(actual.size(), count);
        // They were the same objects initially written.
        int64_t re = 3;
        int64_t im = 5;
        for (int i = 0; i < count; ++i, re *= im, im += 3) {
            actual.insert(std::make_pair(re, im));
        }
        BOOST_CHECK_EQUAL(actual.size(), count);
    }

    void testReaderSyncDiscovery() {
        std::set<int64_t> sync_points_syncing;
        std::set<int64_t> sync_points_reading;
        {
            /*
             * sync() will stop at a block with 0 objects. But read()
             * will transparently skip such blocks. So this test will
             * fail if there are blocks with zero objects. In order to
             * avoid such failures, we read one object after sync.
             */
            avro::DataFileReader<ComplexInteger> df(filename, writerSchema);
            ComplexInteger ci;
            for (int64_t prev = 0; prev != df.previousSync(); df.sync(prev)) {
                df.read(ci);
                prev = df.previousSync();
                sync_points_syncing.insert(prev);
            }
            df.close();
        }
        {
            avro::DataFileReader<ComplexInteger> df(filename, writerSchema);
            sync_points_reading.insert(df.previousSync());
            ComplexInteger ci;
            while (df.read(ci)) {
                sync_points_reading.insert(df.previousSync());
            }
            sync_points_reading.insert(df.previousSync());
            df.close();
        }
        BOOST_CHECK(sync_points_syncing == sync_points_reading);
        // Just to make sure we're actually finding a reasonable number of
        // splits.. rather than bugs like only find the first split.
        BOOST_CHECK_GT(sync_points_syncing.size(), 10);
    }

    // This is a direct port of testSplits() from
    // lang/java/avro/src/test/java/org/apache/avro/TestDataFile.java.
    void testReaderSplits() {
        boost::mt19937 random(static_cast<uint32_t>(time(nullptr)));
        avro::DataFileReader<ComplexInteger> df(filename, writerSchema);
        std::ifstream just_for_length(
            filename, std::ifstream::ate | std::ifstream::binary);
        int length = just_for_length.tellg();
        int splits = 10;
        int end = length;     // end of split
        int remaining = end;  // bytes remaining
        int actual_count = 0; // count of entries
        while (remaining > 0) {
            int start =
                std::max(0, end - boost::random::uniform_int_distribution<>(0, 2 * length / splits)(random));
            df.sync(start); // count entries in split
            while (!df.pastSync(end)) {
                ComplexInteger ci;
                df.read(ci);
                actual_count++;
            }
            remaining -= end - start;
            end = start;
        }
        BOOST_CHECK_EQUAL(actual_count, count);
    }

    void testReadDouble() {
        avro::DataFileReader<ComplexDouble> df(filename, writerSchema);
        int i = 0;
        ComplexDouble ci;
        double re = 3.0;
        double im = 5.0;
        while (df.read(ci)) {
            BOOST_CHECK_CLOSE(ci.re, re, 0.0001);
            BOOST_CHECK_CLOSE(ci.im, im, 0.0001);
            re += (im - 0.7);
            im += 3.1;
            ++i;
        }
        BOOST_CHECK_EQUAL(i, count);
    }

    /**
     * Constructs the DataFileReader in two steps.
     */
    void testReadDoubleTwoStep() {
        unique_ptr<avro::DataFileReaderBase>
            base(new avro::DataFileReaderBase(filename));
        avro::DataFileReader<ComplexDouble> df(std::move(base));
        BOOST_CHECK_EQUAL(toString(writerSchema), toString(df.readerSchema()));
        BOOST_CHECK_EQUAL(toString(writerSchema), toString(df.dataSchema()));
        int i = 0;
        ComplexDouble ci;
        double re = 3.0;
        double im = 5.0;
        while (df.read(ci)) {
            BOOST_CHECK_CLOSE(ci.re, re, 0.0001);
            BOOST_CHECK_CLOSE(ci.im, im, 0.0001);
            re += (im - 0.7);
            im += 3.1;
            ++i;
        }
        BOOST_CHECK_EQUAL(i, count);
    }

    /**
     * Constructs the DataFileReader in two steps using a different
     * reader schema.
     */
    void testReadDoubleTwoStepProject() {
        unique_ptr<avro::DataFileReaderBase>
            base(new avro::DataFileReaderBase(filename));
        avro::DataFileReader<Double> df(std::move(base), readerSchema);

        BOOST_CHECK_EQUAL(toString(readerSchema), toString(df.readerSchema()));
        BOOST_CHECK_EQUAL(toString(writerSchema), toString(df.dataSchema()));
        int i = 0;
        Double ci;
        double re = 3.0;
        double im = 5.0;
        while (df.read(ci)) {
            BOOST_CHECK_CLOSE(ci.re, re, 0.0001);
            re += (im - 0.7);
            im += 3.1;
            ++i;
        }
        BOOST_CHECK_EQUAL(i, count);
    }
    /**
     * Test writing DataFiles into other streams operations.
     */
    void testZip() {
        const size_t number_of_objects = 100;
        // first create a large file
        ValidSchema dschema = avro::compileJsonSchemaFromString(sch);
        {
            avro::DataFileWriter<ComplexInteger> writer(
                filename, dschema, 16 * 1024, avro::DEFLATE_CODEC);

            for (size_t i = 0; i < number_of_objects; ++i) {
                ComplexInteger d;
                d.re = i;
                d.im = 2 * i;
                writer.write(d);
            }
        }
        {
            avro::DataFileReader<ComplexInteger> reader(filename, dschema);
            std::vector<int> found;
            ComplexInteger record;
            while (reader.read(record)) {
                found.push_back(record.re);
            }
            BOOST_CHECK_EQUAL(found.size(), number_of_objects);
            for (unsigned int i = 0; i < found.size(); ++i) {
                BOOST_CHECK_EQUAL(found[i], i);
            }
        }
    }

#ifdef SNAPPY_CODEC_AVAILABLE
    void testSnappy() {
        // Add enough objects to span multiple blocks
        const size_t number_of_objects = 1000000;
        // first create a large file
        ValidSchema dschema = avro::compileJsonSchemaFromString(sch);
        {
            avro::DataFileWriter<ComplexInteger> writer(
                filename, dschema, 16 * 1024, avro::SNAPPY_CODEC);

            for (size_t i = 0; i < number_of_objects; ++i) {
                ComplexInteger d;
                d.re = i;
                d.im = 2 * i;
                writer.write(d);
            }
        }
        {
            avro::DataFileReader<ComplexInteger> reader(filename, dschema);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            std::vector<int64_t> found;
            ComplexInteger record;
            while (reader.read(record)) {
                found.push_back(record.re);
            }
            BOOST_CHECK_EQUAL(found.size(), number_of_objects);
            for (unsigned int i = 0; i < found.size(); ++i) {
                BOOST_CHECK_EQUAL(found[i], i);
            }
        }
    }
#endif

    void testSchemaReadWrite() {
        uint32_t a = 42;
        {
            avro::DataFileWriter<uint32_t> df(filename, writerSchema);
            df.write(a);
        }

        {
            avro::DataFileReader<uint32_t> df(filename);
            uint32_t b;
            df.read(b);
            BOOST_CHECK_EQUAL(b, a);
        }
    }

    void testSchemaReadWriteWithDoc() {
        uint32_t a = 42;
        {
            avro::DataFileWriter<uint32_t> df(filename, writerSchema);
            df.write(a);
        }

        {
            avro::DataFileReader<uint32_t> df(filename);
            uint32_t b;
            df.read(b);
            BOOST_CHECK_EQUAL(b, a);

            const NodePtr &root = df.readerSchema().root();
            BOOST_CHECK_EQUAL(root->getDoc(), "record_doc");
            BOOST_CHECK_EQUAL(root->leafAt(0)->getDoc(), "field_doc");
            BOOST_CHECK_EQUAL(root->leafAt(1)->getDoc(), "");
            BOOST_CHECK_EQUAL(root->leafAt(2)->getDoc(), "");
            BOOST_CHECK_EQUAL(root->leafAt(3)->getDoc(), "A_\"quoted_doc\"");
            BOOST_CHECK_EQUAL(root->leafAt(4)->getDoc(), "doc with\nspaces");
            BOOST_CHECK_EQUAL(root->leafAt(5)->getDoc(), "extra slashes\\\\");
        }
    }
};

void addReaderTests(test_suite *ts, const shared_ptr<DataFileTest> &t) {
    ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testReadFull, t));
    ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testReadProjection, t));
    ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testReaderGeneric, t));
    ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testReaderGenericByName, t));
    ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testReaderGenericProjection,
                                  t));
    ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testCleanup, t));
}

struct WriterObj {
    std::string s1;
    std::string s2;
    WriterObj(const char *s1, const char *s2) : s1(s1), s2(s2) {}
};

struct ReaderObj {
    std::string s2;
    explicit ReaderObj(const char *s2) : s2(s2) {}
};

namespace avro {
template<>
struct codec_traits<WriterObj> {
    static void encode(Encoder &e, const WriterObj &v) {
        avro::encode(e, v.s1);
        avro::encode(e, v.s2);
    }
};

template<>
struct codec_traits<ReaderObj> {
    static void decode(Decoder &d, ReaderObj &v) {
        if (auto *rd =
                dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (unsigned long it : fo) {
                switch (it) {
                    case 0: {
                        avro::decode(d, v.s2);
                        break;
                    }
                    default: {
                        break;
                    }
                }
            }
        } else {
            avro::decode(d, v.s2);
        }
    }
};
} // namespace avro

void testSkipString(avro::Codec codec) {
    const char *writerSchemaStr = "{"
                                  "\"type\": \"record\", \"name\": \"R\", \"fields\":["
                                  "{\"name\": \"s1\", \"type\": \"string\"},"
                                  "{\"name\": \"s2\", \"type\": \"string\"}"
                                  "]}";
    const char *readerSchemaStr = "{"
                                  "\"type\": \"record\", \"name\": \"R\", \"fields\":["
                                  "{\"name\": \"s2\", \"type\": \"string\"}"
                                  "]}";
    avro::ValidSchema writerSchema =
        avro::compileJsonSchemaFromString(writerSchemaStr);
    avro::ValidSchema readerSchema =
        avro::compileJsonSchemaFromString(readerSchemaStr);
    const size_t stringLen = 10240;
    char largeString[stringLen + 1];

    for (size_t i = 0; i < stringLen; i++) {
        largeString[i] = 'a';
    }
    largeString[stringLen] = '\0';

    const char *filename = "test_skip.df";
    {
        avro::DataFileWriter<WriterObj> df(filename,
                                           writerSchema, 100, codec);
        df.write(WriterObj(largeString, "b1"));
        df.write(WriterObj(largeString, "b2"));
        df.flush();
        df.close();
    }
    {
        avro::DataFileReader<ReaderObj> df(filename, readerSchema);
        ReaderObj ro("");
        BOOST_CHECK_EQUAL(df.read(ro), true);
        BOOST_CHECK_EQUAL(ro.s2, "b1");
        BOOST_CHECK_EQUAL(df.read(ro), true);
        BOOST_CHECK_EQUAL(ro.s2, "b2");
        BOOST_CHECK_EQUAL(df.read(ro), false);
    }
}

void testSkipStringNullCodec() {
    BOOST_TEST_CHECKPOINT(__func__);
    testSkipString(avro::NULL_CODEC);
}

void testSkipStringDeflateCodec() {
    BOOST_TEST_CHECKPOINT(__func__);
    testSkipString(avro::DEFLATE_CODEC);
}

#ifdef SNAPPY_CODEC_AVAILABLE
void testSkipStringSnappyCodec() {
    BOOST_TEST_CHECKPOINT(__func__);
    testSkipString(avro::SNAPPY_CODEC);
}
#endif

struct TestRecord {
    std::string s1;
    int64_t id;
    TestRecord(const char *s1, int64_t id) : s1(s1), id(id) {}
};

namespace avro {
template<>
struct codec_traits<TestRecord> {
    static void encode(Encoder &e, const TestRecord &v) {
        avro::encode(e, v.s1);
        avro::encode(e, v.id);
    }

    static void decode(Decoder &d, TestRecord &v) {
        avro::decode(d, v.s1);
        avro::decode(d, v.id);
    }
};
} // namespace avro

void testLastSync(avro::Codec codec) {

    //
    // This test does validates equivalence of the lastSync API on the writer and the previousSync() returned by the reader for every record read.
    //

    avro::ValidSchema writerSchema =
        avro::compileJsonSchemaFromString(schemaWithIdAndString);

    const size_t stringLen = 100;
    char largeString[stringLen + 1];
    for (size_t i = 0; i < stringLen; i++) {
        largeString[i] = 'a';
    }

    largeString[stringLen] = '\0';

    const char *filename = "test_lastSync.df";
    std::deque<std::pair<uint64_t, int>> syncMetadata;
    int numberOfRecords = 100;
    {
        avro::DataFileWriter<TestRecord> df(filename,
                                            writerSchema, 1024, codec);

        uint64_t lastSync = df.getCurrentBlockStart();
        syncMetadata.emplace_back(lastSync, 0);
        for (int i = 0; i < numberOfRecords; i++) {
            df.write(TestRecord(largeString, (int64_t) i));

            // During the write, gather all the sync boundaries from the lastSync() API
            if (df.getCurrentBlockStart() != lastSync) {
                int recordsUptoSync =
                    i; // 1 less than total number of records written, since the sync block is sealed before a write
                syncMetadata.emplace_back(lastSync, recordsUptoSync);
                lastSync = df.getCurrentBlockStart();

                //::printf("\nPast current block start %llu, total rows upto sync %d", lastSync, recordsUptoSync);
            }
        }

        //::printf("\nPast current block start %llu, total rows upto sync %d", df.getCurrentBlockStart(), numberOfRecords);
        syncMetadata.emplace_back(df.getCurrentBlockStart(), numberOfRecords);

        df.flush();
        df.close();
    }

    // Validate that the sync points returned by the writer using the lastSync API and the reader are the same for every record
    {
        avro::DataFileReader<TestRecord> df(filename);
        TestRecord readRecord("", 0);

        for (int index = 0; index < numberOfRecords; index++) {
            int rowsRead = index;
            if (rowsRead > syncMetadata.front().second) {
                syncMetadata.pop_front();
            }

            BOOST_CHECK_EQUAL(df.previousSync(), syncMetadata.front().first);

            BOOST_CHECK_EQUAL(df.read(readRecord), true);

            int64_t expectedId = index;
            BOOST_CHECK_EQUAL(expectedId, readRecord.id);
        }

        if (numberOfRecords > syncMetadata.front().second) {
            syncMetadata.pop_front();
        }

        // validate previousSync matches even at the end of the file
        BOOST_CHECK_EQUAL(df.previousSync(), syncMetadata.front().first);
        BOOST_CHECK_EQUAL(1, syncMetadata.size()); // only 1 item must be remaining in the syncMetadata queue
    }
}

void testReadRecordEfficientlyUsingLastSync(avro::Codec codec) {

    //
    // This test highlights a motivating use case for the lastSync API on the DataFileWriter.
    //

    avro::ValidSchema writerSchema =
        avro::compileJsonSchemaFromString(schemaWithIdAndString);

    const size_t stringLen = 100;
    char largeString[stringLen + 1];
    for (size_t i = 0; i < stringLen; i++) {
        largeString[i] = 'a';
    }

    largeString[stringLen] = '\0';

    const char *filename = "test_readRecordUsingLastSync.df";

    size_t numberOfRecords = 100;
    size_t recordToRead = 37; // pick specific record to read efficiently
    size_t syncPointWithRecord = 0;
    size_t finalSync = 0;
    size_t recordsUptoLastSync = 0;
    size_t firstSyncPoint = 0;
    {
        avro::DataFileWriter<TestRecord> df(filename,
                                            writerSchema, 1024, codec);

        firstSyncPoint = df.getCurrentBlockStart();
        syncPointWithRecord = firstSyncPoint;
        for (size_t i = 0; i < numberOfRecords; i++) {
            df.write(TestRecord(largeString, (int64_t) i));

            // During the write, gather all the sync boundaries from the lastSync() API
            size_t recordsWritten = i + 1;
            if ((recordsWritten <= recordToRead) && (df.getCurrentBlockStart() != syncPointWithRecord)) {
                recordsUptoLastSync =
                    i; // 1 less than total number of records written, since the sync block is sealed before a write
                syncPointWithRecord = df.getCurrentBlockStart();

                //::printf("\nPast current block start %llu, total rows upto sync %d", syncPointWithRecord, recordsUptoLastSync);
            }
        }

        finalSync = df.getCurrentBlockStart();
        df.flush();
        df.close();
    }

    // Validate that we're able to stitch together {header block | specific block with record} and read the specific record from the stitched block
    {
        std::unique_ptr<avro::SeekableInputStream>
            seekableInputStream = avro::fileSeekableInputStream(filename, 1000000);

        const uint8_t *pData = nullptr;
        size_t length = 0;
        bool hasRead = seekableInputStream->next(&pData, &length);
        BOOST_CHECK(hasRead);

        // keep it simple, assume we've got in all data we want. We have a high buffersize to ensure this above.
        BOOST_CHECK_GE(length, firstSyncPoint);
        BOOST_CHECK_GE(length, finalSync);

        std::vector<uint8_t> stitchedData;
        // reserve space for header and data from specific block
        stitchedData.reserve(firstSyncPoint + (finalSync - syncPointWithRecord));

        // Copy header of the file
        std::copy(pData, pData + firstSyncPoint, std::back_inserter(stitchedData));

        // Copy data from the sync block containing the record of interest
        std::copy(pData + syncPointWithRecord, pData + finalSync, std::back_inserter(stitchedData));

        // Convert to inputStream
        std::unique_ptr<avro::InputStream>
            inputStream = avro::memoryInputStream(stitchedData.data(), stitchedData.size());

        int recordsUptoRecordToRead = recordToRead - recordsUptoLastSync;

        // Ensure this is not the first record in the chunk.
        BOOST_CHECK_GT(recordsUptoRecordToRead, 0);

        avro::DataFileReader<TestRecord> df(std::move(inputStream));
        TestRecord readRecord("", 0);
        //::printf("\nReading %d rows until specific record is reached", recordsUptoRecordToRead);
        for (int index = 0; index < recordsUptoRecordToRead; index++) {
            BOOST_CHECK_EQUAL(df.read(readRecord), true);

            int64_t expectedId = (recordToRead - recordsUptoRecordToRead + index);
            BOOST_CHECK_EQUAL(expectedId, readRecord.id);
        }

        // read specific record
        BOOST_CHECK_EQUAL(df.read(readRecord), true);
        BOOST_CHECK_EQUAL(recordToRead, readRecord.id);
    }
}

void testLastSyncNullCodec() {
    BOOST_TEST_CHECKPOINT(__func__);
    testLastSync(avro::NULL_CODEC);
}

void testLastSyncDeflateCodec() {
    BOOST_TEST_CHECKPOINT(__func__);
    testLastSync(avro::DEFLATE_CODEC);
}

#ifdef SNAPPY_CODEC_AVAILABLE
void testLastSyncSnappyCodec() {
    BOOST_TEST_CHECKPOINT(__func__);
    testLastSync(avro::SNAPPY_CODEC);
}
#endif

void testReadRecordEfficientlyUsingLastSyncNullCodec() {
    BOOST_TEST_CHECKPOINT(__func__);
    testReadRecordEfficientlyUsingLastSync(avro::NULL_CODEC);
}

void testReadRecordEfficientlyUsingLastSyncDeflateCodec() {
    BOOST_TEST_CHECKPOINT(__func__);
    testReadRecordEfficientlyUsingLastSync(avro::DEFLATE_CODEC);
}

#ifdef SNAPPY_CODEC_AVAILABLE
void testReadRecordEfficientlyUsingLastSyncSnappyCodec() {
    BOOST_TEST_CHECKPOINT(__func__);
    testReadRecordEfficientlyUsingLastSync(avro::SNAPPY_CODEC);
}
#endif

test_suite *
init_unit_test_suite(int argc, char *argv[]) {
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test0.df");
        shared_ptr<DataFileTest> t1(new DataFileTest("test1.d0", sch, isch, 0));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testWrite, t1));
        addReaderTests(ts, t1);
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test1.df");
        shared_ptr<DataFileTest> t1(new DataFileTest("test1.df", sch, isch));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testWrite, t1));
        addReaderTests(ts, t1);
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test1.defalate.df");
        shared_ptr<DataFileTest> t1(new DataFileTest("test1.deflate.df", sch, isch));
        ts->add(BOOST_CLASS_TEST_CASE(
            &DataFileTest::testWriteWithDeflateCodec, t1));
        addReaderTests(ts, t1);
        boost::unit_test::framework::master_test_suite().add(ts);
    }
#ifdef SNAPPY_CODEC_AVAILABLE
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test1.snappy.df");
        shared_ptr<DataFileTest> t1(new DataFileTest("test1.snappy.df", sch, isch));
        ts->add(BOOST_CLASS_TEST_CASE(
            &DataFileTest::testWriteWithSnappyCodec, t1));
        addReaderTests(ts, t1);
        boost::unit_test::framework::master_test_suite().add(ts);
    }
#endif
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test2.df");
        shared_ptr<DataFileTest> t2(new DataFileTest("test2.df", sch, isch));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testWriteGeneric, t2));
        addReaderTests(ts, t2);
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test3.df");
        shared_ptr<DataFileTest> t3(new DataFileTest("test3.df", dsch, dblsch));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testWriteDouble, t3));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testReadDouble, t3));
        ts->add(
            BOOST_CLASS_TEST_CASE(&DataFileTest::testReadDoubleTwoStep, t3));
        ts->add(BOOST_CLASS_TEST_CASE(
            &DataFileTest::testReadDoubleTwoStepProject, t3));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testCleanup, t3));
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test4.df");
        shared_ptr<DataFileTest> t4(new DataFileTest("test4.df", dsch, dblsch));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testTruncate, t4));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testCleanup, t4));
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test5.df");
        shared_ptr<DataFileTest> t5(new DataFileTest("test5.df", sch, isch));
        ts->add(
            BOOST_CLASS_TEST_CASE(&DataFileTest::testWriteGenericByName, t5));
        addReaderTests(ts, t5);
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test6.df");
        shared_ptr<DataFileTest> t6(new DataFileTest("test6.df", dsch, dblsch));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testZip, t6));
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test8.df");
        shared_ptr<DataFileTest> t8(new DataFileTest("test8.df", dsch, dblsch));
#ifdef SNAPPY_CODEC_AVAILABLE
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testSnappy, t8));
#endif
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test7.df");
        shared_ptr<DataFileTest> t7(new DataFileTest("test7.df", fsch, fsch));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testSchemaReadWrite, t7));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testCleanup, t7));
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test9.df");
        shared_ptr<DataFileTest> t9(new DataFileTest("test9.df", sch, sch));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testWrite, t9));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testReaderSyncSeek, t9));
        //ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testCleanup, t9));
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test10.df");
        shared_ptr<DataFileTest> t(new DataFileTest("test10.df", sch, sch));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testWrite, t));
        ts->add(
            BOOST_CLASS_TEST_CASE(&DataFileTest::testReaderSyncDiscovery, t));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testCleanup, t));
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test11.df");
        shared_ptr<DataFileTest> t(new DataFileTest("test11.df", sch, sch));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testWrite, t));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testReaderSplits, t));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testCleanup, t));
        boost::unit_test::framework::master_test_suite().add(ts);
    }
    {
        auto *ts = BOOST_TEST_SUITE("DataFile tests: test12.df");
        shared_ptr<DataFileTest> t(new DataFileTest("test12.df", ischWithDoc, ischWithDoc));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testWrite, t));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testSchemaReadWriteWithDoc, t));
        ts->add(BOOST_CLASS_TEST_CASE(&DataFileTest::testCleanup, t));
        boost::unit_test::framework::master_test_suite().add(ts);
    }

    boost::unit_test::framework::master_test_suite().add(BOOST_TEST_CASE(&testSkipStringNullCodec));
    boost::unit_test::framework::master_test_suite().add(BOOST_TEST_CASE(&testSkipStringDeflateCodec));
#ifdef SNAPPY_CODEC_AVAILABLE
    boost::unit_test::framework::master_test_suite().add(BOOST_TEST_CASE(&testSkipStringSnappyCodec));
#endif

    boost::unit_test::framework::master_test_suite().add(BOOST_TEST_CASE(&testLastSyncNullCodec));
    boost::unit_test::framework::master_test_suite().add(BOOST_TEST_CASE(&testLastSyncDeflateCodec));
#ifdef SNAPPY_CODEC_AVAILABLE
    boost::unit_test::framework::master_test_suite().add(BOOST_TEST_CASE(&testLastSyncSnappyCodec));
#endif

    boost::unit_test::framework::master_test_suite().add(BOOST_TEST_CASE(&testReadRecordEfficientlyUsingLastSyncNullCodec));
    boost::unit_test::framework::master_test_suite().add(BOOST_TEST_CASE(&testReadRecordEfficientlyUsingLastSyncDeflateCodec));
#ifdef SNAPPY_CODEC_AVAILABLE
    boost::unit_test::framework::master_test_suite().add(BOOST_TEST_CASE(&testReadRecordEfficientlyUsingLastSyncSnappyCodec));
#endif

    return nullptr;
}

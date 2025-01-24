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
#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <string.h>

#if defined(__clang__)
#pragma clang diagnostic ignored "-Winvalid-offsetof"
#endif

#include "testgen.hh"  // < generated header
#include "testgen2.hh" // < generated header

#include "Compiler.hh"
#include "Node.hh"
#include "Reader.hh"
#include "ResolverSchema.hh"
#include "ResolvingReader.hh"
#include "Serializer.hh"
#include "ValidSchema.hh"
#include "Writer.hh"
#include "buffer/BufferPrint.hh"

std::string gWriter("jsonschemas/bigrecord");
std::string gReader("jsonschemas/bigrecord2");

void printRecord(testgen::RootRecord &record) {
    using namespace testgen;
    std::cout << "mylong " << record.mylong << '\n';
    std::cout << "inval1 " << record.nestedrecord.inval1 << '\n';
    std::cout << "inval2 " << record.nestedrecord.inval2 << '\n';
    std::cout << "inval3 " << record.nestedrecord.inval3 << '\n';

    Map_of_int::MapType::const_iterator mapiter = record.mymap.value.begin();
    while (mapiter != record.mymap.value.end()) {
        std::cout << "mymap " << mapiter->first << " " << mapiter->second << '\n';
        ++mapiter;
    }

    Array_of_double::ArrayType::iterator arrayiter = record.myarray.value.begin();
    while (arrayiter != record.myarray.value.end()) {
        std::cout << "myarray " << *arrayiter << '\n';
        ++arrayiter;
    }

    std::cout << "myeum = " << record.myenum.value << '\n';

    if (record.myunion.choice == 1) {
        const Map_of_int &theMap = record.myunion.getValue<Map_of_int>();
        mapiter = theMap.value.begin();
        while (mapiter != theMap.value.end()) {
            std::cout << "unionmap " << mapiter->first << " " << mapiter->second << '\n';
            ++mapiter;
        }
    }

    if (record.anotherunion.choice == 0) {
        std::cout << "unionbytes ";
        const std::vector<uint8_t> &val = record.anotherunion.getValue<std::vector<uint8_t>>();
        for (size_t i = 0; i < val.size(); ++i) {
            std::cout << i << ":" << static_cast<int>(val[i]) << " ";
        }
        std::cout << '\n';
    }

    std::cout << "mybool " << record.mybool << '\n';
    std::cout << "inval1 " << record.anothernested.inval1 << '\n';
    std::cout << "inval2 " << record.anothernested.inval2 << '\n';
    std::cout << "inval3 " << record.anothernested.inval3 << '\n';

    std::cout << "fixed ";
    for (size_t i = 0; i < record.myfixed.fixedSize; ++i) {
        std::cout << i << ":" << static_cast<int>(record.myfixed.value[i]) << " ";
    }
    std::cout << '\n';

    std::cout << "anotherint " << record.anotherint << '\n';

    std::cout << "bytes ";
    for (size_t i = 0; i < record.bytes.size(); ++i) {
        std::cout << i << ":" << static_cast<int>(record.bytes[i]) << " ";
    }
    std::cout << '\n';
}

void printRecord(testgen2::RootRecord &record) {
    using namespace testgen2;
    std::cout << "mylong " << record.mylong << '\n';
    std::cout << "inval1 " << record.nestedrecord.inval1 << '\n';
    std::cout << "inval2 " << record.nestedrecord.inval2 << '\n';
    std::cout << "inval3 " << record.nestedrecord.inval3 << '\n';

    Map_of_long::MapType::const_iterator mapiter = record.mymap.value.begin();
    while (mapiter != record.mymap.value.end()) {
        std::cout << "mymap " << mapiter->first << " " << mapiter->second << '\n';
        ++mapiter;
    }

    Array_of_double::ArrayType::iterator arrayiter = record.myarray.value.begin();
    while (arrayiter != record.myarray.value.end()) {
        std::cout << "myarray " << *arrayiter << '\n';
        ++arrayiter;
    }

    std::cout << "myeum = " << record.myenum.value << '\n';

    if (record.myunion.choice == 1) {
        const Map_of_float &theMap = record.myunion.getValue<Map_of_float>();
        Map_of_float::MapType::const_iterator mapiter = theMap.value.begin();
        while (mapiter != theMap.value.end()) {
            std::cout << "unionmap " << mapiter->first << " " << mapiter->second << '\n';
            ++mapiter;
        }
    }

    std::cout << "unionbytes ";
    const std::vector<uint8_t> &val = record.anotherunion;
    for (size_t i = 0; i < val.size(); ++i) {
        std::cout << i << ":" << static_cast<int>(val[i]) << " ";
    }
    std::cout << '\n';

    std::cout << "inval1 " << record.anothernested.inval1 << '\n';
    std::cout << "inval2 " << record.anothernested.inval2 << '\n';
    std::cout << "inval3 " << record.anothernested.inval3 << '\n';

    if (record.myfixed.choice == 1) {
        const md5 &myfixed = record.myfixed.getValue<md5>();
        std::cout << "fixed ";
        for (size_t i = 0; i < myfixed.fixedSize; ++i) {
            std::cout << i << ":" << static_cast<int>(myfixed.value[i]) << " ";
        }
        std::cout << '\n';
    }

    std::cout << "anotherint " << record.anotherint << '\n';

    std::cout << "bytes ";
    for (size_t i = 0; i < record.bytes.size(); ++i) {
        std::cout << i << ":" << static_cast<int>(record.bytes[i]) << " ";
    }
    std::cout << '\n';
    std::cout << "newbool " << record.newbool << '\n';
}

void setRecord(testgen::RootRecord &myRecord) {
    using namespace testgen;

    uint8_t fixed[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    myRecord.mylong = 212;
    myRecord.nestedrecord.inval1 = std::numeric_limits<double>::min();
    myRecord.nestedrecord.inval2 = "hello world";
    myRecord.nestedrecord.inval3 = std::numeric_limits<int32_t>::max();

    Map_of_int::GenericSetter setter = myRecord.mymap.genericSetter;
    Map_of_int::ValueType *val = setter(&myRecord.mymap, "one");
    *val = 100;
    val = setter(&myRecord.mymap, "two");
    *val = 200;

    myRecord.myarray.addValue(3434.9);
    myRecord.myarray.addValue(7343.9);
    myRecord.myarray.addValue(-63445.9);
    myRecord.myenum.value = testgen::ExampleEnum::one;
    testgen::Map_of_int map;
    map.addValue("one", 1);
    map.addValue("two", 2);
    myRecord.myunion.set_Map_of_int(map);
    std::vector<uint8_t> vec;
    vec.push_back(1);
    vec.push_back(2);
    myRecord.anotherunion.set_bytes(vec);
    myRecord.mybool = true;
    myRecord.anothernested.inval1 = std::numeric_limits<double>::max();
    myRecord.anothernested.inval2 = "goodbye world";
    myRecord.anothernested.inval3 = std::numeric_limits<int32_t>::min();
    memcpy(myRecord.myfixed.value, fixed, testgen::md5::fixedSize);
    myRecord.anotherint = 4534;
    myRecord.bytes.push_back(10);
    myRecord.bytes.push_back(20);
}

struct TestCodeGenerator {

    void serializeToScreen() {
        std::cout << "Serialize:\n";
        avro::Writer writer;

        avro::serialize(writer, myRecord_);
        std::cout << writer.buffer();
        std::cout << "end Serialize\n";
    }

    void serializeToScreenValid() {
        std::cout << "Validated Serialize:\n";
        avro::ValidatingWriter writer(schema_);

        avro::serialize(writer, myRecord_);
        std::cout << writer.buffer();
        std::cout << "end Validated Serialize\n";
    }

    void checkArray(const testgen::Array_of_double &a1, const testgen::Array_of_double &a2) {
        BOOST_CHECK_EQUAL(a1.value.size(), 3U);
        BOOST_CHECK_EQUAL(a1.value.size(), a2.value.size());
        for (size_t i = 0; i < a1.value.size(); ++i) {
            BOOST_CHECK_EQUAL(a1.value[i], a2.value[i]);
        }
    }

    void checkMap(const testgen::Map_of_int &map1, const testgen::Map_of_int &map2) {
        BOOST_CHECK_EQUAL(map1.value.size(), map2.value.size());
        testgen::Map_of_int::MapType::const_iterator iter1 = map1.value.begin();
        testgen::Map_of_int::MapType::const_iterator end = map1.value.end();
        testgen::Map_of_int::MapType::const_iterator iter2 = map2.value.begin();

        while (iter1 != end) {
            BOOST_CHECK_EQUAL(iter1->first, iter2->first);
            BOOST_CHECK_EQUAL(iter1->second, iter2->second);
            ++iter1;
            ++iter2;
        }
    }

    void checkBytes(const std::vector<uint8_t> &v1, const std::vector<uint8_t> &v2) {
        BOOST_CHECK_EQUAL(v1.size(), 2U);
        BOOST_CHECK_EQUAL(v1.size(), v2.size());
        for (size_t i = 0; i < v1.size(); ++i) {
            BOOST_CHECK_EQUAL(v1[i], v2[i]);
        }
    }

    void checkNested(const testgen::Nested &rec1, const testgen::Nested &rec2) {
        BOOST_CHECK_EQUAL(rec1.inval1, rec2.inval1);
        BOOST_CHECK_EQUAL(rec1.inval2, rec2.inval2);
        BOOST_CHECK_EQUAL(rec1.inval3, rec2.inval3);
    }

    void checkOk(const testgen::RootRecord &rec1, const testgen::RootRecord &rec2) {
        BOOST_CHECK_EQUAL(rec1.mylong, rec1.mylong);

        checkNested(rec1.nestedrecord, rec2.nestedrecord);
        checkMap(rec1.mymap, rec2.mymap);
        checkArray(rec1.myarray, rec2.myarray);

        BOOST_CHECK_EQUAL(rec1.myenum.value, rec2.myenum.value);

        BOOST_CHECK_EQUAL(rec1.myunion.choice, rec2.myunion.choice);
        // in this test I know choice was 1
        {
            BOOST_CHECK_EQUAL(rec1.myunion.choice, 1);
            checkMap(rec1.myunion.getValue<testgen::Map_of_int>(), rec2.myunion.getValue<testgen::Map_of_int>());
        }

        BOOST_CHECK_EQUAL(rec1.anotherunion.choice, rec2.anotherunion.choice);
        // in this test I know choice was 0
        {
            BOOST_CHECK_EQUAL(rec1.anotherunion.choice, 0);
            using mytype = std::vector<uint8_t>;
            checkBytes(rec1.anotherunion.getValue<mytype>(),
                       rec2.anotherunion.getValue<testgen::Union_of_bytes_null::T0>());
        }

        checkNested(rec1.anothernested, rec2.anothernested);

        BOOST_CHECK_EQUAL(rec1.mybool, rec2.mybool);

        for (int i = 0; i < static_cast<int>(testgen::md5::fixedSize); ++i) {
            BOOST_CHECK_EQUAL(rec1.myfixed.value[i], rec2.myfixed.value[i]);
        }
        BOOST_CHECK_EQUAL(rec1.anotherint, rec2.anotherint);

        checkBytes(rec1.bytes, rec2.bytes);
    }

    void testParser() {
        avro::Writer s;

        avro::serialize(s, myRecord_);

        testgen::RootRecord inRecord;
        avro::Reader p(s.buffer());
        avro::parse(p, inRecord);

        checkOk(myRecord_, inRecord);
    }

    void testParserValid() {
        avro::ValidatingWriter s(schema_);

        avro::serialize(s, myRecord_);

        testgen::RootRecord inRecord;
        avro::ValidatingReader p(schema_, s.buffer());
        avro::parse(p, inRecord);

        checkOk(myRecord_, inRecord);
    }

    void testNameIndex() {
        const avro::NodePtr &node = schema_.root();
        size_t index = 0;
        bool found = node->nameIndex("anothernested", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 8U);

        found = node->nameIndex("myenum", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 4U);

        const avro::NodePtr &enumNode = node->leafAt(index);
        found = enumNode->nameIndex("one", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 1U);
    }

    void test() {
        std::cout << "Running code generation tests\n";

        testNameIndex();

        serializeToScreen();
        serializeToScreenValid();

        testParser();
        testParserValid();

        std::cout << "Finished code generation tests\n";
    }

    TestCodeGenerator() {
        setRecord(myRecord_);
        std::ifstream in(gWriter.c_str());
        avro::compileJsonSchema(in, schema_);
    }

    testgen::RootRecord myRecord_;
    avro::ValidSchema schema_;
};

struct TestSchemaResolving {

    void checkArray(const testgen::Array_of_double &a1, const testgen2::Array_of_double &a2) {
        BOOST_CHECK_EQUAL(a1.value.size(), 3U);
        BOOST_CHECK_EQUAL(a1.value.size(), a2.value.size());
        for (size_t i = 0; i < a1.value.size(); ++i) {
            BOOST_CHECK_EQUAL(a1.value[i], a2.value[i]);
        }
    }

    void checkMap(const testgen::Map_of_int &map1, const testgen2::Map_of_long &map2) {
        BOOST_CHECK_EQUAL(map1.value.size(), map2.value.size());
        testgen::Map_of_int::MapType::const_iterator iter1 = map1.value.begin();
        testgen::Map_of_int::MapType::const_iterator end = map1.value.end();
        testgen2::Map_of_long::MapType::const_iterator iter2 = map2.value.begin();

        while (iter1 != end) {
            BOOST_CHECK_EQUAL(iter1->first, iter2->first);
            BOOST_CHECK_EQUAL(static_cast<float>(iter1->second), iter2->second);
            ++iter1;
            ++iter2;
        }
    }

    void checkMap(const testgen::Map_of_int &map1, const testgen2::Map_of_float &map2) {
        BOOST_CHECK_EQUAL(map1.value.size(), map2.value.size());
        testgen::Map_of_int::MapType::const_iterator iter1 = map1.value.begin();
        testgen::Map_of_int::MapType::const_iterator end = map1.value.end();
        testgen2::Map_of_float::MapType::const_iterator iter2 = map2.value.begin();

        while (iter1 != end) {
            BOOST_CHECK_EQUAL(iter1->first, iter2->first);
            BOOST_CHECK_EQUAL(static_cast<int64_t>(iter1->second), iter2->second);
            ++iter1;
            ++iter2;
        }
    }

    void checkBytes(const std::vector<uint8_t> &v1, const std::vector<uint8_t> &v2) {
        BOOST_CHECK_EQUAL(v1.size(), 2U);
        BOOST_CHECK_EQUAL(v1.size(), v2.size());
        for (size_t i = 0; i < v1.size(); ++i) {
            BOOST_CHECK_EQUAL(v1[i], v2[i]);
        }
    }

    void checkNested(const testgen::Nested &rec1, const testgen2::Nested &rec2) {
        BOOST_CHECK_EQUAL(rec1.inval1, rec2.inval1);
        BOOST_CHECK_EQUAL(rec1.inval2, rec2.inval2);
        BOOST_CHECK_EQUAL(rec1.inval3, rec2.inval3);
    }

    void checkOk(const testgen::RootRecord &rec1, const testgen2::RootRecord &rec2) {
        BOOST_CHECK_EQUAL(rec1.mylong, rec1.mylong);

        checkNested(rec1.nestedrecord, rec2.nestedrecord);
        checkMap(rec1.mymap, rec2.mymap);
        checkArray(rec1.myarray, rec2.myarray);

        // enum was remapped from 1 to 2
        BOOST_CHECK_EQUAL(rec1.myenum.value, 1);
        BOOST_CHECK_EQUAL(rec2.myenum.value, 2);

        // in this test I know choice was 1
        {
            BOOST_CHECK_EQUAL(rec1.myunion.choice, 1);
            BOOST_CHECK_EQUAL(rec2.myunion.choice, 2);
            checkMap(rec1.myunion.getValue<testgen::Map_of_int>(), rec2.myunion.getValue<testgen2::Map_of_float>());
        }

        {
            BOOST_CHECK_EQUAL(rec1.anotherunion.choice, 0);
            using mytype = std::vector<uint8_t>;
            checkBytes(rec1.anotherunion.getValue<mytype>(), rec2.anotherunion);
        }

        checkNested(rec1.anothernested, rec2.anothernested);

        BOOST_CHECK_EQUAL(rec2.newbool, false);

        BOOST_CHECK_EQUAL(rec2.myfixed.choice, 1);
        {
            const testgen2::md5 &myfixed2 = rec2.myfixed.getValue<testgen2::md5>();
            for (int i = 0; i < static_cast<int>(testgen::md5::fixedSize); ++i) {
                BOOST_CHECK_EQUAL(rec1.myfixed.value[i], myfixed2.value[i]);
            }
        }
    }

    avro::InputBuffer serializeWriteRecordToBuffer() {
        std::ostringstream ostring;
        avro::Writer s;
        avro::serialize(s, writeRecord_);
        return s.buffer();
    }

    void parseData(const avro::InputBuffer &buf, avro::ResolverSchema &xSchema) {
        avro::ResolvingReader r(xSchema, buf);

        avro::parse(r, readRecord_);
    }

    void test() {
        std::cout << "Running schema resolution tests\n";
        testgen2::RootRecord_Layout layout;

        avro::ResolverSchema xSchema(writerSchema_, readerSchema_, layout);

        printRecord(writeRecord_);

        avro::InputBuffer buffer = serializeWriteRecordToBuffer();
        parseData(buffer, xSchema);

        printRecord(readRecord_);

        checkOk(writeRecord_, readRecord_);
        std::cout << "Finished schema resolution tests\n";
    }

    TestSchemaResolving() {
        setRecord(writeRecord_);
        std::ifstream win(gWriter.c_str());
        avro::compileJsonSchema(win, writerSchema_);

        std::ifstream rin(gReader.c_str());
        avro::compileJsonSchema(rin, readerSchema_);
    }

    testgen::RootRecord writeRecord_;
    avro::ValidSchema writerSchema_;

    testgen2::RootRecord readRecord_;
    avro::ValidSchema readerSchema_;
};

template<typename T>
void addTestCase(boost::unit_test::test_suite &test) {
    std::shared_ptr<T> newtest(new T);
    test.add(BOOST_CLASS_TEST_CASE(&T::test, newtest));
}

boost::unit_test::test_suite *
init_unit_test_suite(int argc, char *argv[]) {
    using namespace boost::unit_test;

    const char *srcPath = getenv("top_srcdir");

    if (srcPath) {
        std::string srcPathStr(srcPath);
        gWriter = srcPathStr + '/' + gWriter;
        gReader = srcPathStr + '/' + gReader;
    } else {
        if (argc > 1) {
            gWriter = argv[1];
        }

        if (argc > 2) {
            gReader = argv[2];
        }
    }
    std::cout << "Using writer schema " << gWriter << std::endl;
    std::cout << "Using reader schema " << gReader << std::endl;

    test_suite *test = BOOST_TEST_SUITE("Avro C++ unit test suite");

    addTestCase<TestCodeGenerator>(*test);
    addTestCase<TestSchemaResolving>(*test);

    return test;
}

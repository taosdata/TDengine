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
#include <iostream>

#include "Compiler.hh"
#include "Decoder.hh"
#include "Encoder.hh"
#include "Node.hh"
#include "Parser.hh"
#include "Schema.hh"
#include "SchemaResolution.hh"
#include "Serializer.hh"
#include "Stream.hh"
#include "ValidSchema.hh"
#include "Zigzag.hh"
#include "boost/make_shared.hpp"
#include "boost/shared_ptr.hpp"
#include "buffer/BufferPrint.hh"
#include "buffer/BufferStream.hh"

#include "AvroSerialize.hh"

using namespace avro;

static const uint8_t fixeddata[16] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

#ifdef max
#undef max
#endif
struct TestSchema {
    TestSchema() = default;

    static void createExampleSchema() {
        // First construct our complex data type:
        avro::RecordSchema myRecord("complex");

        // Now populate my record with fields (each field is another schema):
        myRecord.addField("real", avro::DoubleSchema());
        myRecord.addField("imaginary", avro::DoubleSchema());

        // The complex record is the same as used above, let's make a schema
        // for an array of these record

        avro::ArraySchema complexArray(myRecord);

        avro::ValidSchema validComplexArray(complexArray);
        validComplexArray.toJson(std::cout);
    }

    void buildSchema() {
        RecordSchema record("RootRecord");

        record.addField("mylong", LongSchema());

        IntSchema intSchema;
        avro::MapSchema map = MapSchema(IntSchema());

        record.addField("mymap", map);

        ArraySchema array = ArraySchema(DoubleSchema());

        const std::string s("myarray");
        record.addField(s, array);

        EnumSchema myenum("ExampleEnum");
        myenum.addSymbol("zero");
        myenum.addSymbol("one");
        myenum.addSymbol("two");
        myenum.addSymbol("three");

        bool caught = false;
        try {
            myenum.addSymbol("three");
        } catch (Exception &e) {
            std::cout << "(intentional) exception: " << e.what() << '\n';
            caught = true;
        }
        BOOST_CHECK_EQUAL(caught, true);

        record.addField("myenum", myenum);

        UnionSchema onion;
        onion.addType(NullSchema());
        onion.addType(map);
        onion.addType(FloatSchema());

        record.addField("myunion", onion);

        RecordSchema nestedRecord("NestedRecord");
        nestedRecord.addField("floatInNested", FloatSchema());

        record.addField("nested", nestedRecord);

        record.addField("mybool", BoolSchema());
        FixedSchema fixed(16, "fixed16");
        record.addField("myfixed", fixed);

        caught = false;
        try {
            record.addField("mylong", LongSchema());
        } catch (Exception &e) {
            std::cout << "(intentional) exception: " << e.what() << '\n';
            caught = true;
        }
        BOOST_CHECK_EQUAL(caught, true);

        record.addField("mylong2", LongSchema());

        record.addField("anotherint", intSchema);

        schema_.setSchema(record);
    }

    void checkNameLookup() const {
        NodePtr node = schema_.root();

        size_t index = 0;
        bool found = node->nameIndex("mylongxxx", index);
        BOOST_CHECK_EQUAL(found, false);

        found = node->nameIndex("mylong", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 0U);

        found = node->nameIndex("mylong2", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 8U);

        found = node->nameIndex("myenum", index);
        BOOST_CHECK_EQUAL(found, true);
        NodePtr enumNode = node->leafAt(index);

        found = enumNode->nameIndex("one", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 1U);

        found = enumNode->nameIndex("three", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 3U);

        found = enumNode->nameIndex("four", index);
        BOOST_CHECK_EQUAL(found, false);
    }

    template<typename Serializer>
    void printUnion(Serializer &s, int path) {
        s.writeUnion(path);
        if (path == 0) {
            std::cout << "Null in union\n";
            s.writeNull();
        } else if (path == 1) {
            std::cout << "Map in union\n";
            s.writeMapBlock(2);
            s.writeString("Foo");
            s.writeInt(16);
            s.writeString("Bar");
            s.writeInt(17);
            s.writeMapBlock(1);
            s.writeString("FooBar");
            s.writeInt(18);
            s.writeMapEnd();
        } else {
            std::cout << "Float in union\n";
            s.writeFloat(200.);
        }
    }

    template<typename Serializer>
    void writeEncoding(Serializer &s, int path) {
        std::cout << "Record\n";
        s.writeRecord();
        s.writeInt(1000);

        std::cout << "Map\n";
        s.writeMapBlock(2);
        s.writeString(std::string("Foo"));
        s.writeInt(16);
        s.writeString(std::string("Bar"));
        s.writeInt(17);
        s.writeMapEnd();

        std::cout << "Array\n";
        s.writeArrayBlock(2);
        s.writeDouble(100.0);
        s.writeDouble(1000.0);
        s.writeArrayEnd();

        std::cout << "Enum\n";
        s.writeEnum(3);

        std::cout << "Union\n";
        printUnion(s, path);

        std::cout << "Record\n";
        s.writeRecord();
        s.writeFloat(-101.101f);
        s.writeRecordEnd();

        std::cout << "Bool\n";
        s.writeBool(true);

        std::cout << "Fixed16\n";

        s.writeFixed(fixeddata);

        std::cout << "Long\n";
        s.writeLong(7010728798977672067LL);

        std::cout << "Int\n";
        s.writeInt(-3456);
        s.writeRecordEnd();
    }

    void printEncoding() {
        std::cout << "Encoding\n";
        Serializer<Writer> s;
        writeEncoding(s, 0);
        std::cout << s.buffer();
    }

    void printValidatingEncoding(int path) {
        std::cout << "Validating Encoding " << path << "\n";
        Serializer<ValidatingWriter> s(schema_);
        writeEncoding(s, path);
        std::cout << s.buffer();
    }

    void saveValidatingEncoding(int path) {
        std::ofstream out("test.avro");
        Serializer<ValidatingWriter> s(schema_);
        writeEncoding(s, path);
        InputBuffer buf = s.buffer();
        istream is(buf);
        out << is.rdbuf();
    }

    void printNext(Parser<Reader> &p) {
        // no-op printer
    }

    static void printNext(Parser<ValidatingReader> &p) {
        std::cout << "Next: \"" << nextType(p);
        std::string recordName;
        std::string fieldName;
        if (currentRecordName(p, recordName)) {
            std::cout << "\" record: \"" << recordName;
        }
        if (nextFieldName(p, fieldName)) {
            std::cout << "\" field: \"" << fieldName;
        }
        std::cout << "\"\n";
    }

    template<typename Parser>
    void readMap(Parser &p) {
        int64_t size;
        do {
            printNext(p);
            size = p.readMapBlockSize();
            std::cout << "Size " << size << '\n';
            for (int64_t i = 0; i < size; ++i) {
                std::string key;
                printNext(p);
                p.readString(key);
                printNext(p);
                int32_t intval = p.readInt();
                std::cout << key << ":" << intval << '\n';
            }
        } while (size != 0);
    }

    template<typename Parser>
    void readArray(Parser &p) {
        int64_t size;
        double d = 0.0;
        do {
            printNext(p);
            size = p.readArrayBlockSize();
            std::cout << "Size " << size << '\n';
            for (int64_t i = 0; i < size; ++i) {
                printNext(p);
                d = p.readDouble();
                std::cout << i << ":" << d << '\n';
            }
        } while (size != 0);
        BOOST_CHECK_EQUAL(d, 1000.0);
    }

    template<typename Parser>
    void readNestedRecord(Parser &p) {
        printNext(p);
        p.readRecord();
        printNext(p);
        float f = p.readFloat();
        std::cout << f << '\n';
        BOOST_CHECK_EQUAL(f, -101.101f);
        p.readRecordEnd();
    }

    template<typename Parser>
    void readFixed(Parser &p) {

        std::array<uint8_t, 16> input{};
        p.readFixed(input);
        BOOST_CHECK_EQUAL(input.size(), 16U);

        for (int i = 0; i < 16; ++i) {
            std::cout << static_cast<int>(input[i]) << ' ';
        }
        std::cout << '\n';
    }

    template<typename Parser>
    void readData(Parser &p) {
        printNext(p);
        p.readRecord();

        printNext(p);
        int64_t longval = p.readLong();
        std::cout << longval << '\n';
        BOOST_CHECK_EQUAL(longval, 1000);

        readMap(p);
        readArray(p);

        printNext(p);
        longval = p.readEnum();
        std::cout << "Enum choice " << longval << '\n';

        printNext(p);
        longval = p.readUnion();
        std::cout << "Union path " << longval << '\n';
        readMap(p);

        readNestedRecord(p);

        printNext(p);
        bool boolval = p.readBool();
        std::cout << boolval << '\n';
        BOOST_CHECK_EQUAL(boolval, true);

        printNext(p);
        readFixed(p);

        printNext(p);
        longval = p.readLong();
        std::cout << longval << '\n';
        BOOST_CHECK_EQUAL(longval, 7010728798977672067LL);

        printNext(p);
        int32_t intval = p.readInt();
        std::cout << intval << '\n';
        BOOST_CHECK_EQUAL(intval, -3456);
        p.readRecordEnd();
    }

    void readRawData() {
        std::ifstream in("test.avro");
        ostream os;
        os << in.rdbuf();
        Parser<Reader> p(os.getBuffer());
        readData(p);
    }

    void readValidatedData() {
        std::ifstream in("test.avro");
        ostream os;
        os << in.rdbuf();
        Parser<ValidatingReader> p(schema_, os.getBuffer());
        readData(p);
    }

    void test() {
        std::cout << "Before\n";
        schema_.toJson(std::cout);
        schema_.toFlatList(std::cout);
        buildSchema();
        std::cout << "After\n";
        schema_.toJson(std::cout);
        schema_.toFlatList(std::cout);

        checkNameLookup();

        printEncoding();
        printValidatingEncoding(0);
        printValidatingEncoding(1);
        printValidatingEncoding(2);

        saveValidatingEncoding(1);
        readRawData();
        readValidatedData();

        createExampleSchema();
    }

    ValidSchema schema_;
};

struct TestEncoding {

    static void compare(int32_t val) {
        uint32_t encoded = encodeZigzag32(val);
        BOOST_CHECK_EQUAL(decodeZigzag32(encoded), val);
    }

    static void compare(int64_t val) {
        uint64_t encoded = encodeZigzag64(val);
        BOOST_CHECK_EQUAL(decodeZigzag64(encoded), val);
    }

    template<typename IntType>
    void testEncoding(IntType start, IntType stop) {
        std::cout << "testing from " << start << " to " << stop << " inclusive\n";
        IntType val = start;
        IntType diff = stop - start + 1;

        for (IntType i = 0; i < diff; ++i) {
            compare(val + i);
        }
    }

    template<typename IntType>
    void testEncoding() {
        testEncoding<IntType>(std::numeric_limits<IntType>::min(), std::numeric_limits<IntType>::min() + 1000);
        testEncoding<IntType>(-1000, 1000);
        testEncoding<IntType>(std::numeric_limits<IntType>::max() - 1000, std::numeric_limits<IntType>::max());
    }

    void test() {
        testEncoding<int32_t>();
        testEncoding<int64_t>();
    }
};

struct TestNested {
    TestNested() = default;

    void createSchema() {
        std::cout << "TestNested\n";
        RecordSchema rec("LongListContainer");

        RecordSchema list("LongList");
        list.addField("value", LongSchema());
        UnionSchema next;
        next.addType(NullSchema());
        next.addType(SymbolicSchema(Name("LongList"), list.root()));
        list.addField("next", next);
        list.addField("end", BoolSchema());
        rec.addField("list", list);

        RecordSchema arrayTree("ArrayTree");
        arrayTree.addField("label", StringSchema());
        arrayTree.addField("children", ArraySchema(SymbolicSchema(Name("ArrayTree"), arrayTree.root())));
        rec.addField("array_tree", arrayTree);

        schema_.setSchema(rec);
        schema_.toJson(std::cout);
        schema_.toFlatList(std::cout);
    }

    InputBuffer serializeNoRecurse() const {
        std::cout << "No recurse\n";
        Serializer<ValidatingWriter> s(schema_);
        s.writeRecord();
        {
            s.writeRecord();
            s.writeLong(1);
            s.writeUnion(0);
            s.writeNull();
            s.writeBool(true);
            s.writeRecordEnd();
        }
        {
            s.writeRecord();
            s.writeString("hello world");
            s.writeArrayEnd();
            s.writeRecordEnd();
        }
        s.writeRecordEnd();

        return s.buffer();
    }

    static void encodeNoRecurse(Encoder &e) {
        std::cout << "Encode no recurse\n";
        e.encodeLong(1);
        e.encodeUnionIndex(0);
        e.encodeNull();
        e.encodeBool(true);

        e.encodeString("hello world");
        e.arrayStart();
        e.arrayEnd();
    }

    InputBuffer serializeRecurse() const {
        std::cout << "Recurse\n";
        Serializer<ValidatingWriter> s(schema_);
        s.writeRecord();
        {
            s.writeRecord();
            s.writeLong(1);
            s.writeUnion(1);
            {
                s.writeRecord();
                s.writeLong(2);
                s.writeUnion(1);
                {
                    s.writeRecord();
                    s.writeLong(3);
                    s.writeUnion(0);
                    { s.writeNull(); }
                    s.writeBool(false);
                    s.writeRecordEnd();
                }
                s.writeBool(false);
                s.writeRecordEnd();
            }
            s.writeBool(true);
            s.writeRecordEnd();
        }
        {
            s.writeRecord();
            s.writeString("a");
            s.writeArrayBlock(2);
            {
                s.writeRecord();
                s.writeString("aa");
                s.writeArrayBlock(1);
                {
                    s.writeRecord();
                    s.writeString("aaa");
                    s.writeArrayEnd();
                    s.writeRecordEnd();
                }
                s.writeArrayEnd();
                s.writeRecordEnd();
            }
            {
                s.writeRecord();
                s.writeString("ab");
                s.writeArrayEnd();
                s.writeRecordEnd();
            }
            s.writeArrayEnd();
            s.writeRecordEnd();
        }
        s.writeRecordEnd();

        return s.buffer();
    }

    static void encodeRecurse(Encoder &e) {
        std::cout << "Encode recurse\n";
        e.encodeLong(1);
        e.encodeUnionIndex(1);
        {
            e.encodeLong(2);
            e.encodeUnionIndex(1);
            {
                e.encodeLong(3);
                e.encodeUnionIndex(0);
                {
                    e.encodeNull();
                }
                e.encodeBool(false);
            }
            e.encodeBool(false);
        }
        e.encodeBool(true);

        e.encodeString("a");
        e.arrayStart();
        e.setItemCount(2);
        {
            e.encodeString("aa");
            e.arrayStart();
            e.setItemCount(1);
            {
                e.encodeString("aaa");
                e.arrayStart();
                e.arrayEnd();
            }
            e.arrayEnd();
        }
        {
            e.encodeString("ab");
            e.arrayStart();
            e.arrayEnd();
        }
        e.arrayEnd();
    }

    void readRecord(Parser<ValidatingReader> &p) {
        p.readRecord();
        readListRecord(p);
        readArrayRecord(p);
        p.readRecordEnd();
    }

    void readListRecord(Parser<ValidatingReader> &p) {
        p.readRecord();
        int64_t val = p.readLong();
        std::cout << "longval = " << val << '\n';
        int64_t path = p.readUnion();
        if (path == 1) {
            readListRecord(p);
        } else {
            p.readNull();
        }
        bool b = p.readBool();
        std::cout << "bval = " << b << '\n';
        p.readRecordEnd();
    }

    void readArrayRecord(Parser<ValidatingReader> &p) {
        p.readRecord();
        std::string label;
        p.readString(label);
        std::cout << "label = " << label << '\n';
        for (int64_t bs = p.readArrayBlockSize(); bs > 0;
             bs = p.readArrayBlockSize()) {
            for (int64_t i = 0; i < bs; ++i) {
                readArrayRecord(p);
            }
        }
        p.readRecordEnd();
    }

    void validatingParser(InputBuffer &buf) {
        Parser<ValidatingReader> p(schema_, buf);
        readRecord(p);
    }

    void decodeListRecord(Decoder &d) {
        int64_t val = d.decodeLong();
        std::cout << "longval = " << val << '\n';
        int64_t path = d.decodeUnionIndex();
        if (path == 1) {
            decodeListRecord(d);
        } else {
            d.decodeNull();
        }
        bool b = d.decodeBool();
        std::cout << "bval = " << b << '\n';
    }

    void decodeArrayRecord(Decoder &d) {
        std::string label = d.decodeString();
        std::cout << "label = " << label << '\n';
        for (int64_t bs = d.arrayStart(); bs > 0; bs = d.arrayNext()) {
            std::cout << "array block size = " << bs << '\n';
            for (int64_t i = 0; i < bs; ++i) {
                decodeArrayRecord(d);
            }
        }
    }

    void runDecoder(Decoder &d) {
        decodeListRecord(d);
        decodeArrayRecord(d);
    }

    void testToScreen() const {
        InputBuffer buf1 = serializeNoRecurse();
        InputBuffer buf2 = serializeRecurse();
        std::cout << buf1;
        std::cout << buf2;
    }

    // Tests for Serializer + Parser
    void testParseNoRecurse() {
        std::cout << "ParseNoRecurse\n";
        InputBuffer buf = serializeNoRecurse();

        validatingParser(buf);
    }

    void testParseRecurse() {
        std::cout << "ParseRecurse\n";
        InputBuffer buf = serializeRecurse();

        validatingParser(buf);
    }

    // Tests for encode + decode
    void runEncodeDecode(Encoder &e, Decoder &d, void (*encode_fn)(Encoder &)) {
        std::unique_ptr<OutputStream> out = memoryOutputStream();
        e.init(*out);
        encode_fn(e);
        std::unique_ptr<InputStream> in = memoryInputStream(*out);
        d.init(*in);
        runDecoder(d);
    }

    void testDecodeNoRecurse() {
        std::cout << "DecodeNoRecurse\n";
        runEncodeDecode(*validatingEncoder(schema_, binaryEncoder()),
                        *validatingDecoder(schema_, binaryDecoder()),
                        encodeNoRecurse);
    }

    void testDecodeRecurse() {
        std::cout << "DecodeRecurse\n";
        runEncodeDecode(*validatingEncoder(schema_, binaryEncoder()),
                        *validatingDecoder(schema_, binaryDecoder()),
                        encodeRecurse);
    }

    void testDecodeNoRecurseJson() {
        std::cout << "DecodeNoRecurseJson\n";
        runEncodeDecode(*jsonEncoder(schema_),
                        *jsonDecoder(schema_),
                        encodeNoRecurse);
    }

    void testDecodeRecurseJson() {
        std::cout << "DecodeRecurseJson\n";
        runEncodeDecode(*jsonEncoder(schema_),
                        *jsonDecoder(schema_),
                        encodeRecurse);
    }

    void test() {
        createSchema();
        testToScreen();

        testParseNoRecurse();
        testParseRecurse();

        testDecodeNoRecurse();
        testDecodeRecurse();
        testDecodeNoRecurseJson();
        testDecodeRecurseJson();
    }

    ValidSchema schema_;
};

struct TestGenerated {
    TestGenerated() = default;

    void test() {
        std::cout << "TestGenerated\n";

        int32_t val = 100;
        float f = 200.0;

        Writer writer;

        serialize(writer, val);
        serialize(writer, Null());
        serialize(writer, f);

        std::cout << writer.buffer();
    }
};

struct TestBadStuff {
    void testBadFile() {
        std::cout << "TestBadFile\n";

        avro::ValidSchema schema;
        std::ifstream in("agjoewejefkjs");
        std::string error;
        bool result = avro::compileJsonSchema(in, schema, error);
        BOOST_CHECK_EQUAL(result, false);
        std::cout << "(intentional) error: " << error << '\n';
    }

    void testBadSchema() {
        std::cout << "TestBadSchema\n";

        std::string str(R"({ "type" : "wrong" })");
        std::istringstream in(str);

        avro::ValidSchema schema;
        std::string error;
        bool result = avro::compileJsonSchema(in, schema, error);
        BOOST_CHECK_EQUAL(result, false);
        std::cout << "(intentional) error: " << error << '\n';
    }

    void test() {
        std::cout << "TestBadStuff\n";
        testBadFile();
        testBadSchema();
    }
};

struct TestResolution {
    TestResolution() : int_(IntSchema()),
                       long_(LongSchema()),
                       bool_(BoolSchema()),
                       float_(FloatSchema()),
                       double_(DoubleSchema()),

                       mapOfInt_(MapSchema(IntSchema())),
                       mapOfDouble_(MapSchema(DoubleSchema())),

                       arrayOfLong_(ArraySchema(LongSchema())),
                       arrayOfFloat_(ArraySchema(FloatSchema())) {
        {
            EnumSchema one("one");
            one.addSymbol("X");
            enumOne_.setSchema(one);

            EnumSchema two("two");
            two.addSymbol("Y");
            enumTwo_.setSchema(two);
        }

        {
            UnionSchema one;
            one.addType(IntSchema());
            one.addType(FloatSchema());
            unionOne_.setSchema(one);

            UnionSchema two;
            two.addType(IntSchema());
            two.addType(DoubleSchema());
            unionTwo_.setSchema(two);
        }
    }

    SchemaResolution resolve(const ValidSchema &writer, const ValidSchema &reader) {
        return writer.root()->resolve(*reader.root());
    }

    void test() {
        std::cout << "TestResolution\n";

        BOOST_CHECK_EQUAL(resolve(long_, long_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(long_, bool_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(bool_, long_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(int_, long_), RESOLVE_PROMOTABLE_TO_LONG);
        BOOST_CHECK_EQUAL(resolve(long_, int_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(int_, float_), RESOLVE_PROMOTABLE_TO_FLOAT);
        BOOST_CHECK_EQUAL(resolve(float_, int_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(int_, double_), RESOLVE_PROMOTABLE_TO_DOUBLE);
        BOOST_CHECK_EQUAL(resolve(double_, int_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(long_, float_), RESOLVE_PROMOTABLE_TO_FLOAT);
        BOOST_CHECK_EQUAL(resolve(float_, long_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(long_, double_), RESOLVE_PROMOTABLE_TO_DOUBLE);
        BOOST_CHECK_EQUAL(resolve(double_, long_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(float_, double_), RESOLVE_PROMOTABLE_TO_DOUBLE);
        BOOST_CHECK_EQUAL(resolve(double_, float_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(int_, mapOfInt_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(mapOfInt_, int_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(mapOfInt_, mapOfInt_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(mapOfDouble_, mapOfInt_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(mapOfInt_, mapOfDouble_), RESOLVE_PROMOTABLE_TO_DOUBLE);

        BOOST_CHECK_EQUAL(resolve(long_, arrayOfLong_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(arrayOfLong_, long_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(arrayOfLong_, arrayOfLong_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(arrayOfFloat_, arrayOfLong_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(arrayOfLong_, arrayOfFloat_), RESOLVE_PROMOTABLE_TO_FLOAT);

        BOOST_CHECK_EQUAL(resolve(enumOne_, enumOne_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(enumOne_, enumTwo_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(float_, unionOne_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(double_, unionOne_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(float_, unionTwo_), RESOLVE_PROMOTABLE_TO_DOUBLE);

        BOOST_CHECK_EQUAL(resolve(unionOne_, float_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(unionOne_, double_), RESOLVE_PROMOTABLE_TO_DOUBLE);
        BOOST_CHECK_EQUAL(resolve(unionTwo_, float_), RESOLVE_PROMOTABLE_TO_FLOAT);
        BOOST_CHECK_EQUAL(resolve(unionOne_, unionTwo_), RESOLVE_MATCH);
    }

private:
    ValidSchema int_;
    ValidSchema long_;
    ValidSchema bool_;
    ValidSchema float_;
    ValidSchema double_;

    ValidSchema mapOfInt_;
    ValidSchema mapOfDouble_;

    ValidSchema arrayOfLong_;
    ValidSchema arrayOfFloat_;

    ValidSchema enumOne_;
    ValidSchema enumTwo_;

    ValidSchema unionOne_;
    ValidSchema unionTwo_;
};

void testNestedArraySchema() {
    ArraySchema b0 = ArraySchema(NullSchema());
    ArraySchema a0 = ArraySchema(b0);

    avro::ValidSchema vs(a0);
    std::ostringstream actual;
    vs.toJson(actual);

    std::string expected = "{\n\
    \"type\": \"array\",\n\
    \"items\": {\n\
        \"type\": \"array\",\n\
        \"items\": \"null\"\n\
    }\n\
}\n";
    BOOST_CHECK_EQUAL(expected, actual.str());
}

void testNestedMapSchema() {
    MapSchema b0 = MapSchema(NullSchema());
    MapSchema a0 = MapSchema(b0);

    avro::ValidSchema vs(a0);
    std::ostringstream actual;
    vs.toJson(actual);

    std::string expected = "{\n\
    \"type\": \"map\",\n\
    \"values\": {\n\
        \"type\": \"map\",\n\
        \"values\": \"null\"\n\
    }\n\
}\n";
    BOOST_CHECK_EQUAL(expected, actual.str());
}

boost::unit_test::test_suite *
init_unit_test_suite(int /*argc*/, char * /*argv*/[]) {
    using namespace boost::unit_test;

    auto *test = BOOST_TEST_SUITE("Avro C++ unit test suite");

    test->add(BOOST_CLASS_TEST_CASE(&TestEncoding::test,
                                    boost::make_shared<TestEncoding>()));
    test->add(BOOST_CLASS_TEST_CASE(&TestSchema::test,
                                    boost::make_shared<TestSchema>()));
    test->add(BOOST_CLASS_TEST_CASE(&TestNested::test,
                                    boost::make_shared<TestNested>()));
    test->add(BOOST_CLASS_TEST_CASE(&TestGenerated::test,
                                    boost::make_shared<TestGenerated>()));
    test->add(BOOST_CLASS_TEST_CASE(&TestBadStuff::test,
                                    boost::make_shared<TestBadStuff>()));
    test->add(BOOST_CLASS_TEST_CASE(&TestResolution::test,
                                    boost::make_shared<TestResolution>()));
    test->add(BOOST_TEST_CASE(&testNestedArraySchema));
    test->add(BOOST_TEST_CASE(&testNestedMapSchema));

    return test;
}

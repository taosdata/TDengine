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
#include "GenericDatum.hh"
#include "ValidSchema.hh"

#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/parameterized_test.hpp>
#include <boost/test/unit_test.hpp>

namespace avro {
namespace schema {

const char *basicSchemas[] = {
    "\"null\"",
    "\"boolean\"",
    "\"int\"",
    "\"long\"",
    "\"float\"",
    "\"double\"",
    "\"bytes\"",
    "\"string\"",

    // Primitive types - longer
    R"({ "type": "null" })",
    R"({ "type": "boolean" })",
    R"({ "type": "int" })",
    R"({ "type": "long" })",
    R"({ "type": "float" })",
    R"({ "type": "double" })",
    R"({ "type": "bytes" })",
    R"({ "type": "string" })",

    // Record
    R"({"type":"record","name":"Test","doc":"Doc_string","fields":[]})",
    "{\"type\":\"record\",\"name\":\"Test\",\"fields\":"
    "[{\"name\":\"f\",\"type\":\"long\"}]}",
    "{\"type\":\"record\",\"name\":\"Test\",\"fields\":"
    "[{\"name\":\"f1\",\"type\":\"long\",\"doc\":\"field_doc\"},"
    "{\"name\":\"f2\",\"type\":\"int\"}]}",
    "{\"type\":\"error\",\"name\":\"Test\",\"fields\":"
    "[{\"name\":\"f1\",\"type\":\"long\"},"
    "{\"name\":\"f2\",\"type\":\"int\"}]}",

    // Recursive.
    "{\"type\":\"record\",\"name\":\"LongList\","
    "\"fields\":[{\"name\":\"value\",\"type\":\"long\",\"doc\":\"recursive_doc\"},"
    "{\"name\":\"next\",\"type\":[\"LongList\",\"null\"]}]}",
    // Enum
    R"({"type":"enum","doc":"enum_doc","name":"Test","symbols":["A","B"]})",

    // Array
    R"({"type":"array","doc":"array_doc","items":"long"})",
    "{\"type\":\"array\",\"items\":{\"type\":\"enum\","
    "\"name\":\"Test\",\"symbols\":[\"A\",\"B\"]}}",

    // Map
    R"({"type":"map","doc":"map_doc","values":"long"})",
    "{\"type\":\"map\",\"values\":{\"type\":\"enum\", "
    "\"name\":\"Test\",\"symbols\":[\"A\",\"B\"]}}",

    // Union
    R"(["string","null","long"])",

    // Fixed
    R"({"type":"fixed","doc":"fixed_doc","name":"Test","size":1})",
    "{\"type\":\"fixed\",\"name\":\"MyFixed\","
    "\"namespace\":\"org.apache.hadoop.avro\",\"size\":1}",
    R"({"type":"fixed","name":"Test","size":1})",
    R"({"type":"fixed","name":"Test","size":1})",

    // Extra attributes (should be ignored)
    R"({"type": "null", "extra attribute": "should be ignored"})",
    R"({"type": "boolean", "extra1": 1, "extra2": 2, "extra3": 3})",
    "{\"type\": \"record\",\"name\": \"Test\",\"fields\": "
    "[{\"name\": \"f\",\"type\": \"long\"}], \"extra attribute\": 1}",
    "{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"],"
    "\"extra attribute\": 1}",
    R"({"type": "array", "items": "long", "extra attribute": 1})",
    R"({"type": "map", "values": "long", "extra attribute": 1})",
    R"({"type": "fixed", "name": "Test", "size": 1, "extra attribute": 1})",

    // defaults
    // default double -  long
    R"({ "name":"test", "type": "record", "fields": [ {"name": "double","type": "double","default" : 2 }]})",
    // default double - double
    R"({ "name":"test", "type": "record", "fields": [ {"name": "double","type": "double","default" : 1.2 }]})",

    // namespace with '$' in it.
    "{\"type\":\"record\",\"name\":\"Test\",\"namespace\":\"a.b$\",\"fields\":"
    "[{\"name\":\"f\",\"type\":\"long\"}]}",
};

const char *basicSchemaErrors[] = {
    // Record
    // No fields
    R"({"type":"record","name":"LongList"})",
    // Fields not an array
    R"({"type":"record","name":"LongList", "fields": "hi"})",

    // Undefined name
    "{\"type\":\"record\",\"name\":\"LongList\","
    "\"fields\":[{\"name\":\"value\",\"type\":\"long\"},"
    "{\"name\":\"next\",\"type\":[\"LongListA\",\"null\"]}]}",

    // Enum
    // Symbols not an array
    "{\"type\": \"enum\", \"name\": \"Status\", \"symbols\": "
    "\"Normal Caution Critical\"}",
    // Name not a string
    "{\"type\": \"enum\", \"name\": [ 0, 1, 1, 2, 3, 5, 8 ], "
    "\"symbols\": [\"Golden\", \"Mean\"]}",
    // No name
    "{\"type\": \"enum\", \"symbols\" : [\"I\", \"will\", "
    "\"fail\", \"no\", \"name\"]}",
    // Duplicate symbol
    "{\"type\": \"enum\", \"name\": \"Test\","
    "\"symbols\" : [\"AA\", \"AA\"]}",

    // Union
    // Duplicate type
    R"(["string", "long", "long"])",
    // Duplicate type
    "[{\"type\": \"array\", \"items\": \"long\"}, "
    "{\"type\": \"array\", \"items\": \"string\"}]",

    // Fixed
    // No size
    R"({"type": "fixed", "name": "Missing size"})",
    // No name
    R"({"type": "fixed", "size": 314})",

    // defaults
    // default double - null
    R"({ "name":"test", "type": "record", "fields": [ {"name": "double","type": "double","default" : null }]})",
    // default double - string
    R"({ "name":"test", "type": "record", "fields": [ {"name": "double","type": "double","default" : "string" }]})"

};

const char *roundTripSchemas[] = {
    "\"null\"",
    "\"boolean\"",
    "\"int\"",
    "\"long\"",
    "\"float\"",
    "\"double\"",
    "\"bytes\"",
    "\"string\"",
    // Record
    R"({"type":"record","name":"Test","fields":[]})",
    "{\"type\":\"record\",\"name\":\"Test\",\"fields\":"
    "[{\"name\":\"f\",\"type\":\"long\"}]}",
    "{\"type\":\"record\",\"name\":\"Test\",\"fields\":"
    "[{\"name\":\"f1\",\"type\":\"long\"},"
    "{\"name\":\"f2\",\"type\":\"int\"}]}",
    /* Avro-C++ cannot do a round-trip on error schemas.
 * "{\"type\":\"error\",\"name\":\"Test\",\"fields\":"
 *       "[{\"name\":\"f1\",\"type\":\"long\"},"
 *       "{\"name\":\"f2\",\"type\":\"int\"}]}"
 */
    // Recursive.
    "{\"type\":\"record\",\"name\":\"LongList\","
    "\"fields\":[{\"name\":\"value\",\"type\":\"long\"},"
    "{\"name\":\"next\",\"type\":[\"LongList\",\"null\"]}]}",
    // Enum
    R"({"type":"enum","name":"Test","symbols":["A","B"]})",

    // Array
    R"({"type":"array","items":"long"})",
    "{\"type\":\"array\",\"items\":{\"type\":\"enum\","
    "\"name\":\"Test\",\"symbols\":[\"A\",\"B\"]}}",

    // Map
    R"({"type":"map","values":"long"})",
    "{\"type\":\"map\",\"values\":{\"type\":\"enum\","
    "\"name\":\"Test\",\"symbols\":[\"A\",\"B\"]}}",

    // Union
    R"(["string","null","long"])",

    // Fixed
    R"({"type":"fixed","name":"Test","size":1})",
    "{\"type\":\"fixed\",\"namespace\":\"org.apache.hadoop.avro\","
    "\"name\":\"MyFixed\",\"size\":1}",
    R"({"type":"fixed","name":"Test","size":1})",
    R"({"type":"fixed","name":"Test","size":1})",

    // Logical types
    R"({"type":"bytes","logicalType":"decimal","precision":12,"scale":6})",
    R"({"type":"fixed","name":"test","size":16,"logicalType":"decimal","precision":38,"scale":9})",
    R"({"type":"fixed","name":"test","size":129,"logicalType":"decimal","precision":310,"scale":155})",
    R"({"type":"int","logicalType":"date"})",
    R"({"type":"int","logicalType":"time-millis"})",
    R"({"type":"long","logicalType":"time-micros"})",
    R"({"type":"long","logicalType":"timestamp-millis"})",
    R"({"type":"long","logicalType":"timestamp-micros"})",
    R"({"type":"fixed","name":"test","size":12,"logicalType":"duration"})",
    R"({"type":"string","logicalType":"uuid"})",

    // namespace with '$' in it.
    "{\"type\":\"record\",\"namespace\":\"a.b$\",\"name\":\"Test\",\"fields\":"
    "[{\"name\":\"f\",\"type\":\"long\"}]}",
};

const char *malformedLogicalTypes[] = {
    // Wrong base type.
    R"({"type":"long","logicalType": "decimal","precision": 10})",
    R"({"type":"string","logicalType":"date"})",
    R"({"type":"string","logicalType":"time-millis"})",
    R"({"type":"string","logicalType":"time-micros"})",
    R"({"type":"string","logicalType":"timestamp-millis"})",
    R"({"type":"string","logicalType":"timestamp-micros"})",
    R"({"type":"string","logicalType":"duration"})",
    R"({"type":"long","logicalType":"uuid"})",
    // Missing the required field 'precision'.
    R"({"type":"bytes","logicalType":"decimal"})",
    // The claimed precision is not supported by the size of the fixed type.
    R"({"type":"fixed","logicalType":"decimal","size":4,"name":"a","precision":20})",
    R"({"type":"fixed","logicalType":"decimal","size":129,"name":"a","precision":311})",
    // Scale is larger than precision.
    R"({"type":"bytes","logicalType":"decimal","precision":5,"scale":10})"};
const char *schemasToCompact[] = {
    // Schema without any whitespace
    R"({"type":"record","name":"Test","fields":[]})",

    // Schema with whitespaces outside of field names/values only.
    "{\"type\":   \"record\",\n   \n\"name\":\"Test\", \t\t\"fields\":[]}\n \n",

    // Schema with whitespaces both inside and outside of field names/values.
    "{\"type\":   \"record\",  \"name\":               \"ComplexInteger\"\n, "
    "\"doc\": \"record_doc °C \u00f8 \x1f \\n \n \t\", "
    "\"fields\": ["
    "{\"name\":   \"re1\", \"type\":               \"long\", "
    "\"doc\":   \"A \\\"quoted doc\\\"\"      },                 "
    "{\"name\":  \"re2\", \"type\":   \"long\", \n\t"
    "\"doc\": \"extra slashes\\\\\\\\\"}"
    "]}"};

const char *compactSchemas[] = {
    R"({"type":"record","name":"Test","fields":[]})",
    R"({"type":"record","name":"Test","fields":[]})",
    "{\"type\":\"record\",\"name\":\"ComplexInteger\","
    "\"doc\":\"record_doc °C \u00f8 \\u001f \\n \\n \\t\","
    "\"fields\":["
    "{\"name\":\"re1\",\"type\":\"long\",\"doc\":\"A \\\"quoted doc\\\"\"},"
    "{\"name\":\"re2\",\"type\":\"long\",\"doc\":\"extra slashes\\\\\\\\\"}"
    "]}"};

void testTypes() {
    BOOST_CHECK_EQUAL(isAvroType(AVRO_BOOL), true);
}

static void testBasic(const char *schema) {
    BOOST_TEST_CHECKPOINT(schema);
    compileJsonSchemaFromString(schema);
}

static void testBasic_fail(const char *schema) {
    BOOST_TEST_CHECKPOINT(schema);
    BOOST_CHECK_THROW(compileJsonSchemaFromString(schema), Exception);
}

static void testCompile(const char *schema) {
    BOOST_TEST_CHECKPOINT(schema);
    compileJsonSchemaFromString(std::string(schema));
}

// Test that the JSON output from a valid schema matches the JSON that was
// used to construct it, apart from whitespace changes.
static void testRoundTrip(const char *schema) {
    BOOST_TEST_CHECKPOINT(schema);
    avro::ValidSchema compiledSchema =
        compileJsonSchemaFromString(std::string(schema));
    std::ostringstream os;
    compiledSchema.toJson(os);
    std::string result = os.str();
    result.erase(std::remove_if(result.begin(), result.end(), ::isspace), result.end()); // Remove whitespace
    BOOST_CHECK(result == std::string(schema));
    // Verify that the compact schema from toJson has the same content as the
    // schema.
    std::string result2 = compiledSchema.toJson(false);
    BOOST_CHECK(result2 == std::string(schema));
}

static void testCompactSchemas() {
    for (size_t i = 0; i < sizeof(schemasToCompact) / sizeof(schemasToCompact[0]); i++) {
        const char *schema = schemasToCompact[i];
        BOOST_TEST_CHECKPOINT(schema);
        avro::ValidSchema compiledSchema =
            compileJsonSchemaFromString(std::string(schema));

        std::string result = compiledSchema.toJson(false);
        BOOST_CHECK_EQUAL(result, compactSchemas[i]);
    }
}

static void testLogicalTypes() {
    const char *bytesDecimalType = "{\n\
        \"type\": \"bytes\",\n\
        \"logicalType\": \"decimal\",\n\
        \"precision\": 10,\n\
        \"scale\": 2\n\
    }";
    const char *fixedDecimalType = "{\n\
        \"type\": \"fixed\",\n\
        \"size\": 16,\n\
        \"name\": \"fixedDecimalType\",\n\
        \"logicalType\": \"decimal\",\n\
        \"precision\": 12,\n\
        \"scale\": 6\n\
    }";
    const char *dateType = "{\n\
        \"type\": \"int\", \"logicalType\": \"date\"\n\
    }";
    const char *timeMillisType = "{\n\
        \"type\": \"int\", \"logicalType\": \"time-millis\"\n\
    }";
    const char *timeMicrosType = "{\n\
        \"type\": \"long\", \"logicalType\": \"time-micros\"\n\
    }";
    const char *timestampMillisType = "{\n\
        \"type\": \"long\", \"logicalType\": \"timestamp-millis\"\n\
    }";
    const char *timestampMicrosType = "{\n\
        \"type\": \"long\", \"logicalType\": \"timestamp-micros\"\n\
    }";
    const char *durationType = "{\n\
        \"type\": \"fixed\",\n\
        \"size\": 12,\n\
        \"name\": \"durationType\",\n\
        \"logicalType\": \"duration\"\n\
    }";
    const char *uuidType = "{\n\
        \"type\": \"string\",\n\
        \"logicalType\": \"uuid\"\n\
    }";
    // AVRO-2923 Union with LogicalType
    const char* unionType = "[\n\
        {\"type\":\"string\", \"logicalType\":\"uuid\"},\"null\"\n\
    ]";
    {
        BOOST_TEST_CHECKPOINT(bytesDecimalType);
        ValidSchema schema1 = compileJsonSchemaFromString(bytesDecimalType);
        BOOST_CHECK(schema1.root()->type() == AVRO_BYTES);
        LogicalType logicalType = schema1.root()->logicalType();
        BOOST_CHECK(logicalType.type() == LogicalType::DECIMAL);
        BOOST_CHECK(logicalType.precision() == 10);
        BOOST_CHECK(logicalType.scale() == 2);

        BOOST_TEST_CHECKPOINT(fixedDecimalType);
        ValidSchema schema2 = compileJsonSchemaFromString(fixedDecimalType);
        BOOST_CHECK(schema2.root()->type() == AVRO_FIXED);
        logicalType = schema2.root()->logicalType();
        BOOST_CHECK(logicalType.type() == LogicalType::DECIMAL);
        BOOST_CHECK(logicalType.precision() == 12);
        BOOST_CHECK(logicalType.scale() == 6);

        GenericDatum bytesDatum(schema1);
        BOOST_CHECK(bytesDatum.logicalType().type() == LogicalType::DECIMAL);
        GenericDatum fixedDatum(schema2);
        BOOST_CHECK(fixedDatum.logicalType().type() == LogicalType::DECIMAL);
    }
    {
        BOOST_TEST_CHECKPOINT(dateType);
        ValidSchema schema = compileJsonSchemaFromString(dateType);
        BOOST_CHECK(schema.root()->type() == AVRO_INT);
        BOOST_CHECK(schema.root()->logicalType().type() == LogicalType::DATE);
        GenericDatum datum(schema);
        BOOST_CHECK(datum.logicalType().type() == LogicalType::DATE);
    }
    {
        BOOST_TEST_CHECKPOINT(timeMillisType);
        ValidSchema schema = compileJsonSchemaFromString(timeMillisType);
        BOOST_CHECK(schema.root()->type() == AVRO_INT);
        LogicalType logicalType = schema.root()->logicalType();
        BOOST_CHECK(logicalType.type() == LogicalType::TIME_MILLIS);
        GenericDatum datum(schema);
        BOOST_CHECK(datum.logicalType().type() == LogicalType::TIME_MILLIS);
    }
    {
        BOOST_TEST_CHECKPOINT(timeMicrosType);
        ValidSchema schema = compileJsonSchemaFromString(timeMicrosType);
        BOOST_CHECK(schema.root()->type() == AVRO_LONG);
        LogicalType logicalType = schema.root()->logicalType();
        BOOST_CHECK(logicalType.type() == LogicalType::TIME_MICROS);
        GenericDatum datum(schema);
        BOOST_CHECK(datum.logicalType().type() == LogicalType::TIME_MICROS);
    }
    {
        BOOST_TEST_CHECKPOINT(timestampMillisType);
        ValidSchema schema = compileJsonSchemaFromString(timestampMillisType);
        BOOST_CHECK(schema.root()->type() == AVRO_LONG);
        LogicalType logicalType = schema.root()->logicalType();
        BOOST_CHECK(logicalType.type() == LogicalType::TIMESTAMP_MILLIS);
        GenericDatum datum(schema);
        BOOST_CHECK(datum.logicalType().type() == LogicalType::TIMESTAMP_MILLIS);
    }
    {
        BOOST_TEST_CHECKPOINT(timestampMicrosType);
        ValidSchema schema = compileJsonSchemaFromString(timestampMicrosType);
        BOOST_CHECK(schema.root()->type() == AVRO_LONG);
        LogicalType logicalType = schema.root()->logicalType();
        BOOST_CHECK(logicalType.type() == LogicalType::TIMESTAMP_MICROS);
        GenericDatum datum(schema);
        BOOST_CHECK(datum.logicalType().type() == LogicalType::TIMESTAMP_MICROS);
    }
    {
        BOOST_TEST_CHECKPOINT(durationType);
        ValidSchema schema = compileJsonSchemaFromString(durationType);
        BOOST_CHECK(schema.root()->type() == AVRO_FIXED);
        BOOST_CHECK(schema.root()->fixedSize() == 12);
        LogicalType logicalType = schema.root()->logicalType();
        BOOST_CHECK(logicalType.type() == LogicalType::DURATION);
        GenericDatum datum(schema);
        BOOST_CHECK(datum.logicalType().type() == LogicalType::DURATION);
    }
    {
        BOOST_TEST_CHECKPOINT(uuidType);
        ValidSchema schema = compileJsonSchemaFromString(uuidType);
        BOOST_CHECK(schema.root()->type() == AVRO_STRING);
        LogicalType logicalType = schema.root()->logicalType();
        BOOST_CHECK(logicalType.type() == LogicalType::UUID);
        GenericDatum datum(schema);
        BOOST_CHECK(datum.logicalType().type() == LogicalType::UUID);
    }
    {
        BOOST_TEST_CHECKPOINT(unionType);
        ValidSchema schema = compileJsonSchemaFromString(unionType);
        BOOST_CHECK(schema.root()->type() == AVRO_UNION);
        LogicalType logicalType = schema.root()->logicalType();
        BOOST_CHECK(logicalType.type() == LogicalType::NONE);
        GenericDatum datum(schema);
        BOOST_CHECK(datum.logicalType().type() == LogicalType::UUID);
    }
}

static void testMalformedLogicalTypes(const char *schema) {
    BOOST_TEST_CHECKPOINT(schema);
    ValidSchema parsedSchema = compileJsonSchemaFromString(schema);
    LogicalType logicalType = parsedSchema.root()->logicalType();
    BOOST_CHECK(logicalType.type() == LogicalType::NONE);
    GenericDatum datum(parsedSchema);
    BOOST_CHECK(datum.logicalType().type() == LogicalType::NONE);
}

} // namespace schema
} // namespace avro

#define ENDOF(x) (x + sizeof(x) / sizeof(x[0]))

#define ADD_PARAM_TEST(ts, func, data) \
    ts->add(BOOST_PARAM_TEST_CASE(&func, data, ENDOF(data)))

boost::unit_test::test_suite *
init_unit_test_suite(int /*argc*/, char * /*argv*/[]) {
    using namespace boost::unit_test;

    auto *ts = BOOST_TEST_SUITE("Avro C++ unit tests for schemas");
    ts->add(BOOST_TEST_CASE(&avro::schema::testTypes));
    ADD_PARAM_TEST(ts, avro::schema::testBasic, avro::schema::basicSchemas);
    ADD_PARAM_TEST(ts, avro::schema::testBasic_fail,
                   avro::schema::basicSchemaErrors);
    ADD_PARAM_TEST(ts, avro::schema::testCompile, avro::schema::basicSchemas);
    ADD_PARAM_TEST(ts, avro::schema::testRoundTrip, avro::schema::roundTripSchemas);
    ts->add(BOOST_TEST_CASE(&avro::schema::testLogicalTypes));
    ADD_PARAM_TEST(ts, avro::schema::testMalformedLogicalTypes,
                   avro::schema::malformedLogicalTypes);
    ts->add(BOOST_TEST_CASE(&avro::schema::testCompactSchemas));
    return ts;
}

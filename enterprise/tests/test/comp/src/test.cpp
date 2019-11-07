#include <iostream>
#include <ctime>
#include <iomanip>
#include <random>
#include <fstream>
#include <iterator>
#include <vector>
#include <cstdlib>
#include <string>
#include <regex>
#include <string.h>

#include "tsdb.h"
#include "tsCompression.h"
#include "TestTimestamp.h"
#include "TestBool.h"
#include "TestString.h"
#include "TestDouble.h"
#include "TestFloat.h"
#include "TestInt.h"
#include "TestInt.cpp"

void getHelp();
void genBool();
void genTinyint();
void genSmallint();
void genInt();
void genBigint();
void genFloat();
void genDouble();
void genTimestamp();

int main(int argc, char *argv[]) {
    // Deal with options.
    if (argc == 1) {
        getHelp();
        exit(1);
    }
    bool isfileset = false;
    std::string fname;
    bool isgen = false;
    bool istypeset = false;
    std::string type;
    for (int i = 1; i < argc; i++){
        if (strcmp(argv[i], "-h") == 0) {
            getHelp();
            exit(0);
        }
        else if (strcmp(argv[i], "-f") == 0) {
            isfileset = true;
            if (i+1 < argc) {
                fname = argv[++i];
            }
            else{
                std::cout << "Filename is required after -f" << std::endl;
                exit(1);
            }
        }
        else if (strcmp(argv[i], "-g") == 0) {
            isgen = true;
        }
        else if (strcmp(argv[i], "-t") == 0) {
            istypeset = true;
            if (i+1 < argc) {
                type = argv[++i];
                auto reg = std::regex("(\\bbool\\b)|(\\btinyint\\b)|(\\bsmallint\\b)|(\\bint\\b)|(\\bbigint\\b)|(\\bfloat\\b)|(\\bdouble\\b)|(\\btimestamp\\b)");
                if (not std::regex_match(type, reg)) {
                    std::cout << "Wrong type assign. Type -h for help." << std::endl;
                    exit(1);
                }
            }
            else{
                std::cout << "Type name is required." << std::endl;
                exit(1);
            }
        }
        else {
            std::cout << "Wrong args. Type -h for help." << std::endl;
            exit(1);
        }
    }

    if (isgen) {
        // To generate data.
        if (istypeset) {
            if (type == "bool") {
                genBool();
            }
            else if (type == "tinyint"){
                genTinyint();
            }
            else if (type == "smallint"){
                genSmallint();
            }
            else if (type == "int"){
                genInt();
            }
            else if (type == "bigint"){
                genBigint();
            }
            else if (type == "float"){
                genFloat();
            }
            else if (type == "double"){
                genDouble();
            }
            else if (type == "timestamp"){
                genTimestamp();
            }
        }
        else {
            genBool();
            genTinyint();
            genSmallint();
            genInt();
            genBigint();
            genFloat();
            genDouble();
            genTimestamp();
        }
    }
    else {
        // To test the compression data.
        if (not isfileset) {
            std::cerr << "Please assign a file to test." << std::endl;
            exit(1);
        }

        if (istypeset) {
            if (type == "bool") {
                TestBool test(fname);
                test.test(10, true);
                // std::cout << "Compressiont rate: ";
                // std::cout << std::fixed << std::setprecision(3);
                // std::cout << test.test() << std::endl;
            }
            else if (type == "tinyint"){
                TestInt<char> test(fname);
                test.test(10, true);
                // std::cout << "Compressiont rate: ";
                // std::cout << std::fixed << std::setprecision(3);
                // std::cout << test.test() << std::endl;
            }
            else if (type == "smallint"){
                TestInt<short> test(fname);
                test.test(10, true);
                // std::cout << "Compressiont rate: ";
                // std::cout << std::fixed << std::setprecision(3);
                // std::cout << test.test() << std::endl;
            }
            else if (type == "int"){
                TestInt<int> test(fname);
                test.test(10, true);
                // std::cout << "Compressiont rate: ";
                // std::cout << std::fixed << std::setprecision(3);
                // std::cout << test.test() << std::endl;
            }
            else if (type == "bigint"){
                TestInt<long> test(fname);
                test.test(10, true);
                // std::cout << "Compressiont rate: ";
                // std::cout << std::fixed << std::setprecision(3);
                // std::cout << test.test() << std::endl;
            }
            else if (type == "float"){
                TestFloat test(fname);
                test.test(10, true);
                // std::cout << "Compressiont rate: ";
                // std::cout << std::fixed << std::setprecision(3);
                // std::cout << test.test() << std::endl;
            }
            else if (type == "double"){
                TestDouble test(fname);
                test.test(10, true);
                // std::cout << "Compressiont rate: ";
                // std::cout << std::fixed << std::setprecision(3);
                // std::cout << test.test() << std::endl;
            }
            else if (type == "timestamp"){
                TestTimestamp test(fname);
                test.test(10, true);
                // std::cout << "Compressiont rate: ";
                // std::cout << std::fixed << std::setprecision(3);
                // std::cout << test.test() << std::endl;
            }
        }
        else {
            std::cerr << "Please assign the type of the file." << std::endl;
            exit(1);
        }
    }
}

void getHelp() {
    std::cout << "=====================================" << std::endl;
    std::cout << "      -f : assign the input file name" << std::endl;
    std::cout << "      -h : get help" << std::endl;
    std::cout << "      -g : generate data" << std::endl;
    std::cout << "      -t : assign the data type" << std::endl;
    std::cout << "           arg       |  type" << std::endl;
    std::cout << "           =================" << std::endl;
    std::cout << "           bool      |  bool  " << std::endl;
    std::cout << "           tinyint   |  INT8  " << std::endl;
    std::cout << "           smallint  |  INT16 " << std::endl;
    std::cout << "           int       |  INT32 " << std::endl;
    std::cout << "           bigint    |  INT64 " << std::endl;
    std::cout << "           float     |  float " << std::endl;
    std::cout << "           double    |  double" << std::endl;
    std::cout << "           timestamp |  timestamp" << std::endl;
}

void genBool() {
    TestBool test_bool1(0.5, 5000);
    test_bool1.generateData();
    test_bool1.writeData("bool.bin1");

    TestBool test_bool2(0.6, 5000);
    test_bool2.generateData();
    test_bool2.writeData("bool.bin2");

    TestBool test_bool3(0.7, 5000);
    test_bool3.generateData();
    test_bool3.writeData("bool.bin3");

    TestBool test_bool4(0.8, 5000);
    test_bool4.generateData();
    test_bool4.writeData("bool.bin4");

    TestBool test_bool5(0.9, 5000);
    test_bool5.generateData();
    test_bool5.writeData("bool.bin5");
}
void genTinyint() {
    TestInt<char> test_tinyint1(0, 10, 0, 5, 0, 5000);
    test_tinyint1.generateData();
    test_tinyint1.writeData("tinyint.bin1");

    TestInt<char> test_tinyint2(0, 10, 0, 1, 0, 5000);
    test_tinyint2.generateData();
    test_tinyint2.writeData("tinyint.bin2");

    TestInt<char> test_tinyint3(0, 10, 0, 10, 0, 5000);
    test_tinyint3.generateData();
    test_tinyint3.writeData("tinyint.bin3");

    TestInt<char> test_tinyint4(0, 10, 0, 20, 0, 5000);
    test_tinyint4.generateData();
    test_tinyint4.writeData("tinyint.bin4");

    TestInt<char> test_tinyint5(0, 10, 0, 50, 0, 5000);
    test_tinyint5.generateData();
    test_tinyint5.writeData("tinyint.bin5");

}
void genSmallint() {
    TestInt<short> test_smallint1(3, 0, 0,  2, 0, 5000);
    test_smallint1.generateData();
    test_smallint1.writeData("smallint.bin1");

    TestInt<short> test_smallint2(5, 0, 0, 2, 0, 5000);
    test_smallint2.generateData();
    test_smallint2.writeData("smallint.bin2");

    TestInt<short> test_smallint3(3, 0, 0, 6, 0, 5000);
    test_smallint3.generateData();
    test_smallint3.writeData("smallint.bin3");

    TestInt<short> test_smallint4(5, 0, 0, 3, 0, 5000);
    test_smallint4.generateData();
    test_smallint4.writeData("smallint.bin4");

    TestInt<short> test_smallint5(5, 0, 0, 10, 0, 5000);
    test_smallint5.generateData();
    test_smallint5.writeData("smallint.bin5");
}
void genInt() {
    TestInt<int> test_int1(10, 0, 0,  1, 0, 5000);
    test_int1.generateData();
    test_int1.writeData("int.bin1");

    TestInt<int> test_int2(10, 0, 0,  5, 0, 5000);
    test_int2.generateData();
    test_int2.writeData("int.bin2");

    TestInt<int> test_int3(10, 0, 0,  3, 0, 5000);
    test_int3.generateData();
    test_int3.writeData("int.bin3");

    TestInt<int> test_int4(10, 0, 0,  6, 0, 5000);
    test_int4.generateData();
    test_int4.writeData("int.bin4");

    TestInt<int> test_int5(20, 0, 0,  8, 0, 5000);
    test_int5.generateData();
    test_int5.writeData("int.bin5");
}
void genBigint() {
    TestInt<long> test_bigint1(100, 1000000, 0,  10, 0, 5000);
    test_bigint1.generateData();
    test_bigint1.writeData("bigint.bin1");

    TestInt<long> test_bigint2(100, 1000000, 0,  20, 0, 5000);
    test_bigint2.generateData();
    test_bigint2.writeData("bigint.bin2");

    TestInt<long> test_bigint3(100, 1000000, 0,  4, 0, 5000);
    test_bigint3.generateData();
    test_bigint3.writeData("bigint.bin3");

    TestInt<long> test_bigint4(1000, 100000, 0,  0, 0, 5000);
    test_bigint4.generateData();
    test_bigint4.writeData("bigint.bin4");

    TestInt<long> test_bigint5(20, 0, 0,  8, 0, 5000);
    test_bigint5.generateData();
    test_bigint5.writeData("bigint.bin5");
}
void genFloat() {
    TestFloat test_float1(50, 1000, 100, 0, 0, 0, 5000);
    test_float1.generateData();
    test_float1.writeData("float.bin1");

    TestFloat test_float2(50, 1000, 100, 0, 5, 0, 5000);
    test_float2.generateData();
    test_float2.writeData("float.bin2");

    TestFloat test_float3(50, 1000, 100, 0, 0, 1, 5000);
    test_float3.generateData();
    test_float3.writeData("float.bin3");

    TestFloat test_float4(50, 1000, 100, 0, 5, 1, 5000);
    test_float4.generateData();
    test_float4.writeData("float.bin4");

    TestFloat test_float5(0, 0, 10, 5000);
    test_float5.generateData();
    test_float5.writeData("float.bin5");
}
void genDouble() {
    TestDouble test_double1(50, 1000, 100, 0, 0, 0, 5000);
    test_double1.generateData();
    test_double1.writeData("double.bin1");

    TestDouble test_double2(50, 1000, 100, 0, 5, 0, 5000);
    test_double2.generateData();
    test_double2.writeData("double.bin2");

    TestDouble test_double3(50, 1000, 100, 0, 0, 1, 5000);
    test_double3.generateData();
    test_double3.writeData("double.bin3");

    TestDouble test_double4(50, 1000, 100, 0, 5, 1, 5000);
    test_double4.generateData();
    test_double4.writeData("double.bin4");

    TestDouble test_double5(0, 0, 10, 5000);
    test_double5.generateData();
    test_double5.writeData("double.bin5");
}

void genTimestamp() {
    TestTimestamp test_timestamp1(5, 0, 1, 5000, 0.01);
    test_timestamp1.generateData();
    test_timestamp1.writeData("timestamp.bin1");

    TestTimestamp test_timestamp2(60, 0, 5, 5000, 0.01);
    test_timestamp2.generateData();
    test_timestamp2.writeData("timestamp.bin2");

    TestTimestamp test_timestamp3(300, 0, 60, 5000, 0.01);
    test_timestamp3.generateData();
    test_timestamp3.writeData("timestamp.bin3");

    TestTimestamp test_timestamp4(5, 0, 1, 5000, 0.05);
    test_timestamp4.generateData();
    test_timestamp4.writeData("timestamp.bin4");

    TestTimestamp test_timestamp5(600, 0, 30, 5000, 0.01);
    test_timestamp5.generateData();
    test_timestamp5.writeData("timestamp.bin5");
}

// -f : assign the input file name.
// -t : assign the data type
//      bool   :  bool
//      char   :  INT8
//      short  :  INT16
//      int    :  INT32
//      long   :  INT64
//      float  :  float
//      double :  double
// -g : generate data
// -h : get help information.

/* Bool type:
 * If p <= 0.935, use tsCompressBool
 * if p > 0.935, use tsCompressBoolRLE.
 */

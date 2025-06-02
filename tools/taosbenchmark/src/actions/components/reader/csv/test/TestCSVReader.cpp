#include <iostream>
#include <cassert>
#include "CSVReader.h"


void test_open_invalid_file() {
    try {
        CSVReader reader("invalid_file.csv", false, ',');
        assert(false && "Expected exception for invalid file path");
    } catch (const std::runtime_error& e) {
        std::cout << "test_open_invalid_file passed\n";
    }
}

void test_read_empty_file() {
    std::ofstream empty_file("empty.csv");
    empty_file.close();

    CSVReader reader("empty.csv", false, ',');
    auto rows = reader.read_all();
    assert(rows.empty() && "Expected no rows for empty file");
    std::cout << "test_read_empty_file passed\n";
}

void test_read_simple_file() {
    std::ofstream simple_file("simple.csv");
    simple_file << "name,age,city\n";
    simple_file << "Alice,30,New York\n";
    simple_file << "Bob,25,Los Angeles\n";
    simple_file.close();

    CSVReader reader("simple.csv", true, ',');
    auto rows = reader.read_all();
    assert(rows.size() == 2 && "Expected 2 rows");
    assert(rows[0][0] == "Alice" && rows[0][1] == "30" && rows[0][2] == "New York");
    assert(rows[1][0] == "Bob" && rows[1][1] == "25" && rows[1][2] == "Los Angeles");
    std::cout << "test_read_simple_file passed\n";
}

void test_parse_complex_line() {
    std::ofstream complex_file("complex.csv");
    complex_file << "\"field1\",\"field2,with,comma\",\"field3\"";
    complex_file.close();

    CSVReader reader("complex.csv", false, ',');
    auto fields = reader.read_next().value_or(CSVRow{});
    assert(fields.size() == 3 && "Expected 3 fields");
    assert(fields[0] == "field1");
    assert(fields[1] == "field2,with,comma");
    assert(fields[2] == "field3");
    std::cout << "test_parse_complex_line passed\n";
}

void test_skip_header() {
    std::ofstream file_with_header("header.csv");
    file_with_header << "header1,header2,header3\n";
    file_with_header << "data1,data2,data3\n";
    file_with_header.close();

    CSVReader reader("header.csv", true, ',');
    auto rows = reader.read_all();
    assert(rows.size() == 1 && "Expected 1 row after skipping header");
    assert(rows[0][0] == "data1" && rows[0][1] == "data2" && rows[0][2] == "data3");
    std::cout << "test_skip_header passed\n";
}

int main() {
    test_open_invalid_file();
    test_read_empty_file();
    test_read_simple_file();
    test_parse_complex_line();
    test_skip_header();

    std::cout << "All tests passed!\n";
    return 0;
}
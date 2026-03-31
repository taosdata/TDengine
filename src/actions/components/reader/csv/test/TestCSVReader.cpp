#include <iostream>
#include <fstream>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include "CSVReader.hpp"

namespace fs = std::filesystem;

void test_open_invalid_file() {
    try {
        CSVReader reader("invalid_file.csv", false, ',');
        assert(false && "Expected exception for invalid file path");
    } catch (const std::runtime_error&) {
        std::cout << "test_open_invalid_file passed\n";
    }
}

void test_read_empty_file() {
    std::ofstream empty_file("empty.csv");
    empty_file.close();

    // vincentlaucsb csv-parser throws on empty files
    try {
        CSVReader reader("empty.csv", false, ',');
        auto rows = reader.read_all();
        assert(rows.empty() && "Expected no rows for empty file");
    } catch (const std::runtime_error&) {
        // Expected: csv-parser cannot open empty files
    }
    std::remove("empty.csv");
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
    std::remove("simple.csv");
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
    std::remove("complex.csv");
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
    std::remove("header.csv");
    std::cout << "test_skip_header passed\n";
}

void test_column_count() {
    std::ofstream f("colcount.csv");
    f << "a,b,c\n1,2,3\n";
    f.close();

    CSVReader reader("colcount.csv", true, ',');
    assert(reader.column_count() == 3);
    std::remove("colcount.csv");
    std::cout << "test_column_count passed\n";
}

void test_reset() {
    std::ofstream f("reset.csv");
    f << "a,b\n1,2\n3,4\n";
    f.close();

    CSVReader reader("reset.csv", true, ',');
    auto rows1 = reader.read_all();
    assert(rows1.size() == 2);

    reader.reset();
    auto rows2 = reader.read_all();
    assert(rows2.size() == 2);
    assert(rows1[0][0] == rows2[0][0]);
    std::remove("reset.csv");
    std::cout << "test_reset passed\n";
}

void test_multi_file() {
    fs::create_directory("multi_csv_test");
    {
        std::ofstream f("multi_csv_test/a.csv");
        f << "x,y\n1,2\n3,4\n";
    }
    {
        std::ofstream f("multi_csv_test/b.csv");
        f << "x,y\n5,6\n7,8\n";
    }

    CSVReader reader(std::vector<std::string>{"multi_csv_test/a.csv", "multi_csv_test/b.csv"}, true, ',');
    auto rows = reader.read_all();
    assert(rows.size() == 4 && "Expected 4 rows from 2 files");
    assert(rows[0][0] == "1" && rows[0][1] == "2");
    assert(rows[1][0] == "3" && rows[1][1] == "4");
    assert(rows[2][0] == "5" && rows[2][1] == "6");
    assert(rows[3][0] == "7" && rows[3][1] == "8");
    assert(reader.column_count() == 2);

    fs::remove_all("multi_csv_test");
    std::cout << "test_multi_file passed\n";
}

void test_multi_file_no_header() {
    fs::create_directory("multi_csv_nh");
    {
        std::ofstream f("multi_csv_nh/a.csv");
        f << "1,2\n3,4\n";
    }
    {
        std::ofstream f("multi_csv_nh/b.csv");
        f << "5,6\n";
    }

    CSVReader reader(std::vector<std::string>{"multi_csv_nh/a.csv", "multi_csv_nh/b.csv"}, false, ',');
    auto rows = reader.read_all();
    assert(rows.size() == 3 && "Expected 3 rows");
    assert(rows[2][0] == "5" && rows[2][1] == "6");

    fs::remove_all("multi_csv_nh");
    std::cout << "test_multi_file_no_header passed\n";
}

void test_read_next_line_by_line() {
    std::ofstream f("lineby.csv");
    f << "a\n1\n2\n3\n";
    f.close();

    CSVReader reader("lineby.csv", true, ',');
    auto r1 = reader.read_next();
    assert(r1 && (*r1)[0] == "1");
    auto r2 = reader.read_next();
    assert(r2 && (*r2)[0] == "2");
    auto r3 = reader.read_next();
    assert(r3 && (*r3)[0] == "3");
    auto r4 = reader.read_next();
    assert(!r4);
    std::remove("lineby.csv");
    std::cout << "test_read_next_line_by_line passed\n";
}

// ============================================================
// Helpers (supplemental)
// ============================================================

static void create_test_file(const std::string& filename, const std::string& content) {
    std::ofstream f(filename);
    f << content;
    f.close();
}

static void remove_test_file(const std::string& filename) {
    std::remove(filename.c_str());
}


// ============================================================
// Supplemental tests
// ============================================================

void test_empty_file_paths_throws() {
    try {
        CSVReader reader(std::vector<std::string>{}, true, ',');
        assert(false && "Should throw for empty file paths");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("No CSV file paths provided") != std::string::npos);
    }
    std::cout << "test_empty_file_paths_throws passed\n";
}

void test_semicolon_delimiter() {
    const std::string filename = "csv_semi.csv";
    create_test_file(filename,
        "name;age;city\n"
        "Alice;30;New York\n"
        "Bob;25;LA\n");

    CSVReader reader(filename, true, ';');
    assert(reader.column_count() == 3);

    auto rows = reader.read_all();
    assert(rows.size() == 2);
    assert(rows[0][0] == "Alice");
    assert(rows[0][1] == "30");
    assert(rows[0][2] == "New York");
    assert(rows[1][0] == "Bob");

    remove_test_file(filename);
    std::cout << "test_semicolon_delimiter passed\n";
}

void test_pipe_delimiter() {
    const std::string filename = "csv_pipe.csv";
    create_test_file(filename,
        "a|b|c\n"
        "1|2|3\n");

    CSVReader reader(filename, true, '|');
    assert(reader.column_count() == 3);

    auto rows = reader.read_all();
    assert(rows.size() == 1);
    assert(rows[0][0] == "1");
    assert(rows[0][1] == "2");
    assert(rows[0][2] == "3");

    remove_test_file(filename);
    std::cout << "test_pipe_delimiter passed\n";
}

void test_no_header_column_count() {
    const std::string filename = "csv_noheader_cc.csv";
    create_test_file(filename,
        "10,20,30,40\n"
        "50,60,70,80\n");

    CSVReader reader(filename, false, ',');
    assert(reader.column_count() == 4 && "Should determine column count from first data row");

    auto rows = reader.read_all();
    assert(rows.size() == 2);
    assert(rows[0][0] == "10");
    assert(rows[0][3] == "40");
    assert(rows[1][0] == "50");

    remove_test_file(filename);
    std::cout << "test_no_header_column_count passed\n";
}

void test_no_header_single_row() {
    const std::string filename = "csv_noheader_1row.csv";
    create_test_file(filename, "aaa,bbb\n");

    CSVReader reader(filename, false, ',');
    assert(reader.column_count() == 2);

    auto rows = reader.read_all();
    assert(rows.size() == 1);
    assert(rows[0][0] == "aaa");
    assert(rows[0][1] == "bbb");

    remove_test_file(filename);
    std::cout << "test_no_header_single_row passed\n";
}

void test_multi_file_reset() {
    fs::create_directory("csv_multi_reset");
    create_test_file("csv_multi_reset/a.csv", "v\n1\n2\n");
    create_test_file("csv_multi_reset/b.csv", "v\n3\n4\n");

    CSVReader reader(std::vector<std::string>{"csv_multi_reset/a.csv", "csv_multi_reset/b.csv"}, true, ',');

    auto rows1 = reader.read_all();
    assert(rows1.size() == 4);

    reader.reset();

    auto rows2 = reader.read_all();
    assert(rows2.size() == 4);
    assert(rows1[0][0] == rows2[0][0]);
    assert(rows1[3][0] == rows2[3][0]);

    fs::remove_all("csv_multi_reset");
    std::cout << "test_multi_file_reset passed\n";
}

void test_multi_file_read_next_boundary() {
    fs::create_directory("csv_multi_boundary");
    create_test_file("csv_multi_boundary/a.csv", "col\nA\n");
    create_test_file("csv_multi_boundary/b.csv", "col\nB\n");
    create_test_file("csv_multi_boundary/c.csv", "col\nC\n");

    CSVReader reader(std::vector<std::string>{"csv_multi_boundary/a.csv", "csv_multi_boundary/b.csv", "csv_multi_boundary/c.csv"}, true, ',');

    auto r1 = reader.read_next();
    assert(r1.has_value() && (*r1)[0] == "A");

    auto r2 = reader.read_next();
    assert(r2.has_value() && (*r2)[0] == "B");

    auto r3 = reader.read_next();
    assert(r3.has_value() && (*r3)[0] == "C");

    auto r4 = reader.read_next();
    assert(!r4.has_value() && "All files exhausted");

    fs::remove_all("csv_multi_boundary");
    std::cout << "test_multi_file_read_next_boundary passed\n";
}

void test_whitespace_trimming() {
    const std::string filename = "csv_trim.csv";
    create_test_file(filename,
        "a,b,c\n"
        "  hello  , world ,foo\n");

    CSVReader reader(filename, true, ',');
    auto row = reader.read_next();
    assert(row.has_value());
    assert((*row)[0] == "hello" && "Should trim spaces");
    assert((*row)[1] == "world" && "Should trim spaces");
    assert((*row)[2] == "foo");

    remove_test_file(filename);
    std::cout << "test_whitespace_trimming passed\n";
}

void test_tab_trimming() {
    const std::string filename = "csv_tabtrim.csv";
    create_test_file(filename,
        "a,b\n"
        "\tval1\t,\tval2\t\n");

    CSVReader reader(filename, true, ',');
    auto row = reader.read_next();
    assert(row.has_value());
    assert((*row)[0] == "val1" && "Should trim tabs");
    assert((*row)[1] == "val2" && "Should trim tabs");

    remove_test_file(filename);
    std::cout << "test_tab_trimming passed\n";
}

void test_move_constructor() {
    const std::string filename = "csv_move.csv";
    create_test_file(filename,
        "x\n1\n2\n3\n");

    CSVReader reader1(filename, true, ',');
    auto r1 = reader1.read_next();
    assert(r1.has_value() && (*r1)[0] == "1");

    CSVReader reader2(std::move(reader1));

    auto r2 = reader2.read_next();
    assert(r2.has_value() && (*r2)[0] == "2");

    auto r3 = reader2.read_next();
    assert(r3.has_value() && (*r3)[0] == "3");

    auto r4 = reader2.read_next();
    assert(!r4.has_value());

    remove_test_file(filename);
    std::cout << "test_move_constructor passed\n";
}

void test_move_assignment() {
    const std::string filename1 = "csv_move_a.csv";
    const std::string filename2 = "csv_move_b.csv";
    create_test_file(filename1, "x\nAAA\n");
    create_test_file(filename2, "x\nBBB\n");

    CSVReader reader1(filename1, true, ',');
    CSVReader reader2(filename2, true, ',');

    reader2 = std::move(reader1);

    auto row = reader2.read_next();
    assert(row.has_value() && (*row)[0] == "AAA" && "Should now read from file1");

    remove_test_file(filename1);
    remove_test_file(filename2);
    std::cout << "test_move_assignment passed\n";
}

void test_read_all_after_partial_read() {
    const std::string filename = "csv_partial.csv";
    create_test_file(filename,
        "v\n1\n2\n3\n4\n");

    CSVReader reader(filename, true, ',');

    reader.read_next();
    reader.read_next();

    auto rows = reader.read_all();
    assert(rows.size() == 4 && "read_all should reset and read from start");
    assert(rows[0][0] == "1");
    assert(rows[3][0] == "4");

    remove_test_file(filename);
    std::cout << "test_read_all_after_partial_read passed\n";
}

void test_header_only_file() {
    const std::string filename = "csv_headeronly.csv";
    create_test_file(filename, "col1,col2,col3\n");

    CSVReader reader(filename, true, ',');
    assert(reader.column_count() == 3 && "Should detect 3 columns from header");

    auto rows = reader.read_all();
    assert(rows.empty() && "No data rows expected");

    auto r = reader.read_next();
    assert(!r.has_value());

    remove_test_file(filename);
    std::cout << "test_header_only_file passed\n";
}

void test_quoted_fields_with_newline() {
    const std::string filename = "csv_quoted_nl.csv";
    create_test_file(filename,
        "a,b\n"
        "\"hello\nworld\",normal\n");

    CSVReader reader(filename, true, ',');
    auto rows = reader.read_all();
    assert(!rows.empty() && "Should parse quoted field without crashing");
    bool found_hello = false;
    for (const auto& row : rows) {
        for (const auto& field : row) {
            if (field.find("hello") != std::string::npos) {
                found_hello = true;
            }
        }
    }
    (void)found_hello;
    assert(found_hello && "Should find 'hello' in parsed data");

    remove_test_file(filename);
    std::cout << "test_quoted_fields_with_newline passed\n";
}

void test_quoted_fields_with_escaped_quotes() {
    const std::string filename = "csv_escaped_q.csv";
    create_test_file(filename,
        "a,b\n"
        "\"say \"\"hello\"\"\",world\n");

    CSVReader reader(filename, true, ',');
    auto row = reader.read_next();
    assert(row.has_value());
    assert((*row)[0] == "say \"hello\"" && "Should handle escaped quotes (RFC 4180)");
    assert((*row)[1] == "world");

    remove_test_file(filename);
    std::cout << "test_quoted_fields_with_escaped_quotes passed\n";
}

void test_single_string_constructor() {
    const std::string filename = "csv_single_ctor.csv";
    create_test_file(filename,
        "a,b\n"
        "X,Y\n");

    CSVReader reader(filename, true, ',');
    assert(reader.column_count() == 2);

    auto rows = reader.read_all();
    assert(rows.size() == 1);
    assert(rows[0][0] == "X");
    assert(rows[0][1] == "Y");

    remove_test_file(filename);
    std::cout << "test_single_string_constructor passed\n";
}

void test_multi_file_no_header_seamless() {
    fs::create_directory("csv_multi_nh_s");
    create_test_file("csv_multi_nh_s/a.csv", "10,20\n30,40\n");
    create_test_file("csv_multi_nh_s/b.csv", "50,60\n70,80\n");

    CSVReader reader(std::vector<std::string>{"csv_multi_nh_s/a.csv", "csv_multi_nh_s/b.csv"}, false, ',');
    assert(reader.column_count() == 2);

    auto rows = reader.read_all();
    assert(rows.size() == 4);
    assert(rows[0][0] == "10");
    assert(rows[1][0] == "30");
    assert(rows[2][0] == "50");
    assert(rows[3][0] == "70");

    fs::remove_all("csv_multi_nh_s");
    std::cout << "test_multi_file_no_header_seamless passed\n";
}

void test_many_columns() {
    const std::string filename = "csv_many_cols.csv";
    {
        std::ofstream f(filename);
        for (int i = 0; i < 50; ++i) {
            if (i > 0) f << ",";
            f << "col" << i;
        }
        f << "\n";
        for (int i = 0; i < 50; ++i) {
            if (i > 0) f << ",";
            f << i * 10;
        }
        f << "\n";
        f.close();
    }

    CSVReader reader(filename, true, ',');
    assert(reader.column_count() == 50);

    auto rows = reader.read_all();
    assert(rows.size() == 1);
    assert(rows[0].size() == 50);
    assert(rows[0][0] == "0");
    assert(rows[0][49] == "490");

    remove_test_file(filename);
    std::cout << "test_many_columns passed\n";
}

void test_empty_fields() {
    const std::string filename = "csv_empty_fields.csv";
    create_test_file(filename,
        "a,b,c\n"
        ",hello,\n"
        ",,\n");

    CSVReader reader(filename, true, ',');
    auto rows = reader.read_all();
    assert(rows.size() == 2);
    assert(rows[0][0].empty() && "First field empty");
    assert(rows[0][1] == "hello");
    assert(rows[0][2].empty() && "Third field empty");
    assert(rows[1][0].empty());
    assert(rows[1][1].empty());
    assert(rows[1][2].empty());

    remove_test_file(filename);
    std::cout << "test_empty_fields passed\n";
}

void test_read_next_after_exhaustion() {
    const std::string filename = "csv_exhaust.csv";
    create_test_file(filename,
        "v\n1\n");

    CSVReader reader(filename, true, ',');

    auto r1 = reader.read_next();
    assert(r1.has_value());

    auto r2 = reader.read_next();
    assert(!r2.has_value());

    auto r3 = reader.read_next();
    assert(!r3.has_value());

    auto r4 = reader.read_next();
    assert(!r4.has_value());

    remove_test_file(filename);
    std::cout << "test_read_next_after_exhaustion passed\n";
}

int main() {
    test_open_invalid_file();
    test_read_empty_file();
    test_read_simple_file();
    test_parse_complex_line();
    test_skip_header();
    test_column_count();
    test_reset();
    test_multi_file();
    test_multi_file_no_header();
    test_read_next_line_by_line();

    // Supplemental tests
    test_empty_file_paths_throws();
    test_single_string_constructor();
    test_semicolon_delimiter();
    test_pipe_delimiter();
    test_no_header_column_count();
    test_no_header_single_row();
    test_multi_file_reset();
    test_multi_file_read_next_boundary();
    test_multi_file_no_header_seamless();
    test_whitespace_trimming();
    test_tab_trimming();
    test_move_constructor();
    test_move_assignment();
    test_read_all_after_partial_read();
    test_header_only_file();
    test_quoted_fields_with_newline();
    test_quoted_fields_with_escaped_quotes();
    test_many_columns();
    test_empty_fields();
    test_read_next_after_exhaustion();

    std::cout << "All tests passed!\n";
    return 0;
}
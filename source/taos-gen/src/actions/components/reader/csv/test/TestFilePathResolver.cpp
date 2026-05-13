#include "FilePathResolver.hpp"
#include <iostream>
#include <cassert>
#include <fstream>
#include "FilesystemCompat.hpp"


void create_file(const std::string& path, const std::string& content = "") {
    std::ofstream f(path);
    f << content;
    f.close();
}

void test_single_file() {
    create_file("resolver_test.csv", "a,b\n1,2\n");
    auto result = FilePathResolver::resolve("resolver_test.csv");
    assert(result.size() == 1);
    assert(result[0] == "resolver_test.csv");
    std::remove("resolver_test.csv");
    std::cout << "test_single_file passed\n";
}

void test_nonexistent_file() {
    try {
        FilePathResolver::resolve("nonexistent_file.csv");
        assert(false && "Expected exception");
    } catch (const std::runtime_error&) {}
    std::cout << "test_nonexistent_file passed\n";
}

void test_empty_path() {
    try {
        FilePathResolver::resolve("");
        assert(false && "Expected exception");
    } catch (const std::runtime_error&) {}
    std::cout << "test_empty_path passed\n";
}

void test_directory() {
    fs::create_directory("resolver_test_dir");
    create_file("resolver_test_dir/b.csv", "x\n1\n");
    create_file("resolver_test_dir/a.csv", "x\n2\n");
    create_file("resolver_test_dir/c.txt", "not csv");

    auto result = FilePathResolver::resolve("resolver_test_dir");
    assert(result.size() == 2);
    // Should be sorted by name
    assert(result[0].find("a.csv") != std::string::npos);
    assert(result[1].find("b.csv") != std::string::npos);

    fs::remove_all("resolver_test_dir");
    std::cout << "test_directory passed\n";
}

void test_directory_empty() {
    fs::create_directory("resolver_empty_dir");
    try {
        FilePathResolver::resolve("resolver_empty_dir");
        assert(false && "Expected exception for empty directory");
    } catch (const std::runtime_error&) {}
    fs::remove_all("resolver_empty_dir");
    std::cout << "test_directory_empty passed\n";
}

void test_glob_pattern() {
    fs::create_directory("resolver_glob_dir");
    create_file("resolver_glob_dir/data_01.csv", "x\n1\n");
    create_file("resolver_glob_dir/data_02.csv", "x\n2\n");
    create_file("resolver_glob_dir/other.csv", "x\n3\n");
    create_file("resolver_glob_dir/data_03.txt", "not csv");

    auto result = FilePathResolver::resolve("resolver_glob_dir/data_*.csv");
    assert(result.size() == 2);
    assert(result[0].find("data_01.csv") != std::string::npos);
    assert(result[1].find("data_02.csv") != std::string::npos);

    fs::remove_all("resolver_glob_dir");
    std::cout << "test_glob_pattern passed\n";
}

void test_glob_question_mark() {
    fs::create_directory("resolver_qmark_dir");
    create_file("resolver_qmark_dir/f1.csv", "x\n");
    create_file("resolver_qmark_dir/f2.csv", "x\n");
    create_file("resolver_qmark_dir/f10.csv", "x\n");

    auto result = FilePathResolver::resolve("resolver_qmark_dir/f?.csv");
    assert(result.size() == 2);

    fs::remove_all("resolver_qmark_dir");
    std::cout << "test_glob_question_mark passed\n";
}

void test_glob_no_match() {
    fs::create_directory("resolver_nomatch_dir");
    create_file("resolver_nomatch_dir/a.txt", "x\n");

    try {
        FilePathResolver::resolve("resolver_nomatch_dir/*.csv");
        assert(false && "Expected exception");
    } catch (const std::runtime_error&) {}

    fs::remove_all("resolver_nomatch_dir");
    std::cout << "test_glob_no_match passed\n";
}

int main() {
    test_single_file();
    test_nonexistent_file();
    test_empty_path();
    test_directory();
    test_directory_empty();
    test_glob_pattern();
    test_glob_question_mark();
    test_glob_no_match();

    std::cout << "\nAll FilePathResolver tests passed!\n";
    return 0;
}

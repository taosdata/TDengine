#include "EncodingConverter.hpp"
#include <cassert>
#include <iostream>
#include <string>

void test_utf8_to_utf8() {
    std::string data = "hello, 世界";
    std::string result = EncodingConverter::convert(data, EncodingType::UTF8, EncodingType::UTF8);
    assert(result == data);
    std::cout << "test_utf8_to_utf8 passed.\n";
}

void test_empty_input() {
    std::string data = "";
    std::string result = EncodingConverter::convert(data, EncodingType::UTF8, EncodingType::GBK);
    assert(result.empty());
    std::cout << "test_empty_input passed.\n";
}

void test_unsupported_encoding() {
    bool caught = false;
    try {
        EncodingConverter::convert("abc", static_cast<EncodingType>(999), EncodingType::UTF8);
    } catch (const std::invalid_argument& e) {
        assert(std::string(e.what()).find("Unsupported encoding type") != std::string::npos);
        caught = true;
    }
    (void)caught;
    assert(caught);
    std::cout << "test_unsupported_encoding passed.\n";
}

void test_utf8_to_gbk_and_back() {
    std::string utf8_str = "你好，世界";
    std::string gbk_str = EncodingConverter::convert(utf8_str, EncodingType::UTF8, EncodingType::GBK);
    std::string utf8_back = EncodingConverter::convert(gbk_str, EncodingType::GBK, EncodingType::UTF8);
    assert(utf8_back == utf8_str);
    std::cout << "test_utf8_to_gbk_and_back passed.\n";
}

void test_utf8_to_gb18030_and_back() {
    std::string utf8_str = "你好，世界";
    std::string gb18030_str = EncodingConverter::convert(utf8_str, EncodingType::UTF8, EncodingType::GB18030);
    std::string utf8_back = EncodingConverter::convert(gb18030_str, EncodingType::GB18030, EncodingType::UTF8);
    assert(utf8_back == utf8_str);
    std::cout << "test_utf8_to_gb18030_and_back passed.\n";
}

void test_utf8_to_big5_and_back() {
    std::string utf8_str = "你好，世界";
    std::string big5_str = EncodingConverter::convert(utf8_str, EncodingType::UTF8, EncodingType::BIG5);
    std::string utf8_back = EncodingConverter::convert(big5_str, EncodingType::BIG5, EncodingType::UTF8);
    assert(utf8_back == utf8_str);
    std::cout << "test_utf8_to_big5_and_back passed.\n";
}

void test_none_encoding() {
    std::string data = "test none encoding";
    std::string result = EncodingConverter::convert(data, EncodingType::NONE, EncodingType::NONE);
    assert(result == data);
    std::cout << "test_none_encoding passed.\n";
}

int main() {
    test_utf8_to_utf8();
    test_empty_input();
    test_unsupported_encoding();
    test_utf8_to_gbk_and_back();
    test_utf8_to_gb18030_and_back();
    test_utf8_to_big5_and_back();
    test_none_encoding();
    std::cout << "All EncodingConverter tests passed.\n";
    return 0;
}
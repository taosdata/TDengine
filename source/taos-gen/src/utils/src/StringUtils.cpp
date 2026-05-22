#include "StringUtils.hpp"
#include <algorithm>
#include <cctype>
#include <string>
#include <iterator>
#include <iconv.h>
#include <stdexcept>
#include <vector>

namespace {
inline bool is_ascii_space(unsigned char ch) {
    return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v';
}

inline char to_lower_ascii(unsigned char ch) {
    if (ch >= 'A' && ch <= 'Z') {
        return static_cast<char>(ch - 'A' + 'a');
    }
    return static_cast<char>(ch);
}

inline char to_upper_ascii(unsigned char ch) {
    if (ch >= 'a' && ch <= 'z') {
        return static_cast<char>(ch - 'a' + 'A');
    }
    return static_cast<char>(ch);
}
}

std::string StringUtils::to_lower(const std::string& str) {
    std::string lower_str = str;
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(),
                   [](unsigned char c) { return to_lower_ascii(c); });
    return lower_str;
}

std::string StringUtils::to_upper(const std::string& str) {
    std::string upper_str = str;
    std::transform(upper_str.begin(), upper_str.end(), upper_str.begin(),
                   [](unsigned char c) { return to_upper_ascii(c); });
    return upper_str;
}

void StringUtils::trim(std::string& str) {
    str.erase(str.begin(), std::find_if(str.begin(), str.end(), [](unsigned char ch) {
        return !is_ascii_space(ch);
    }));

    str.erase(std::find_if(str.rbegin(), str.rend(), [](unsigned char ch) {
        return !is_ascii_space(ch);
    }).base(), str.end());
}

std::string_view StringUtils::trim_view(std::string_view sv) {
    while (!sv.empty() && is_ascii_space(static_cast<unsigned char>(sv.front()))) {
        sv.remove_prefix(1);
    }
    while (!sv.empty() && is_ascii_space(static_cast<unsigned char>(sv.back()))) {
        sv.remove_suffix(1);
    }
    return sv;
}

bool StringUtils::iequals_ascii(std::string_view input, std::string_view expected_lower) {
    if (input.size() != expected_lower.size()) {
        return false;
    }
    for (size_t i = 0; i < input.size(); ++i) {
        unsigned char ch = static_cast<unsigned char>(input[i]);
        if (to_lower_ascii(ch) != expected_lower[i]) {
            return false;
        }
    }
    return true;
}

void StringUtils::remove_all_spaces(std::string& str) {
    str.erase(std::remove_if(str.begin(), str.end(),
        [](unsigned char ch) { return is_ascii_space(ch); }),
        str.end());
}

// iconv-based utf8 to u16string
std::u16string StringUtils::utf8_to_u16string(const std::string& str) {
    iconv_t cd = iconv_open("UTF-16LE", "UTF-8");
    if (cd == (iconv_t)-1) {
        throw std::runtime_error("iconv_open failed for UTF-8 to UTF-16LE");
    }

    size_t inbytesleft = str.size();
    size_t outbytesleft = (inbytesleft + 1) * 2;
    std::vector<char> outbuf(outbytesleft);
    char* inbuf = const_cast<char*>(str.data());
    char* outptr = outbuf.data();

    size_t res = iconv(cd, &inbuf, &inbytesleft, &outptr, &outbytesleft);
    iconv_close(cd);

    if (res == (size_t)-1) {
        throw std::runtime_error("iconv conversion failed (utf8_to_u16string)");
    }

    size_t outsize = outptr - outbuf.data();
    std::u16string result(reinterpret_cast<const char16_t*>(outbuf.data()), outsize / 2);
    return result;
}

// iconv-based u16string to utf8
std::string StringUtils::u16string_to_utf8(const std::u16string& str) {
    iconv_t cd = iconv_open("UTF-8", "UTF-16LE");
    if (cd == (iconv_t)-1) {
        throw std::runtime_error("iconv_open failed for UTF-16LE to UTF-8");
    }

    size_t inbytesleft = str.size() * 2;
    size_t outbytesleft = inbytesleft * 2 + 1;
    std::vector<char> outbuf(outbytesleft);
    char* inbuf = reinterpret_cast<char*>(const_cast<char16_t*>(str.data()));
    char* outptr = outbuf.data();

    size_t res = iconv(cd, &inbuf, &inbytesleft, &outptr, &outbytesleft);
    iconv_close(cd);

    if (res == (size_t)-1) {
        throw std::runtime_error("iconv conversion failed (u16string_to_utf8)");
    }

    return std::string(outbuf.data(), outptr - outbuf.data());
}
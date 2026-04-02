#include "EncodingConverter.hpp"
#include <stdexcept>
#include <vector>
#include <cassert>

const char* EncodingConverter::encoding_to_iconv(EncodingType type) {
    switch (type) {
        case EncodingType::NONE:
        case EncodingType::UTF8:     return "UTF-8";
        case EncodingType::GBK:      return "GBK";
        case EncodingType::GB18030:  return "GB18030";
        case EncodingType::BIG5:     return "BIG5";
        default:
            throw std::invalid_argument("Unsupported encoding type");
    }
}

void EncodingConverter::throw_iconv_error(const char* operation) {
    switch (errno) {
        case EILSEQ: throw std::runtime_error(operation + std::string(": Illegal byte sequence"));
        case EINVAL: throw std::runtime_error(operation + std::string(": Incomplete multibyte sequence"));
        case E2BIG:  throw std::runtime_error(operation + std::string(": Output buffer too small"));
        default:     throw std::runtime_error(operation + std::string(": Unknown error: ") + std::strerror(errno));
    }
}

std::string EncodingConverter::convert(const std::string& data,
                                      EncodingType from,
                                      EncodingType to) {
    if (data.empty()) {
        return "";
    }

    // Return directly if encoding is the same
    if (from == to) {
        return data;
    }

    // Get encoding names
    const char* tocode = encoding_to_iconv(to);
    const char* fromcode = encoding_to_iconv(from);

    // Create conversion descriptor
    iconv_t cd = iconv_open(tocode, fromcode);
    if (cd == (iconv_t)(-1)) {
        throw_iconv_error("iconv_open failed");
    }

    // Set input/output buffers
    char* inbuf = const_cast<char*>(data.data());
    size_t inbytesleft = data.size();

    // Initial output buffer size
    size_t outbuf_size = data.size() * 4;
    std::vector<char> outbuf(outbuf_size);

    char* outptr = outbuf.data();
    size_t outbytesleft = outbuf_size;

    // Perform conversion
    size_t result = iconv(cd, &inbuf, &inbytesleft, &outptr, &outbytesleft);

    // Handle conversion result
    if (result == (size_t)(-1)) {
        iconv_close(cd);
        throw_iconv_error("iconv conversion failed");
    }

    // Close conversion descriptor
    if (iconv_close(cd) == -1) {
        throw_iconv_error("iconv_close failed");
    }

    // Return the converted string
    size_t converted_size = outbuf_size - outbytesleft;
    return std::string(outbuf.data(), converted_size);
}
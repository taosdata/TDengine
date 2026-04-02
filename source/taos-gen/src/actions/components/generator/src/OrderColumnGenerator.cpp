#include "OrderColumnGenerator.hpp"
#include <stdexcept>


OrderColumnGenerator::OrderColumnGenerator(const ColumnConfigInstance& instance)
    : ColumnGenerator(instance), current_(0)
{
    min_ = instance.config().order_min.value_or(0);
    max_ = instance.config().order_max.value_or(10000);
    current_ = min_;

    if (max_ <= min_) {
        throw std::runtime_error("Invalid order range");
    }

    // Type conversion function
    switch (instance.config().type_tag) {
        case ColumnTypeTag::BOOL:
            convert_ = [](int64_t v) -> ColumnType { return v != 0; };
            break;
        case ColumnTypeTag::TINYINT:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<int8_t>(v); };
            break;
        case ColumnTypeTag::TINYINT_UNSIGNED:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<uint8_t>(v); };
            break;
        case ColumnTypeTag::SMALLINT:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<int16_t>(v); };
            break;
        case ColumnTypeTag::SMALLINT_UNSIGNED:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<uint16_t>(v); };
            break;
        case ColumnTypeTag::INT:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<int32_t>(v); };
            break;
        case ColumnTypeTag::INT_UNSIGNED:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<uint32_t>(v); };
            break;
        case ColumnTypeTag::BIGINT:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<int64_t>(v); };
            break;
        case ColumnTypeTag::BIGINT_UNSIGNED:
            convert_ = [](int64_t v) -> ColumnType { return static_cast<uint64_t>(v); };
            break;
        default:
            throw std::runtime_error("OrderColumnGenerator: unsupported type_tag for order column");
    }
}

ColumnType OrderColumnGenerator::generate() const {
    if (current_ >= max_) {
        current_ = min_;
    }

    int64_t value = current_;
    current_++;
    return convert_(value);
}

ColumnTypeVector OrderColumnGenerator::generate(size_t count) const {
    ColumnTypeVector values;
    values.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        if (current_ >= max_) {
            current_ = min_;
        }

        values.push_back(convert_(current_));
        current_++;
    }

    return values;
}
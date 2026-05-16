#include "ExprColumnGenerator.hpp"
#include <cmath>
#include <random>
#include <stdexcept>
#include <string>

using ConvertFunc = ColumnType(*)(const ColumnType&);
namespace {
    // double -> various integer/floating-point types
    template<typename T>
    ColumnType double_to(const ColumnType& v) {
        return static_cast<T>(std::get<double>(v));
    }

    // double -> string (e.g., for numeric Lua results targeting varchar columns)
    ColumnType double_to_string(const ColumnType& v) {
        return std::to_string(std::get<double>(v));
    }

    // double -> u16string
    ColumnType double_to_u16string(const ColumnType& v) {
        auto s = std::to_string(std::get<double>(v));
        return std::u16string(s.begin(), s.end());
    }

    // string -> u16string
    ColumnType string_to_u16string(const ColumnType& v) {
        const std::string& s = std::get<std::string>(v);
        return StringUtils::utf8_to_u16string(s);
    }

    static const ConvertFunc convert_from_double[] = {
        nullptr,                // NullValue
        nullptr,                // NoneValue
        double_to<bool>,        // bool
        double_to<int8_t>,      // int8_t
        double_to<uint8_t>,     // uint8_t
        double_to<int16_t>,     // int16_t
        double_to<uint16_t>,    // uint16_t
        double_to<int32_t>,     // int32_t
        double_to<uint32_t>,    // uint32_t
        double_to<int64_t>,     // int64_t
        double_to<uint64_t>,    // uint64_t
        double_to<float>,       // float
        nullptr,                // double
        nullptr,                // Decimal
        double_to_u16string,    // u16string
        double_to_string,       // string
        nullptr,                // JsonValue
        nullptr,                // vector<uint8_t>
        nullptr                 // Geometry
    };

    static const ConvertFunc convert_from_string[] = {
        nullptr,                // NullValue
        nullptr,                // NoneValue
        nullptr,                // bool
        nullptr,                // int8_t
        nullptr,                // uint8_t
        nullptr,                // int16_t
        nullptr,                // uint16_t
        nullptr,                // int32_t
        nullptr,                // uint32_t
        nullptr,                // int64_t
        nullptr,                // uint64_t
        nullptr,                // float
        nullptr,                // double
        nullptr,                // Decimal
        string_to_u16string,    // u16string
        nullptr,                // string
        nullptr,                // JsonValue
        nullptr,                // vector<uint8_t>
        nullptr                 // Geometry
    };
}

ExprColumnGenerator::ExprColumnGenerator(const std::string& table_name, const ColumnConfigInstance& instance)
    : ColumnGenerator(instance),
      engine_(table_name, *instance.config().formula) {}

ExprColumnGenerator::ExprColumnGenerator(const ColumnConfigInstance& instance)
    : ExprColumnGenerator("", instance) {}

ColumnType ExprColumnGenerator::generate() const {
    ColumnType value = engine_.evaluate();
    auto target_index = instance_.config().type_index;
    auto source_index = value.index();

    if (source_index == target_index) {
        return value;
    }

    return std::visit([source_index, target_index](auto&& v) -> ColumnType {
        using T = std::decay_t<decltype(v)>;
        // double
        if constexpr (std::is_same_v<T, double>) {
            if (target_index < std::size(convert_from_double) && convert_from_double[target_index]) {
                return convert_from_double[target_index](ColumnType{v});
            }
        }
        // string
        else if constexpr (std::is_same_v<T, std::string>) {
            if (target_index < std::size(convert_from_string) && convert_from_string[target_index]) {
                return convert_from_string[target_index](ColumnType{v});
            }
        }

        throw std::runtime_error(
            "ExprColumnGenerator: Unsupported type conversion from source index(" + std::to_string(source_index) +
            ") to target index(" + std::to_string(target_index) + ")");
    }, value);
}

ColumnTypeVector ExprColumnGenerator::generate(size_t count) const {
    ColumnTypeVector values;
    values.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        values.push_back(generate());
    }

    return values;
}
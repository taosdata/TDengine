#include "RandomColumnGenerator.hpp"
#include "StringUtils.hpp"
#include "pcg_random.hpp"
#include <random>
#include <stdexcept>
#include <algorithm>
#include <charconv>

// Thread-local random number engine
// static thread_local std::mt19937_64 random_engine(std::random_device{}());
// static thread_local std::minstd_rand random_engine(std::random_device{}());
static thread_local pcg32_fast random_engine(pcg_extras::seed_seq_from<std::random_device>{});

RandomColumnGenerator::RandomColumnGenerator(const ColumnConfigInstance& instance)
    : ColumnGenerator(instance) {
    initialize_generator();
}

void RandomColumnGenerator::initialize_generator() {
    // If values are configured, use values first
    if (instance_.config().values_count > 0) {
        cached_values_.reserve(instance_.config().values_count);

        if (!instance_.config().is_var_length()) {
            for (const auto& dbl_value : instance_.config().dbl_values) {
                switch (instance_.config().type_tag) {
                    case ColumnTypeTag::BOOL:
                        cached_values_.push_back(dbl_value != 0.0);
                        break;
                    case ColumnTypeTag::TINYINT:
                        cached_values_.push_back(static_cast<int8_t>(dbl_value));
                        break;
                    case ColumnTypeTag::TINYINT_UNSIGNED:
                        cached_values_.push_back(static_cast<uint8_t>(dbl_value));
                        break;
                    case ColumnTypeTag::SMALLINT:
                        cached_values_.push_back(static_cast<int16_t>(dbl_value));
                        break;
                    case ColumnTypeTag::SMALLINT_UNSIGNED:
                        cached_values_.push_back(static_cast<uint16_t>(dbl_value));
                        break;
                    case ColumnTypeTag::INT:
                        cached_values_.push_back(static_cast<int32_t>(dbl_value));
                        break;
                    case ColumnTypeTag::INT_UNSIGNED:
                        cached_values_.push_back(static_cast<uint32_t>(dbl_value));
                        break;
                    case ColumnTypeTag::BIGINT:
                        cached_values_.push_back(static_cast<int64_t>(dbl_value));
                        break;
                    case ColumnTypeTag::BIGINT_UNSIGNED:
                        cached_values_.push_back(static_cast<uint64_t>(dbl_value));
                        break;
                    case ColumnTypeTag::FLOAT:
                        cached_values_.push_back(static_cast<float>(dbl_value));
                        break;
                    case ColumnTypeTag::DOUBLE:
                        cached_values_.push_back(dbl_value);
                        break;
                    default:
                        throw std::runtime_error("Values not supported for this type");
                }
            }
        } else {
            for (const auto& str_value : instance_.config().str_values) {
                switch (instance_.config().type_tag) {
                    case ColumnTypeTag::NCHAR:
                        cached_values_.push_back(StringUtils::utf8_to_u16string(str_value));
                        break;
                    case ColumnTypeTag::VARCHAR:
                    case ColumnTypeTag::BINARY:
                        cached_values_.push_back(str_value);
                        break;
                    default:
                        throw std::runtime_error("Values not supported for this type");
                }
            }
        }

        // Create distribution object
        auto dist = std::uniform_int_distribution<size_t>(0, cached_values_.size() - 1);

        // Set generator to randomly select from values
        generator_ = [this, dist]() mutable {
            return cached_values_[dist(random_engine)];
        };

    } else {
        // Use min/max range to generate
        switch (instance_.config().type_tag) {
            case ColumnTypeTag::BOOL:
                generator_ = []() {
                    std::bernoulli_distribution dist(0.5);
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::TINYINT:
                generator_ = [this]() {
                    std::uniform_int_distribution<int8_t> dist(
                        static_cast<int8_t>(*instance_.config().min),
                        static_cast<int8_t>(*instance_.config().max - 1)
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::TINYINT_UNSIGNED:
                generator_ = [this]() {
                    std::uniform_int_distribution<uint8_t> dist(
                        static_cast<uint8_t>(*instance_.config().min),
                        static_cast<uint8_t>(*instance_.config().max - 1)
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::SMALLINT:
                generator_ = [this]() {
                    std::uniform_int_distribution<int16_t> dist(
                        static_cast<int16_t>(*instance_.config().min),
                        static_cast<int16_t>(*instance_.config().max - 1)
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::SMALLINT_UNSIGNED:
                generator_ = [this]() {
                    std::uniform_int_distribution<uint16_t> dist(
                        static_cast<uint16_t>(*instance_.config().min),
                        static_cast<uint16_t>(*instance_.config().max - 1)
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::INT:
                generator_ = [this]() {
                    std::uniform_int_distribution<int32_t> dist(
                        static_cast<int32_t>(*instance_.config().min),
                        static_cast<int32_t>(*instance_.config().max - 1)
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::INT_UNSIGNED:
                generator_ = [this]() {
                    std::uniform_int_distribution<uint32_t> dist(
                        static_cast<uint32_t>(*instance_.config().min),
                        static_cast<uint32_t>(*instance_.config().max - 1)
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::BIGINT:
                generator_ = [this]() {
                    std::uniform_int_distribution<int64_t> dist(
                        static_cast<int64_t>(*instance_.config().min),
                        static_cast<int64_t>(*instance_.config().max - 1)
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::BIGINT_UNSIGNED:
                generator_ = [this]() {
                    std::uniform_int_distribution<uint64_t> dist(
                        static_cast<uint64_t>(*instance_.config().min),
                        static_cast<uint64_t>(*instance_.config().max - 1)
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::FLOAT:
                generator_ = [this]() {
                    std::uniform_real_distribution<float> dist(
                        static_cast<float>(*instance_.config().min),
                        static_cast<float>(*instance_.config().max)
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::DOUBLE:
                generator_ = [this]() {
                    std::uniform_real_distribution<double> dist(
                        *instance_.config().min,
                        *instance_.config().max
                    );
                    return dist(random_engine);
                };
                break;
            case ColumnTypeTag::NCHAR:
                generator_ = [this]() {
                    int len = *instance_.config().len;
                    std::uniform_int_distribution<uint16_t> dist(0x4E00, 0x9FA5);
                    std::u16string result;
                    result.reserve(len);
                    for (int i = 0; i < len; ++i) {
                        result.push_back(static_cast<char16_t>(dist(random_engine)));
                    }
                    return result;
                };
                break;
            case ColumnTypeTag::VARCHAR:
            case ColumnTypeTag::BINARY:
                if (instance_.config().corpus) {
                    const auto& corpus = instance_.config().corpus.value();
                    std::uniform_int_distribution<size_t> dist(0, corpus.size() - 1);
                    generator_ = [corpus, dist]() mutable {
                        return std::string(1, corpus[dist(random_engine)]);
                    };
                } else {
                    static const std::string default_corpus = "abcdefghijklmnopqrstuvwxyz";
                    std::uniform_int_distribution<size_t> dist(0, default_corpus.size() - 1);
                    generator_ = [this, dist]() mutable {
                        std::string result;
                        result.reserve(*instance_.config().len);
                        for (int i = 0; i < *instance_.config().len; ++i) {
                            result.push_back(default_corpus[dist(random_engine)]);
                        }
                        return result;
                    };
                }
                break;
            default:
                throw std::runtime_error("Unsupported column type for random generation");
        }
    }
}

ColumnType RandomColumnGenerator::generate() const {
    return generator_();
}

ColumnTypeVector RandomColumnGenerator::generate(size_t count) const {
    ColumnTypeVector values;
    values.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        values.push_back(generator_());
    }

    return values;
}
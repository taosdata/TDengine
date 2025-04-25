#ifndef DATA_GENERATOR_H
#define DATA_GENERATOR_H

#include <vector>
#include <any>
#include <string>
#include <random>
#include <fstream>
#include <taos.h>

// Forward declaration of ParameterContext and ColumnMeta
class ParameterContext;
class ColumnMeta;

// Base class for data generators
class DataGenerator {
public:
    explicit DataGenerator(const ParameterContext& ctx);
    virtual ~DataGenerator() = default;

    template<typename T>
    T generate_value(int64_t seq);

protected:
    virtual void init_buffer();
    template<typename T>
    void fill_buffer();
    template<typename T>
    T get_from_buffer(int64_t seq);

private:
    const ParameterContext& ctx_;
    std::vector<std::any> buffer_;
    size_t buffer_size_ = 0;
    bool use_buffer_ = false;
};

// Random number generation strategy
class RandomGenerator : public DataGenerator {
public:
    RandomGenerator(const ParameterContext& ctx);
    
    template<typename T>
    T realtime_generate(int64_t seq);
    
    template<typename T>
    T generate(const ColumnMeta& meta);

private:
    std::mt19937 rng_;
    std::uniform_real_distribution<double> dist_;
};

// String generation strategy
class StringGenerator : public DataGenerator {
public:
    StringGenerator(const ParameterContext& ctx);
    
    std::string realtime_generate(int64_t seq);

private:
    std::string generate_random(int64_t seq);
    std::vector<std::string> dict_;
    std::mt19937 rng_;
};

// Waveform function data generation strategy
class WaveformGenerator : public DataGenerator {
public:
    WaveformGenerator(const ParameterContext& ctx);
    
    template<typename T>
    T realtime_generate(int64_t seq);

private:
    double amplitude_;
    double frequency_;
    double phase_;
};

// CSV file generation strategy
class CsvGenerator : public DataGenerator {
public:
    CsvGenerator(const ParameterContext& ctx);
    
    template<typename T>
    T realtime_generate(int64_t seq);
    
    std::vector<TableRecord> generate_batch(size_t batch_size);

private:
    template<typename T>
    T parse_line(const std::string& line);

    std::string file_path_;
    std::ifstream file_;
};

#endif // DATA_GENERATOR_H
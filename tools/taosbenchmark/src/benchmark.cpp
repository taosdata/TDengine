#include <queue>
#include <vector>
#include <memory>
#include <thread>
#include <functional>
#include <variant>

#include "param.h"


///////////////////////////////////////////////////////////////////////////////////////////////////////


// 在DatabaseTask中根据参数选择生成器
std::unique_ptr<DataGenerator> create_generator() {
    if (params_.get<bool>("use_csv")) {
        return new CsvGenerator(params_.get<std::string>("csv_path"));
    } else {
        auto schema = params_.get<TableSchema>("schema");
        return new RandomGenerator(schema);
    }
}

// 在Writer中根据参数选择协议
std::unique_ptr<DataWriter> create_writer() {
    switch (params_.get<Protocol>("protocol")) {
        case Protocol::Native: return new NativeWriter();
        case Protocol::REST: return new RestWriter();
        // ...
    }
}



//////////////////////////////////////////////////////////////////////////////


class ResultHandler {
public:
    void handle(const QueryResult& result, const std::string& output) {
        if (!output.empty()) {
            write_to_file(result, output);
        } else {
            discard_result(result);
        }
    }

    void write_to_file(const QueryResult& res, const std::string& path) {
        std::lock_guard<std::mutex> lock(file_mutex_);
        std::ofstream fout(path, std::ios::app);
        fout << res.to_csv_format() << "\n";
    }

private:
    std::mutex file_mutex_;
};






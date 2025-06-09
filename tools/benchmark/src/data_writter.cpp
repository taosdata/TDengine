#include <queue>
#include <vector>
#include <memory>
#include <thread>
#include <functional>
#include <variant>


// 数据写入器抽象
class DataWriter {
public:
    virtual ~DataWriter() = default;
    virtual void write(const TableRecord& record) = 0;
    virtual void flush() = 0;
    virtual size_t get_written_count() const = 0;
};



class BatchWriter : public DataWriter {
public:
    explicit BatchWriter(std::unique_ptr<DataWriter> inner, size_t batch_size)
        : inner_(std::move(inner)), batch_size_(batch_size) {}

    void write(const TableRecord& record) override {
        buffer_.push_back(record);
        if (buffer_.size() >= batch_size_) {
            flush();
        }
    }

    void flush() override {
        if (!buffer_.empty()) {
            inner_->write_batch(buffer_);
            buffer_.clear();
        }
        inner_->flush();
    }

private:
    std::vector<TableRecord> buffer_;
    std::unique_ptr<DataWriter> inner_;
    size_t batch_size_;
};




// 增强型写入器基类
class EnhancedDataWriter : public DataWriter {
public:
    EnhancedDataWriter(std::unique_ptr<IProtocolHandler> protocol,
                      std::unique_ptr<IDataFormatter> formatter,
                      size_t batch_size)
        : protocol_(std::move(protocol)),
          formatter_(std::move(formatter)),
          batch_size_(batch_size) {}

    void write(const TableRecord& record) override {
        buffer_.push_back(record);
        if (buffer_.size() >= batch_size_) {
            flush();
        }
    }

    void flush() override {
        if (!buffer_.empty()) {
            const auto batch_data = formatter_->batch_format(buffer_);
            protocol_->send(batch_data);
            buffer_.clear();
        }
    }

private:
    std::vector<TableRecord> buffer_;
    std::unique_ptr<IProtocolHandler> protocol_;
    std::unique_ptr<IDataFormatter> formatter_;
    size_t batch_size_;
};



// 具体实现示例 - TDengine REST写入器
class TaosRestWriter : public DataWriter {
public:
    TaosRestWriter(const std::string& endpoint) 
        : client_(endpoint) {}

    void write(const TableRecord& record) override {
        std::string sql = build_insert_sql(record);
        client_.post("/rest/sql", sql);
    }

    void flush() override {
        client_.flush();
    }

    size_t get_written_count() const override {
        return client_.get_counter();
    }

private:
    RestClient client_;
};




// CSV文件写入器特化实现
class CsvFileWriter : public EnhancedDataWriter {
public:
    CsvFileWriter(const std::string& filename, 
                 std::unique_ptr<IDataFormatter> formatter)
        : EnhancedDataWriter(
            std::make_unique<FileProtocolHandler>(filename),
            std::move(formatter),
            1) // CSV通常逐行写入
    {
        write_header();
    }

private:
    void write_header() {
        const std::string header = "timestamp,table_name,columns...\n";
        protocol_->send(header);
    }
};

// 协议工厂扩展
class FileProtocolHandler : public IProtocolHandler {
public:
    FileProtocolHandler(const std::string& filename)
        : filename_(filename) {}

    void connect() override {
        file_.open(filename_, std::ios::out | std::ios::app);
    }

    void send(const std::string& data) override {
        file_ << data;
    }

    void close() override {
        file_.close();
    }

private:
    std::ofstream file_;
    std::string filename_;
};

// 创建使用WebSocket+STMT的写入器
auto writer = WriterFactory::create_writer(
    ParameterContext()
        .set("protocol", "websocket")
        .set("format", "stmt"),
    db_config
);

// 创建本地CSV+Schemaless格式写入器
auto csv_writer = std::make_unique<CsvFileWriter>(
    "output.csv",
    std::make_unique<SchemalessFormatter>()
);



class SmartProtocolRouter {
public:
    void add_endpoint(const std::string& pattern, 
                     std::unique_ptr<IProtocolHandler> handler) {
        routes_.emplace_back(pattern, std::move(handler));
    }

    IProtocolHandler& select_handler(const TableRecord& record) {
        for (auto& [pattern, handler] : routes_) {
            if (record.table_name.find(pattern) != std::string::npos) {
                return *handler;
            }
        }
        return default_handler_;
    }

private:
    std::vector<std::pair<std::string, std::unique_ptr<IProtocolHandler>>> routes_;
    IProtocolHandler& default_handler_;
};

class AdaptiveBatcher {
public:
    void add_record(const TableRecord& record) {
        buffer_.push_back(record);
        if (should_flush()) {
            flush();
        }
    }

private:
    bool should_flush() const {
        return buffer_.size() >= max_batch_size_ || 
               total_size() >= max_batch_bytes_;
    }

    size_t max_batch_size_ = 1000;
    size_t max_batch_bytes_ = 4 * 1024 * 1024; // 4MB
};



// 写入器工厂
class WriterFactory {
public:
    static std::unique_ptr<DataWriter> create_writer(
        const ParameterContext& params,
        const DBConfig& db_config) 
    {
        auto protocol = create_protocol(params);
        auto formatter = create_formatter(params);
        size_t batch_size = params.get<size_t>("batch_size", 1000);
        
        return std::make_unique<EnhancedDataWriter>(
            std::move(protocol),
            std::move(formatter),
            batch_size
        );
    }

private:
    static std::unique_ptr<IProtocolHandler> create_protocol(
        const ParameterContext& params) 
    {
        const auto protocol_type = params.get<std::string>("protocol", "native");
        
        if (protocol_type == "native") {
            return std::make_unique<NativeTcpHandler>(parse_connection_config(params));
        } else if (protocol_type == "websocket") {
            return std::make_unique<WebsocketHandler>(parse_ws_config(params));
        } else if (protocol_type == "rest") {
            return std::make_unique<RestHandler>(parse_rest_config(params));
        }
        throw std::invalid_argument("Unsupported protocol");
    }

    static std::unique_ptr<IDataFormatter> create_formatter(
        const ParameterContext& params)
    {
        const auto format_type = params.get<std::string>("format", "schemaless");
        
        if (format_type == "sql") {
            return std::make_unique<SqlFormatter>();
        } else if (format_type == "stmt") {
            return std::make_unique<StmtFormatter>();
        } else if (format_type == "schemaless") {
            return std::make_unique<SchemalessFormatter>();
        }
        throw std::invalid_argument("Unsupported format");
    }
};



class ConnectionPool {
public:
    Connection get_connection() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pool_.empty()) {
            return create_new_connection();
        }
        auto conn = std::move(pool_.back());
        pool_.pop_back();
        return conn;
    }

    void return_connection(Connection conn) {
        std::lock_guard<std::mutex> lock(mutex_);
        pool_.push_back(std::move(conn));
    }

private:
    std::vector<Connection> pool_;
    std::mutex mutex_;
};

// 连接池管理
class ConnectionPool {
public:
    Connection get(const DBConnectionConfig& config) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& pool = pools_[config];
        if (pool.empty()) {
            return create_connection(config);
        }
        auto conn = std::move(pool.back());
        pool.pop_back();
        return conn;
    }

    void release(Connection conn) {
        std::lock_guard<std::mutex> lock(mutex_);
        pools_[conn.config].push_back(std::move(conn));
    }

private:
    std::unordered_map<DBConnectionConfig, std::vector<Connection>> pools_;
    std::mutex mutex_;
};


class ResourcePool {
public:
    // 每个数据库任务独立连接池
    ConnectionPool get_db_pool(size_t db_index) {
        std::lock_guard<std::mutex> lock(mutex_);
        return pools_[db_index];
    }
private:
    std::unordered_map<size_t, ConnectionPool> pools_;
};



// 写入器工厂
class WriterFactory {
public:
    static std::unique_ptr<DataWriter> create(const WriterConfig& config) {
        switch (config.protocol) {
            case Protocol::TCP_NATIVE:
                return std::make_unique<TaosNativeWriter>(config);
            case Protocol::WEBSOCKET:
                return std::make_unique<TaosWebsocketWriter>(config);
            case Protocol::REST:
                return std::make_unique<TaosRestWriter>(config);
            case Protocol::FILE:
                return std::make_unique<FileWriter>(config);
            default:
                throw std::invalid_argument("Unsupported protocol");
        }
    }
};
    
// SQL构造策略
class SqlBuilder {
public:
    virtual std::string build_insert(const TableRecord& record) = 0;
};
    
class StmtBuilder : public SqlBuilder {
public:
    std::string build_insert(const TableRecord& record) override {
        // 生成参数化SQL和绑定数据
        return stmt_template_ + " " + join_bind_values(record);
    }
};
    
class LineProtocolBuilder : public SqlBuilder {
public:
    std::string build_insert(const TableRecord& record) override {
        // 生成行协议格式
        return format_line_protocol(record);
    }
};



class SmartConnection {
public:
    void execute(const std::string& sql) {
        if (need_reconnect_()) {
            reconnect_();
        }
        try {
            do_execute(sql);
        } catch (const ConnectionException& e) {
            handle_error(e);
        }
    }
private:
    std::function<bool()> need_reconnect_ = [] {
        return idle_time() > 300s || error_count_ > 5;
    };
};




// 协议处理抽象层
class IProtocolHandler {
public:
    virtual ~IProtocolHandler() = default;
    virtual void connect() = 0;
    virtual void send(const std::string& data) = 0;
    virtual void close() = 0;
    virtual bool is_connected() const = 0;
};




// 具体协议实现
class NativeTcpHandler : public IProtocolHandler {
public:
    NativeTcpHandler(const ConnectionConfig& config) 
        : config_(config) {}

    void connect() override {
        socket_ = create_tcp_socket(config_.host, config_.port);
        // TDengine 特有的鉴权流程
        authenticate(config_.user, config_.password);
    }

    void send(const std::string& data) override {
        socket_.write(data);
    }

    // 其他方法实现...

private:
    TcpSocket socket_;
    ConnectionConfig config_;
};

class WebsocketHandler : public IProtocolHandler {
public:
    void connect() override {
        ws_client_.connect(config_.url);
        ws_client_.set_proxy(config_.proxy);
    }
    // 其他方法实现...
};

void adaptive_retry(Operation op) {
    const vector<chrono::milliseconds> backoff = {50ms, 100ms, 200ms};
    for (int attempt = 0; attempt < 3; ++attempt) {
        try {
            op();
            return;
        } catch (const Exception& e) {
            if (e.is_fatal()) throw;
            sleep(backoff[attempt]);
        }
    }
    throw MaxRetryException();
}


class RetryPolicy {
public:
    void execute_with_retry(std::function<void()> operation) {
        int attempts = 0;
        while (attempts < max_retries_) {
            try {
                operation();
                return;
            } catch (const NetworkException& e) {
                handle_retry(e, ++attempts);
            }
        }
        throw OperationFailed("Max retries exceeded");
    }

private:
    int max_retries_ = 3;
};



class MultiplexedConnection {
public:
    void send(const std::string& data) {
        auto& conn = get_connection();
        conn.send(data);
    }

private:
    std::vector<TcpConnection> connections_;
    std::atomic<size_t> counter_{0};

    TcpConnection& get_connection() {
        size_t idx = counter_++ % connections_.size();
        return connections_[idx];
    }
};


// 从参数上下文中获取并发控制参数
void DatabaseTask::init_threads() {
    size_t writer_threads = params_.get<int>("writer_threads_per_db", 4);
    consumers_.resize(writer_threads);
}


class LoadBalancer {
public:
    size_t select_writer(const TableRecord& rec) {
        // 根据表名哈希选择写入器
        size_t idx = hash_fn(rec.table_name) % writers_.size();
        return idx;
    }
};




#include "InfluxDBClient.hpp"
#include "LogUtils.hpp"
#include <curl/curl.h>
#include <atomic>
#include <cstdlib>
#include <mutex>
#include <stdexcept>
#include <vector>
#include <zlib.h>

namespace {

std::once_flag curl_global_init_once;
std::once_flag curl_global_cleanup_register_once;
std::atomic<bool> curl_global_ready{false};

void curl_global_cleanup_at_exit() {
    if (curl_global_ready.load(std::memory_order_acquire)) {
        curl_global_cleanup();
        curl_global_ready.store(false, std::memory_order_release);
    }
}

bool ensure_curl_global_initialized() {
    std::call_once(curl_global_init_once, []() {
        CURLcode code = curl_global_init(CURL_GLOBAL_DEFAULT);
        if (code != CURLE_OK) {
            LogUtils::error("InfluxDB: curl_global_init failed: {}", curl_easy_strerror(code));
            return;
        }

        curl_global_ready.store(true, std::memory_order_release);
        std::call_once(curl_global_cleanup_register_once, []() {
            std::atexit(curl_global_cleanup_at_exit);
        });
    });

    return curl_global_ready.load(std::memory_order_acquire);
}

} // namespace

// Callback to capture response body
static size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* response = static_cast<std::string*>(userdata);
    response->append(ptr, size * nmemb);
    return size * nmemb;
}

CurlInfluxDBClient::CurlInfluxDBClient(const InfluxDBConfig& config, const InfluxDBFormatOptions& format_options)
    : config_(config), format_options_(format_options) {
    write_url_ = build_write_url();
    auth_header_ = build_auth_header();
}

CurlInfluxDBClient::~CurlInfluxDBClient() {
    close();
}

std::string CurlInfluxDBClient::build_write_url() const {
    // POST /api/v2/write?org={org}&bucket={bucket}&precision={precision}
    std::string url = config_.url;
    if (!url.empty() && url.back() == '/') {
        url.pop_back();
    }

    // URL-encode query parameters to handle spaces/special characters
    CURL* tmp = curl_easy_init();
    if (tmp) {
        auto url_encode = [&](const std::string& s) -> std::string {
            char* encoded = curl_easy_escape(tmp, s.c_str(), static_cast<int>(s.size()));
            if (!encoded) {
                LogUtils::warn("InfluxDB: curl_easy_escape failed for '{}', using unescaped value", s);
                return s;
            }
            std::string result(encoded);
            curl_free(encoded);
            return result;
        };
        url += "/api/v2/write?org=" + url_encode(config_.org) +
               "&bucket=" + url_encode(config_.bucket) +
               "&precision=" + url_encode(format_options_.precision);
        curl_easy_cleanup(tmp);
    } else {
        // Fallback without encoding (best-effort)
        url += "/api/v2/write?org=" + config_.org +
               "&bucket=" + config_.bucket +
               "&precision=" + format_options_.precision;
    }
    return url;
}

std::string CurlInfluxDBClient::build_auth_header() const {
    return "Token " + config_.token;
}

bool CurlInfluxDBClient::connect() {
    if (is_connected_) return true;

    if (!ensure_curl_global_initialized()) {
        return false;
    }

    curl_ = curl_easy_init();
    if (!curl_) {
        LogUtils::error("InfluxDB: failed to initialize curl");
        return false;
    }

    // Set common options
    curl_easy_setopt(curl_, CURLOPT_URL, write_url_.c_str());
    curl_easy_setopt(curl_, CURLOPT_POST, 1L);
    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPIDLE, 60L);
    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPINTVL, 30L);
    curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(curl_, CURLOPT_CONNECTTIMEOUT, 10L);

    is_connected_ = true;
    LogUtils::debug("InfluxDB client connected to: {}", write_url_);
    return true;
}

bool CurlInfluxDBClient::is_connected() const {
    return is_connected_;
}

void CurlInfluxDBClient::close() {
    if (curl_) {
        curl_easy_cleanup(curl_);
        curl_ = nullptr;
    }
    is_connected_ = false;
}

bool CurlInfluxDBClient::execute(const InfluxDBInsertData& data) {
    if (!curl_) {
        throw std::runtime_error("InfluxDB client not connected");
    }

    if (data.lines.empty()) {
        return true;
    }

    size_t batch_size = format_options_.batch_size;

    // Fast path: if total rows fit in one batch, send directly
    if (static_cast<size_t>(data.total_rows) <= batch_size) {
        return send_chunk(data.lines.c_str(), data.lines.size());
    }

    // Split lines into chunks of batch_size
    const std::string& lines = data.lines;
    size_t pos = 0;
    size_t line_count = 0;
    size_t chunk_start = 0;

    while (pos < lines.size()) {
        size_t nl = lines.find('\n', pos);
        if (nl == std::string::npos) {
            nl = lines.size();
        }
        line_count++;
        size_t next_pos = (nl < lines.size()) ? nl + 1 : nl;

        if (line_count >= batch_size || next_pos >= lines.size()) {
            size_t chunk_end = (nl < lines.size()) ? nl : lines.size();
            size_t chunk_len = chunk_end - chunk_start;

            if (chunk_len > 0) {
                if (!send_chunk(lines.c_str() + chunk_start, chunk_len)) {
                    return false;
                }
            }
            chunk_start = next_pos;
            line_count = 0;
        }

        pos = next_pos;
    }

    return true;
}

bool CurlInfluxDBClient::send_chunk(const char* chunk_data, size_t chunk_size) {
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, ("Authorization: " + auth_header_).c_str());
    headers = curl_slist_append(headers, "Content-Type: text/plain; charset=utf-8");

    const char* post_data = chunk_data;
    curl_off_t post_size = static_cast<curl_off_t>(chunk_size);

    // Optional gzip compression
    std::vector<uint8_t> compressed;
    if (format_options_.gzip && chunk_size > 0) {
        headers = curl_slist_append(headers, "Content-Encoding: gzip");

        uLongf compressed_size = compressBound(chunk_size);
        compressed.resize(compressed_size + 18); // gzip header/trailer overhead

        z_stream zs{};
        // deflateInit2 with gzip encoding (windowBits = 15 + 16)
        if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
            curl_slist_free_all(headers);
            LogUtils::error("InfluxDB: gzip deflateInit2 failed");
            return false;
        }

        zs.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(chunk_data));
        zs.avail_in = static_cast<uInt>(chunk_size);
        zs.next_out = compressed.data();
        zs.avail_out = static_cast<uInt>(compressed.size());

        int ret = deflate(&zs, Z_FINISH);
        deflateEnd(&zs);

        if (ret != Z_STREAM_END) {
            curl_slist_free_all(headers);
            LogUtils::error("InfluxDB: gzip compression failed");
            return false;
        }

        compressed.resize(zs.total_out);
        post_data = reinterpret_cast<const char*>(compressed.data());
        post_size = static_cast<curl_off_t>(compressed.size());
    }

    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE_LARGE, post_size);
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, post_data);

    // Capture response
    std::string response_body;
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_body);

    CURLcode res = curl_easy_perform(curl_);
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, nullptr);
    curl_slist_free_all(headers);

    if (res != CURLE_OK) {
        LogUtils::error("InfluxDB write failed (curl error): {}", curl_easy_strerror(res));
        return false;
    }

    long http_code = 0;
    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_code);

    if (http_code == 204) {
        return true;
    }

    // Error: InfluxDB returns non-204
    std::string preview = response_body.size() > 200
        ? response_body.substr(0, 200) + "..."
        : response_body;
    LogUtils::error("InfluxDB write failed (HTTP {}): {}", http_code, preview);
    return false;
}

// InfluxDBClient wrapper

InfluxDBClient::InfluxDBClient(const InfluxDBConfig& config, const InfluxDBFormatOptions& format_options)
    : client_(std::make_unique<CurlInfluxDBClient>(config, format_options)) {}

InfluxDBClient::~InfluxDBClient() = default;

bool InfluxDBClient::connect() { return client_->connect(); }
bool InfluxDBClient::is_connected() const { return client_->is_connected(); }
void InfluxDBClient::close() { client_->close(); }
bool InfluxDBClient::execute(const InfluxDBInsertData& data) { return client_->execute(data); }

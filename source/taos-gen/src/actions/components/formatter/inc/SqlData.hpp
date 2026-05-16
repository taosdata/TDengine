#pragma once
#include <string>

class SqlData {
public:
    explicit SqlData(std::string&& sql_str)
        : sql_buffer_(std::move(sql_str)) 
    {
        c_str_ = sql_buffer_.data();
    }

    SqlData(const SqlData&) = delete;
    SqlData& operator=(const SqlData&) = delete;

    SqlData(SqlData&&) = default;
    SqlData& operator=(SqlData&&) = default;

    const char* c_str() const noexcept { return c_str_; }
    size_t size() const noexcept { return sql_buffer_.size(); }
    const std::string& str() const noexcept { return sql_buffer_; }

private:
    std::string sql_buffer_;
    const char* c_str_ = nullptr;
};
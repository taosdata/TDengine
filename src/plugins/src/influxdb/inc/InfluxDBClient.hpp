#pragma once

#include "InfluxDBConfig.hpp"
#include "InfluxDBFormatOptions.hpp"
#include "InfluxDBInsertData.hpp"
#include <curl/curl.h>
#include <string>
#include <memory>

class IInfluxDBClient {
public:
    virtual ~IInfluxDBClient() = default;
    virtual bool connect() = 0;
    virtual bool is_connected() const = 0;
    virtual void close() = 0;
    virtual bool execute(const InfluxDBInsertData& data) = 0;
};

class CurlInfluxDBClient : public IInfluxDBClient {
public:
    CurlInfluxDBClient(const InfluxDBConfig& config, const InfluxDBFormatOptions& format_options);
    ~CurlInfluxDBClient() override;

    bool connect() override;
    bool is_connected() const override;
    void close() override;
    bool execute(const InfluxDBInsertData& data) override;

    const std::string& write_url() const { return write_url_; }

private:
    std::string build_write_url() const;
    std::string build_auth_header() const;
    bool send_chunk(const char* data, size_t size);

    const InfluxDBConfig& config_;
    const InfluxDBFormatOptions& format_options_;
    CURL* curl_ = nullptr;
    bool is_connected_ = false;
    std::string write_url_;
    std::string auth_header_;
};

class InfluxDBClient {
public:
    InfluxDBClient(const InfluxDBConfig& config, const InfluxDBFormatOptions& format_options);
    ~InfluxDBClient();

    bool connect();
    bool is_connected() const;
    void close();
    bool execute(const InfluxDBInsertData& data);

    void set_client(std::unique_ptr<IInfluxDBClient> client) {
        client_ = std::move(client);
    }

private:
    std::unique_ptr<IInfluxDBClient> client_;
};

#pragma once
#include "FormatResult.hpp"
#include "InsertDataConfig.hpp"
#include "ActionMetrics.hpp"
#include "ConnectorSource.hpp"
#include <optional>
#include <memory>
#include <variant>
#include <chrono>

class ISinkPlugin {
public:
    virtual ~ISinkPlugin() = default;

    // Connection interface (required)
    virtual bool connect() = 0;
    virtual void close() noexcept = 0;
    virtual bool is_connected() const = 0;

    // Connection with source interface (optional)
    virtual bool connect_with_source(std::optional<ConnectorSource>& conn_source) = 0;

    // Prepare interface (optional)
    virtual bool prepare() = 0;

    // Format interface (required)
    virtual FormatResult format(
        MemoryPool::MemoryBlock* block,
        bool is_checkpoint_recover = false
    ) const = 0;

    // Write interface (required)
    virtual bool write(const BaseInsertData& data) = 0;

    // Get timestamp precision
    virtual std::string get_timestamp_precision() const noexcept = 0;

    // Metrics interface
    virtual const ActionMetrics& get_play_metrics() const noexcept = 0;
    virtual const ActionMetrics& get_write_metrics() const noexcept = 0;
    virtual std::chrono::steady_clock::time_point start_write_time() const noexcept = 0;
    virtual std::chrono::steady_clock::time_point end_write_time() const noexcept = 0;
};
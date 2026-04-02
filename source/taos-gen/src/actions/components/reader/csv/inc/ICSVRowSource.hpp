#pragma once

#include "TableData.hpp"
#include <optional>

class ICSVRowSource {
public:
    virtual ~ICSVRowSource() = default;

    // Get next row, returns nullopt when data is exhausted
    virtual std::optional<RowData> next() = 0;

    // Whether more data is available
    virtual bool has_more() const = 0;

    // Reset to start position (for repeat_read)
    virtual void reset() = 0;

    // Get total number of rows (0 if unknown in streaming)
    virtual size_t total_rows() const = 0;
};

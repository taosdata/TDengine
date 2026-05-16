#pragma once

#include "TagsCSV.hpp"
#include "ColumnsCSV.hpp"
#include "TimestampCSVConfig.hpp"

struct FromCSVConfig {
    bool enabled = false;
    TimestampCSVConfig csv;
    TagsCSV tags;
    ColumnsCSV columns;
};

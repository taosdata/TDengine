#pragma once
#include <vector>
#include <string>
#include "IStmtData.hpp"
#include "ColumnConfigInstance.hpp"
#include "TableData.hpp"

class StmtV2Data : public IStmtData {
public:
    explicit StmtV2Data(const ColumnConfigInstanceVector& col_instances, MultiBatch&& batch);
    StmtV2Data(StmtV2Data&& other) noexcept;
    StmtV2Data& operator=(StmtV2Data&& other) = delete;
    StmtV2Data(const StmtV2Data&) = delete;
    StmtV2Data& operator=(const StmtV2Data&) = delete;

    size_t row_count() const noexcept override;
    size_t column_count() const noexcept override;
    const TAOS_STMT2_BINDV* bindv_ptr() const noexcept override;

private:
    struct ColumnMemory {
        std::vector<char> buffer;
        std::vector<int32_t> lengths;
        std::vector<char> is_nulls;
    };
    struct TableMemory {
        std::vector<ColumnMemory> column_data;
    };

    MultiBatch batch_;
    const ColumnConfigInstanceVector& col_instances_;
    std::vector<const char*> table_names_;
    std::vector<std::vector<TAOS_STMT2_BIND>> column_binds_;
    std::vector<TAOS_STMT2_BIND*> column_bind_ptrs_;
    std::vector<TableMemory> table_memories_;
    TAOS_STMT2_BINDV bindv_{};
};
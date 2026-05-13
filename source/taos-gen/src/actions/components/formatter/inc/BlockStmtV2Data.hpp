#pragma once

#include "IStmtData.hpp"
#include "MemoryPool.hpp"


class BlockStmtV2Data : public IStmtData {
public:
    BlockStmtV2Data(MemoryPool::MemoryBlock* block,
                   const ColumnConfigInstanceVector& col_instances)
        : block_(block), col_instances_(col_instances) {}

    size_t row_count() const noexcept override {
        return block_ ? block_->total_rows : 0;
    }

    size_t column_count() const noexcept override {
        return col_instances_.size();
    }

    const TAOS_STMT2_BINDV* bindv_ptr() const noexcept override {
        return block_ ? &block_->bindv_ : nullptr;
    }

    MemoryPool::MemoryBlock* get_block() const noexcept {
        return block_;
    }

    void reset_block() noexcept {
        block_ = nullptr;
    }

private:
    MemoryPool::MemoryBlock* block_;
    const ColumnConfigInstanceVector& col_instances_;
};
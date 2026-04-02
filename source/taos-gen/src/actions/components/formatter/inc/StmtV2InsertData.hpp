#pragma once
#include "MemoryPool.hpp"
#include <cstdint>
#include <typeindex>

struct StmtV2InsertData {

    StmtV2InsertData(MemoryPool::MemoryBlock* block, bool is_checkpoint_recover = false) : block_(block) {
        block->build_bindv(is_checkpoint_recover);
    }

    StmtV2InsertData(StmtV2InsertData&& other) noexcept = default;

    // Disable copy
    StmtV2InsertData(const StmtV2InsertData&) = delete;
    StmtV2InsertData& operator=(const StmtV2InsertData&) = delete;
    StmtV2InsertData& operator=(StmtV2InsertData&&) = delete;

    ~StmtV2InsertData() = default;

    MemoryPool::MemoryBlock* block_;
};

inline std::type_index STMTV2_TYPE_ID = std::type_index(typeid(StmtV2InsertData));
inline uint64_t STMTV2_TYPE_HASH = STMTV2_TYPE_ID.hash_code();
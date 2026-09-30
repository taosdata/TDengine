#ifndef TDENGINE_READER_FETCH_BENCHMARK_TEST_H
#define TDENGINE_READER_FETCH_BENCHMARK_TEST_H

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <utility>
#include <vector>

#include "streamMsg.h"

namespace reader_fetch_benchmark {

enum class ReaderFetchLayout { Even, Skewed, SharedGroup };

struct ReaderFetchSegment {
  int32_t nodeIndex;
  int32_t beginParam;
  int32_t endParam;
};

struct ReaderFetchGroup {
  int64_t                         gid;
  int32_t                         paramOffset;
  int32_t                         paramCount;
  std::vector<ReaderFetchSegment> segments;
};

inline const char* layoutName(ReaderFetchLayout layout) {
  switch (layout) {
    case ReaderFetchLayout::Even:
      return "even";
    case ReaderFetchLayout::Skewed:
      return "skewed";
    case ReaderFetchLayout::SharedGroup:
      return "shared_group";
  }
  return "unknown";
}

inline std::vector<ReaderFetchGroup> makeScalePlan(int32_t totalParams, int32_t nodeCount, ReaderFetchLayout layout) {
  int32_t groupCount = 1;
  int32_t skewedCount = totalParams;
  /* (std::min)/(std::max), not std::min/std::max: <windows.h> defines min/max as
   * function-like macros unless NOMINMAX is set, and the expansion of a
   * std::max call is not something the compiler can parse. */
  if (layout == ReaderFetchLayout::Even) {
    groupCount = (std::min)(totalParams, (std::max)(1, nodeCount * 2));
  } else if (layout == ReaderFetchLayout::Skewed) {
    skewedCount = (std::max)(1, totalParams * 9 / 10);
    const int32_t remaining = totalParams - skewedCount;
    groupCount = remaining == 0 ? 1 : 1 + (std::min)(remaining, (std::max)(1, nodeCount * 2 - 1));
  }

  std::vector<int32_t> paramsPerGroup(groupCount, totalParams / groupCount);
  for (int32_t i = 0; i < totalParams % groupCount; ++i) ++paramsPerGroup[i];
  if (layout == ReaderFetchLayout::Skewed) {
    std::fill(paramsPerGroup.begin(), paramsPerGroup.end(), 0);
    paramsPerGroup[0] = skewedCount;
    for (int32_t i = 0; i < totalParams - skewedCount; ++i) {
      ++paramsPerGroup[1 + i % (groupCount - 1)];
    }
  }

  std::vector<ReaderFetchGroup> groups;
  groups.reserve(groupCount);
  int32_t paramOffset = 0;
  for (int32_t groupIndex = 0; groupIndex < groupCount; ++groupIndex) {
    ReaderFetchGroup group = {1000 + groupIndex, paramOffset, paramsPerGroup[groupIndex], {}};
    if (layout == ReaderFetchLayout::SharedGroup) {
      for (int32_t nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
        const int32_t begin = totalParams * nodeIndex / nodeCount;
        const int32_t end = totalParams * (nodeIndex + 1) / nodeCount;
        if (begin != end) group.segments.push_back({nodeIndex, begin, end});
      }
    } else {
      const int32_t nodeIndex = layout == ReaderFetchLayout::Skewed && groupIndex == 0 ? 0 : groupIndex % nodeCount;
      group.segments.push_back({nodeIndex, 0, group.paramCount});
    }
    groups.push_back(std::move(group));
    paramOffset += paramsPerGroup[groupIndex];
  }
  return groups;
}

inline int32_t paramCount(const SStreamRuntimeFuncInfo* runtime) {
  int32_t count = 0;
  int32_t iter = 0;
  auto*   calc = static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashIterate(runtime->pGroupCalcInfos, nullptr, &iter));
  while (calc != nullptr) {
    count += taosArrayGetSize(calc->pParams);
    calc = static_cast<SSTriggerGroupCalcInfo*>(tSimpleHashIterate(runtime->pGroupCalcInfos, calc, &iter));
  }
  return count;
}

inline std::pair<int32_t, int32_t> expectedSelection(const std::vector<ReaderFetchGroup>& groups,
                                                     int32_t nodeIndex) {
  int32_t selectedGroups = 0;
  int32_t selectedParams = 0;
  for (const auto& group : groups) {
    if (std::any_of(group.segments.begin(), group.segments.end(),
                    [nodeIndex](const ReaderFetchSegment& segment) { return segment.nodeIndex == nodeIndex; })) {
      ++selectedGroups;
      selectedParams += group.paramCount;
    }
  }
  return {selectedGroups, selectedParams};
}

inline int64_t elapsedNs(const std::function<void()>& operation) {
  const auto start = std::chrono::steady_clock::now();
  operation();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();
}

inline void emit(const char* stage, const char* path, int32_t params, int32_t nodes, ReaderFetchLayout layout,
                 int32_t ancestorDepth, int32_t nodeId, int32_t sample, int32_t selectedGroups, int32_t selectedParams,
                 int32_t requestBytes, int64_t buildNs, int64_t decodeNs, int64_t admissionNs) {
  std::cout << "READER_FETCH_BENCHMARK {\"stage\":\"" << stage << "\",\"path\":\"" << path << "\",\"params\":" << params
            << ",\"nodes\":" << nodes << ",\"layout\":\"" << layoutName(layout)
            << "\",\"ancestor_depth\":" << ancestorDepth << ",\"node_id\":" << nodeId << ",\"sample\":" << sample
            << ",\"selected_groups\":" << selectedGroups << ",\"selected_params\":" << selectedParams
            << ",\"request_bytes\":" << requestBytes << ",\"build_ns\":" << buildNs << ",\"decode_ns\":" << decodeNs
            << ",\"admission_ns\":" << admissionNs << "}" << std::endl;
}

}  // namespace reader_fetch_benchmark

#endif

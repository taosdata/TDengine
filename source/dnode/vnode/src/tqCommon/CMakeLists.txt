aux_source_directory(. TQ_SOURCE_FILES)
add_library(tqCommon STATIC ${TQ_SOURCE_FILES})
target_include_directories(
        tqCommon
        PUBLIC "../inc"
        PUBLIC "../../inc"
)

target_link_libraries(
        tqCommon
        PRIVATE stream
        PRIVATE common
        PRIVATE transport
        PRIVATE executor
        PRIVATE index
        PRIVATE qcom
        PRIVATE qworker
        PRIVATE sync
        PRIVATE tfs
)

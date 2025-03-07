aux_source_directory(src DND_SUT_SRC)
add_library(sut SHARED ${DND_SUT_SRC})
target_include_directories(
  sut
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
  sut PUBLIC gtest_main dnode nodes planner qcom sdb os util common executor function scalar vnode
)

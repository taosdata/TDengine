# Test CheckpointAction
add_executable(TestCheckpointAction
  TestCheckpointAction.cpp
)

target_link_libraries(TestCheckpointAction
  PRIVATE
    engine
    parameter
    core_create
    core_insert
    core_checkpoint
)

add_test(NAME TestCheckpointAction COMMAND TestCheckpointAction)
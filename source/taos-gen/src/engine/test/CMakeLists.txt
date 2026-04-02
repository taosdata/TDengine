# Test JobScheduler
add_executable(TestJobScheduler
  TestJobScheduler.cpp
)

target_link_libraries(TestJobScheduler
  PRIVATE
    engine
    parameter
)

add_test(NAME TestJobScheduler COMMAND TestJobScheduler)
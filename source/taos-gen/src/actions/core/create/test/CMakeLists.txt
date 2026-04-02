# test CreateDatabaseAction
add_executable(TestCreateDatabaseAction
  TestCreateDatabaseAction.cpp
)
target_link_libraries(TestCreateDatabaseAction
  PRIVATE 
    core_create
    ${TAOS_LIBRARIES}
)
add_test(NAME TestCreateDatabaseAction COMMAND TestCreateDatabaseAction)

# test CreateSuperTableAction
add_executable(TestCreateSuperTableAction
  TestCreateSuperTableAction.cpp
)
target_link_libraries(TestCreateSuperTableAction
  PRIVATE 
    core_create
    ${TAOS_LIBRARIES}
)
add_test(NAME TestCreateSuperTableAction COMMAND TestCreateSuperTableAction)

# test CreateChildTableAction
add_executable(TestCreateChildTableAction
  TestCreateChildTableAction.cpp
)
target_link_libraries(TestCreateChildTableAction
  PRIVATE
    core_create
    ${TAOS_LIBRARIES}
)
add_test(NAME TestCreateChildTableAction COMMAND TestCreateChildTableAction)
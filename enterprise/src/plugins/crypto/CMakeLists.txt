# tcrypto library
# add_library(tcrypto "")
# target_sources(tcrypto
#   PRIVATE
#     tcrypto.cpp
#   PUBLIC
#     ${CMAKE_CURRENT_LIST_DIR}/tcrypto.h
# )
# target_include_directories(tcrypto
#   PUBLIC
#     ${CMAKE_CURRENT_LIST_DIR}
# )

# tests
if(BUILD_TEST)
    add_subdirectory(tests)
endif(BUILD_TEST)


PROJECT(TDengine)

IF (TD_LINUX)
  INCLUDE_DIRECTORIES(. ${TD_SOURCE_DIR}/src/inc ${TD_SOURCE_DIR}/src/client/inc  ${TD_SOURCE_DIR}/inc)
  AUX_SOURCE_DIRECTORY(. SRC)

  add_executable(docs_connect_example "")
  add_executable(docs_create_db_demo "")
  add_executable(docs_insert_data_demo "")
  add_executable(docs_query_data_demo "")
  add_executable(docs_with_reqid_demo "")
  add_executable(docs_sml_insert_demo "")
  add_executable(docs_stmt_insert_demo "")
  add_executable(docs_tmq_demo "")

  target_sources(docs_connect_example
      PRIVATE
      "connect_example.c"
      )

  target_sources(docs_create_db_demo
      PRIVATE
      "create_db_demo.c"
      )

  target_sources(docs_insert_data_demo
      PRIVATE
      "insert_data_demo.c"
      )

  target_sources(docs_query_data_demo
    PRIVATE 
    "query_data_demo.c"
    )
    
  target_sources(docs_with_reqid_demo
    PRIVATE 
    "with_reqid_demo.c"
    )

  target_sources(docs_sml_insert_demo
    PRIVATE 
    "sml_insert_demo.c"
    )

    target_sources(docs_stmt_insert_demo
    PRIVATE 
    "stmt_insert_demo.c"
    )

    target_sources(docs_tmq_demo
    PRIVATE 
    "tmq_demo.c"
    )

  target_link_libraries(docs_connect_example
      taos
      )

  target_link_libraries(docs_create_db_demo
      taos
      )

  target_link_libraries(docs_insert_data_demo
      taos
      )

  target_link_libraries(docs_query_data_demo
      taos
      )
  
  target_link_libraries(docs_with_reqid_demo
      taos
      )

  target_link_libraries(docs_sml_insert_demo
      taos
      )

  target_link_libraries(docs_stmt_insert_demo
      taos
      )

  target_link_libraries(docs_tmq_demo
      taos
      pthread
      )

  SET_TARGET_PROPERTIES(docs_connect_example PROPERTIES OUTPUT_NAME docs_connect_example)
  SET_TARGET_PROPERTIES(docs_create_db_demo PROPERTIES OUTPUT_NAME docs_create_db_demo)
  SET_TARGET_PROPERTIES(docs_insert_data_demo PROPERTIES OUTPUT_NAME docs_insert_data_demo)
  SET_TARGET_PROPERTIES(docs_query_data_demo PROPERTIES OUTPUT_NAME docs_query_data_demo)
  SET_TARGET_PROPERTIES(docs_with_reqid_demo PROPERTIES OUTPUT_NAME docs_with_reqid_demo)
  SET_TARGET_PROPERTIES(docs_sml_insert_demo PROPERTIES OUTPUT_NAME docs_sml_insert_demo)
  SET_TARGET_PROPERTIES(docs_stmt_insert_demo PROPERTIES OUTPUT_NAME docs_stmt_insert_demo)
  SET_TARGET_PROPERTIES(docs_tmq_demo PROPERTIES OUTPUT_NAME docs_tmq_demo)
ENDIF ()
IF (TD_DARWIN)
  INCLUDE_DIRECTORIES(. ${TD_SOURCE_DIR}/src/inc ${TD_SOURCE_DIR}/src/client/inc  ${TD_SOURCE_DIR}/inc)
  AUX_SOURCE_DIRECTORY(. SRC)
ENDIF ()

PROJECT(TDengine)

IF (TD_LINUX)
  INCLUDE_DIRECTORIES(. ${TD_SOURCE_DIR}/src/inc ${TD_SOURCE_DIR}/src/client/inc  ${TD_SOURCE_DIR}/inc)
  AUX_SOURCE_DIRECTORY(. SRC)
  #  ADD_EXECUTABLE(demo apitest.c)
  #TARGET_LINK_LIBRARIES(demo taos_static trpc tutil pthread )
  #ADD_EXECUTABLE(sml schemaless.c)
  #TARGET_LINK_LIBRARIES(sml taos_static trpc tutil pthread )
  #ADD_EXECUTABLE(subscribe subscribe.c)
  #TARGET_LINK_LIBRARIES(subscribe taos_static trpc tutil pthread )
  #ADD_EXECUTABLE(epoll epoll.c)
  #TARGET_LINK_LIBRARIES(epoll taos_static trpc tutil pthread lua)

  add_executable(tmq "")
  add_executable(tmq_taosx "")
  add_executable(stream_demo "")
  add_executable(demoapi "")

  target_sources(tmq_taosx
      PRIVATE
      "tmq_taosx.c"
      )

  target_sources(tmq
      PRIVATE
      "tmq.c"
      )

  target_sources(stream_demo
      PRIVATE
      "stream_demo.c"
      )

  target_sources(demoapi
      PRIVATE
      "demoapi.c"
      )

  target_link_libraries(tmq
      taos_static
      )

  target_link_libraries(tmq_taosx
      taos_static
      )

  target_link_libraries(stream_demo
      taos_static
      )

  target_link_libraries(demoapi
      taos_static
      )

  target_include_directories(tmq
      PUBLIC "${TD_SOURCE_DIR}/include/os"
      PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
      )
  target_include_directories(tmq_taosx
      PUBLIC "${TD_SOURCE_DIR}/include/os"
      PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
      )

  target_include_directories(stream_demo
      PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
      )

  target_include_directories(demoapi
      PUBLIC "${TD_SOURCE_DIR}/include/client"
      PUBLIC "${TD_SOURCE_DIR}/include/os"
      PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
      )

  SET_TARGET_PROPERTIES(tmq PROPERTIES OUTPUT_NAME tmq)
  SET_TARGET_PROPERTIES(tmq_taosx PROPERTIES OUTPUT_NAME tmq_taosx)
  SET_TARGET_PROPERTIES(stream_demo PROPERTIES OUTPUT_NAME stream_demo)
  SET_TARGET_PROPERTIES(demoapi PROPERTIES OUTPUT_NAME demoapi)
ENDIF ()
IF (TD_DARWIN)
  INCLUDE_DIRECTORIES(. ${TD_SOURCE_DIR}/src/inc ${TD_SOURCE_DIR}/src/client/inc  ${TD_SOURCE_DIR}/inc)
  AUX_SOURCE_DIRECTORY(. SRC)
  #ADD_EXECUTABLE(demo demo.c)
  #TARGET_LINK_LIBRARIES(demo taos_static trpc tutil pthread lua)
  #ADD_EXECUTABLE(epoll epoll.c)
  #TARGET_LINK_LIBRARIES(epoll taos_static trpc tutil pthread lua)
ENDIF ()

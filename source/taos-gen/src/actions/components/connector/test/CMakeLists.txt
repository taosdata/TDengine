# Test TDengineConnector
add_executable(TestTDengineConnector
  TestTDengineConnector.cpp
)
target_link_libraries(TestTDengineConnector
  PRIVATE
    components_connector
)
add_test(NAME TestTDengineConnector COMMAND TestTDengineConnector)

# Test NativeConnector
add_executable(TestNativeConnector
  TestNativeConnector.cpp
)
target_link_libraries(TestNativeConnector
  PRIVATE
    components_connector
)
add_test(NAME TestNativeConnector COMMAND TestNativeConnector)

# Test WebsocketConnector
add_executable(TestWebsocketConnector
  TestWebsocketConnector.cpp
)
target_link_libraries(TestWebsocketConnector
  PRIVATE
    components_connector
)
add_test(NAME TestWebsocketConnector COMMAND TestWebsocketConnector)

# Test ConnectionPoolImpl
add_executable(TestConnectionPoolImpl
  TestConnectionPoolImpl.cpp
)
target_link_libraries(TestConnectionPoolImpl
  PRIVATE
    components_connector
)
add_test(NAME TestConnectionPoolImpl COMMAND TestConnectionPoolImpl)

# Test ConnectorSource
add_executable(TestConnectorSource
  TestConnectorSource.cpp
)
target_link_libraries(TestConnectorSource
  PRIVATE
    components_connector
)
add_test(NAME TestConnectorSource COMMAND TestConnectorSource)

# Test PooledConnector
add_executable(TestPooledConnector
  TestPooledConnector.cpp
)
target_link_libraries(TestPooledConnector
  PRIVATE
    components_connector
)
add_test(NAME TestPooledConnector COMMAND TestPooledConnector)
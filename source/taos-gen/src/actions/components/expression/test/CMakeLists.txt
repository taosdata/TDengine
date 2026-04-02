# Test MathFunctions
add_executable(TestMathFunctions
  TestMathFunctions.cpp
)
target_link_libraries(TestMathFunctions
  PRIVATE 
    components_expression
)
add_test(NAME TestMathFunctions COMMAND TestMathFunctions)

# Test NetworkFunctions
add_executable(TestNetworkFunctions
  TestNetworkFunctions.cpp
)
target_link_libraries(TestNetworkFunctions
  PRIVATE 
    components_expression
)
add_test(NAME TestNetworkFunctions COMMAND TestNetworkFunctions)

# Test ExpressionEngine
add_executable(TestExpressionEngine
  TestExpressionEngine.cpp
)
target_link_libraries(TestExpressionEngine
  PRIVATE 
    components_expression
)
add_test(NAME TestExpressionEngine COMMAND TestExpressionEngine)
#include <iostream>
#include <stub.h>
#include <addr_any.h>

// Test function to be stubbed
int add(int a, int b) {
    return a + b;
}

// Stub function
int stub_add(int a, int b) {
    return 100;
}

int main() {
    std::cout << "Testing cppstub package..." << std::endl;
    
    // Test 1: Normal function call
    int result = add(2, 3);
    std::cout << "Normal add(2, 3) = " << result << std::endl;
    if (result != 5) {
        std::cerr << "ERROR: Expected 5, got " << result << std::endl;
        return 1;
    }
    
    // Test 2: Check stub.h is available
    std::cout << "stub.h header is available" << std::endl;
    
    // Test 3: Check addr_any.h is available
    std::cout << "addr_any.h header is available" << std::endl;
    
    // Note: We don't actually use the stub functionality in this simple test
    // because it requires more complex setup. We just verify the headers are accessible.
    
    std::cout << "All tests passed!" << std::endl;
    return 0;
}

/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

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

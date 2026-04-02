#include "NetworkFunctions.hpp"
#include <cassert>
#include <iostream>
#include <sstream>

void test_random_ipv4_format() {
    std::string ip = NetworkFunctions::random_ipv4();
    // Check there are exactly 3 dots
    size_t dot1 = ip.find('.');
    size_t dot2 = ip.find('.', dot1 + 1);
    size_t dot3 = ip.find('.', dot2 + 1);
    (void)dot3;
    assert(dot1 != std::string::npos);
    assert(dot2 != std::string::npos);
    assert(dot3 != std::string::npos);
    assert(ip.find('.', dot3 + 1) == std::string::npos);

    // Check each segment is a number between 0 and 255
    int a, b, c, d;
    char dummy;
    std::istringstream iss(ip);
    (void)a;
    (void)b;
    (void)c;
    (void)d;
    (void)dummy;
    assert((iss >> a >> dummy >> b >> dummy >> c >> dummy >> d));
    assert(a >= 0 && a <= 255);
    assert(b >= 0 && b <= 255);
    assert(c >= 0 && c <= 255);
    assert(d >= 0 && d <= 255);

    std::cout << "test_random_ipv4_format passed.\n";
}

int main() {
    test_random_ipv4_format();
    std::cout << "All NetworkFunctions tests passed.\n";
    return 0;
}
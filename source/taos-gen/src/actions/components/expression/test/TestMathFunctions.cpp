#include "MathFunctions.hpp"
#include <cassert>
#include <iostream>

void test_square_wave_basic() {
    // period = 4, offset = 0
    double min = 1.0, max = 5.0;
    int period = 4, offset = 0;
    (void)min;
    (void)max;
    (void)period;
    (void)offset;
    assert(MathFunctions::square_wave(0, min, max, period, offset) == max);
    assert(MathFunctions::square_wave(1, min, max, period, offset) == max);
    assert(MathFunctions::square_wave(2, min, max, period, offset) == min);
    assert(MathFunctions::square_wave(3, min, max, period, offset) == min);
    std::cout << "test_square_wave_basic passed.\n";
}

void test_square_wave_offset() {
    double min = 2.0, max = 8.0;
    int period = 4, offset = 1;
    (void)min;
    (void)max;
    (void)period;
    (void)offset;
    assert(MathFunctions::square_wave(0, min, max, period, offset) == max);
    assert(MathFunctions::square_wave(1, min, max, period, offset) == min);
    assert(MathFunctions::square_wave(2, min, max, period, offset) == min);
    assert(MathFunctions::square_wave(3, min, max, period, offset) == max);
    std::cout << "test_square_wave_offset passed.\n";
}

void test_square_wave_invalid_period() {
    double min = 3.0, max = 7.0;
    int period = 0, offset = 0;
    (void)min;
    (void)max;
    (void)period;
    (void)offset;
    assert(MathFunctions::square_wave(0, min, max, period, offset) == min);
    std::cout << "test_square_wave_invalid_period passed.\n";
}

int main() {
    test_square_wave_basic();
    test_square_wave_offset();
    test_square_wave_invalid_period();
    std::cout << "All MathFunctions tests passed.\n";
    return 0;
}
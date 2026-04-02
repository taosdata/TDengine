#pragma once
#include <chrono>

class TimeRecorder {
public:
    TimeRecorder() : start_(std::chrono::steady_clock::now()) {}
    
    double elapsed() const {
        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(end - start_).count();
    }

private:
    std::chrono::steady_clock::time_point start_;
};
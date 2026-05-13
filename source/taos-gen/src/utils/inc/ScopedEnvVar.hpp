#pragma once

#include <string>

class ScopedEnvVar {
private:
    std::string name_;
    std::string old_value_;
    bool had_old_value_;

public:
    explicit ScopedEnvVar(const std::string& name, const std::string& new_value);

    ~ScopedEnvVar();

    void restore();

    // Deleted copy semantics
    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;
};

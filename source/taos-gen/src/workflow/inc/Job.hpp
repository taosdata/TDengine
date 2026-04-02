#pragma once

#include "Step.hpp"
#include "PluginExtensions.hpp"
#include "SchemaConfig.hpp"
#include <string>
#include <vector>

struct Job {
    std::string key;                // Job identifier
    std::string name;               // Job name
    std::vector<std::string> needs; // Dependent jobs
    std::vector<Step> steps;        // Steps in the job

    bool need_create = false;
    bool find_create = false;
    SchemaConfig schema;
    PluginExtensions extensions;

    Job() = default;
    Job(const std::string& key,
        const std::string& name,
        const std::vector<std::string>& needs,
        const std::vector<Step>& steps)
        : key(key), name(name), needs(needs), steps(steps) {}
};
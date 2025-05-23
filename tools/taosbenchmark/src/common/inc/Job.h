#ifndef JOB_H
#define JOB_H

#include <string>
#include <vector>
#include "Step.h"

struct Job {
    std::string key;                // 作业标识符
    std::string name;               // 作业名称
    std::vector<std::string> needs; // 依赖的其他作业
    std::vector<Step> steps;        // 作业包含的步骤
};

#endif // JOB_H
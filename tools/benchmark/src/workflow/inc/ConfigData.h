#ifndef CONFIG_DATA_H
#define CONFIG_DATA_H

#include <vector>
#include "GlobalConfig.h"
#include "Job.h"

// 顶层配置
struct ConfigData {
    GlobalConfig global;
    int concurrency = 1;
    std::vector<Job> jobs; // 存储作业列表
};

#endif // CONFIG_DATA_H
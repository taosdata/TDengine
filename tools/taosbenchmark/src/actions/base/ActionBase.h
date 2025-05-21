#pragma once
#include "parameter/ConfigData.h"

class ActionBase {
public:
    virtual ~ActionBase() = default;
    virtual void execute() = 0;
};
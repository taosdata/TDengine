#pragma once
#include <any>

class ActionBase {
public:
    virtual ~ActionBase() = default;
    virtual void execute() = 0;
    virtual void notify(const std::any& payload) {(void)payload;}
};
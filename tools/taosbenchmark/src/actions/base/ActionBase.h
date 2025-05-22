#pragma once

class ActionBase {
public:
    virtual ~ActionBase() = default;
    virtual void execute() = 0;
};
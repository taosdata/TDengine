#pragma once
#include <string>
#include <optional>    
#include <memory>      
#include "ActionBase.hpp" 

struct ActionRegisterInfo {
    std::string action_type;
    std::string description;
    std::optional<std::shared_ptr<ActionBase>> action;
};
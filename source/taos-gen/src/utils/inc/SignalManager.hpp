#pragma once
#include <functional>
#include <map>
#include <vector>
#include <mutex>
#include <optional>
#include <csignal>

namespace SignalManager {

using SignalCallback = std::function<void(int)>;

void register_signal(int signum, SignalCallback cb, bool is_final = false);
void setup();

}
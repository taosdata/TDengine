#pragma once
#include <functional>
#include <map>
#include <vector>
#include <mutex>
#include <optional>
#include <csignal>
#include <atomic>

namespace SignalManager {

using SignalCallback = std::function<void(int)>;

void register_signal(int signum);
void register_signal(int signum, SignalCallback cb, bool is_final = false);
void setup();

// Global interrupt flag — set by signal handler, safe to check from any thread
bool interrupted();

}
#include "SignalManager.hpp"

namespace SignalManager {

struct SignalCallbackList {
    std::vector<SignalCallback> normal_callbacks;
    std::optional<SignalCallback> final_callback;
};

static std::map<int, SignalCallbackList> callbacks;
static std::mutex cb_mutex;

void signal_handler(int signum) {
    std::lock_guard<std::mutex> lock(cb_mutex);
    auto it = callbacks.find(signum);
    if (it != callbacks.end()) {
        for (auto& cb : it->second.normal_callbacks) {
            cb(signum);
        }
        if (it->second.final_callback) {
            it->second.final_callback.value()(signum);
        }
    }
}

void register_signal(int signum, SignalCallback cb, bool is_final) {
    std::lock_guard<std::mutex> lock(cb_mutex);
    if (is_final) {
        callbacks[signum].final_callback = std::move(cb);
    } else {
        callbacks[signum].normal_callbacks.push_back(std::move(cb));
    }
}

void setup() {
#if defined(__unix__) || defined(__APPLE__)
    std::lock_guard<std::mutex> lock(cb_mutex);
    struct sigaction sa {};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    for (const auto& kv : callbacks) {
        ::sigaction(kv.first, &sa, nullptr);
    }
#else
    std::lock_guard<std::mutex> lock(cb_mutex);
    for (const auto& kv : callbacks) {
        std::signal(kv.first, signal_handler);
    }
#endif
}

} // namespace SignalManager
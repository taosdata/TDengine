#include "SignalManager.hpp"

namespace SignalManager {

static std::atomic<bool> g_interrupted{false};

struct SignalCallbackList {
    std::vector<SignalCallback> normal_callbacks;
    std::optional<SignalCallback> final_callback;
};

static std::map<int, SignalCallbackList> callbacks;
static std::mutex cb_mutex;

bool interrupted() {
    return g_interrupted.load(std::memory_order_relaxed);
}

void signal_handler(int signum) {
    g_interrupted.store(true, std::memory_order_relaxed);
    // Use try_lock to avoid deadlock if signal interrupts code holding cb_mutex.
    // If lock fails, callbacks are skipped — the global interrupt flag is already set,
    // which is sufficient for cooperative shutdown.
    std::unique_lock<std::mutex> lock(cb_mutex, std::try_to_lock);
    if (!lock.owns_lock()) return;
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

void register_signal(int signum) {
    std::lock_guard<std::mutex> lock(cb_mutex);
    callbacks[signum]; // Ensure entry exists so setup() installs the handler
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
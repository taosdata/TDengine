#pragma once
#include <any>
#include <string>
#include <unordered_map>

using PluginExtensions = std::unordered_map<std::string, std::any>;

template<typename T>
inline const T* get_plugin_config(const PluginExtensions& exts, const std::string& key) {
    auto it = exts.find(key);
    if (it == exts.end()) return nullptr;
    return std::any_cast<T>(&it->second);
}

template<typename T>
inline T* get_plugin_config_mut(PluginExtensions& exts, const std::string& key) {
    auto it = exts.find(key);
    if (it == exts.end()) return nullptr;
    return std::any_cast<T>(&it->second);
}

template<typename T>
inline void set_plugin_config(PluginExtensions& exts, const std::string& key, T&& value) {
    exts[key] = std::forward<T>(value);
}
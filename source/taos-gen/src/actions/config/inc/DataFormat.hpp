#pragma once
#include "PluginExtensions.hpp"
#include <string>

using FormatOptions = PluginExtensions;

struct DataFormat {
    std::string format_type = "sql";
    bool support_tags = false;
    FormatOptions opts;
};

template<typename T>
inline const T* get_format_opt(const DataFormat& df, const std::string& key) {
    return get_plugin_config<T>(df.opts, key);
}

template<typename T>
inline T* get_format_opt_mut(DataFormat& df, const std::string& key) {
    return get_plugin_config_mut<T>(df.opts, key);
}

template<typename T>
inline void set_format_opt(DataFormat& df, const std::string& key, T&& value) {
    set_plugin_config(df.opts, key, std::forward<T>(value));
}

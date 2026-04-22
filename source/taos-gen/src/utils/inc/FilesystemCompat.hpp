// GCC 7 compatibility: <filesystem> lives under <experimental/filesystem>
// and requires linking with -lstdc++fs.
#ifndef TAOSGEN_FILESYSTEM_COMPAT_HPP
#define TAOSGEN_FILESYSTEM_COMPAT_HPP

#if __has_include(<filesystem>)
  #include <filesystem>
  namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #error "No <filesystem> or <experimental/filesystem> support"
#endif

#endif // TAOSGEN_FILESYSTEM_COMPAT_HPP

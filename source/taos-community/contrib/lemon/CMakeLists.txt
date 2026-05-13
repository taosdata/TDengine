add_executable(lemon lemon.c)
# lemon is a host-side parser generator tool; it must not be built with
# sanitizer instrumentation because devtoolset-7 only ships 32-bit ubsan libs
# and the devtoolset-10 workaround in LIBRARY_PATH does not cover host tools.
if(BUILD_SANITIZER)
  target_compile_options(lemon PRIVATE
    -fno-sanitize=address
    -fno-sanitize=undefined
    -fno-sanitize=float-divide-by-zero
    -fno-sanitize=float-cast-overflow
  )
  target_link_options(lemon PRIVATE
    -fno-sanitize=address
    -fno-sanitize=undefined
    -fno-sanitize=float-divide-by-zero
    -fno-sanitize=float-cast-overflow
  )
endif()


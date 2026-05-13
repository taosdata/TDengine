aux_source_directory(src TDEV_SRC)
add_executable(tdev ${TDEV_SRC})

target_include_directories(tdev PUBLIC inc)

add_library(libargp STATIC
    argp/argp-ba.c
    argp/argp-fs-xinl.c
    argp/argp-pv.c
    argp/argp-eexst.c
    argp/argp-help.c
    argp/argp-pvh.c
    argp/argp-fmtstream.c
    argp/argp-parse.c
    argp/argp-pin.c
    argp/getopt.c
    argp/getopt1.c
    argp/getopt_init.c
)
target_compile_options(libargp PRIVATE
    /W3
    /D_CRT_SECURE_NO_WARNINGS
    /wd4267
    /wd4244
)
set_target_properties(libargp PROPERTIES
    PUBLIC_HEADER argp/argp.h
)

add_executable(argp-test test/argp-test.c)
target_compile_options(argp-test PRIVATE
    /W3
    /D_CRT_SECURE_NO_WARNINGS
    /wd4267
    /wd4244
)
target_include_directories(argp-test PRIVATE argp)
target_link_libraries(argp-test libargp)

# wlr -- Workload Recorder file format library (DS 4.2)
#
# Shared by the in-libtaos recorder and the standalone taosreplay tool. Must not
# depend on client internals: the recording format is the only data protocol
# between them (DS 7.2).

aux_source_directory(src WLR_SRC)
add_library(wlr STATIC ${WLR_SRC})

target_include_directories(
    wlr
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    # PRIVATE, and public taos.h only: the type codes stored in a bind blob are
    # the ones taos.h defines, so they are read from there rather than copied as
    # literals. Copying them is what let type 20 be recorded as DECIMAL128 when
    # taos.h calls it GEOMETRY. Same arrangement taosreplay uses.
    PRIVATE "${TD_SOURCE_DIR}/include/client"
)

# PRIVATE, not PUBLIC: wlr needs os/util to compile, but propagating them to
# every consumer reinserts them into the consumer's link line and shifts the
# order of the existing PUBLIC chain, which breaks scalar -> parser resolution
# in the client test targets. Consumers already link os and util themselves.
target_link_libraries(
    wlr
    PRIVATE os
    PRIVATE util
)

# Signals to wlrFormatTest that the encode/decode entry points are linkable, so
# it runs as a conformance suite against this library instead of only exercising
# its embedded DS 6.15.2 reference model.
target_compile_definitions(wlr PUBLIC WLR_FORMAT_HAVE_IMPL)

if(BUILD_TEST)
    add_subdirectory(test)
endif()

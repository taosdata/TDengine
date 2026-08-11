# rdb-selftest -- does this machine write and read a rocksdb database reliably?
#
# Diagnostic tools, not part of taosd. They link rocksdb but nothing from the
# server, so they can be built and run on an affected host on their own.
#
# See README.md for usage, and for a standalone gcc invocation for hosts where
# the project build is not available.

add_executable(rdbwrite rdbwrite.c)
add_executable(rdbread rdbread.c)

# rdbsst needs rocksdb's C++ headers (SstFileReader, options.h). The bundled
# deps/ archive ships only c.h, so build it only when the full headers are
# available -- i.e. when rocksdb was built from source (BUILD_CONTRIB=ON).
# rdbwrite and rdbread use only the C API and build against either.
set(_RDBSST_OK FALSE)
if(TD_ROCKSDB_USE_EXTERNAL)
    set(_RDBSST_OK TRUE)
    add_executable(rdbsst rdbsst.cc)
else()
    message(STATUS "rdb-selftest: skipping rdbsst, rocksdb C++ headers not available "
                   "(configure with BUILD_CONTRIB=ON to enable it)")
endif()

set(_RDB_TARGETS rdbwrite rdbread)
if(_RDBSST_OK)
    list(APPEND _RDB_TARGETS rdbsst)
endif()

foreach(_tgt ${_RDB_TARGETS})
    DEP_td_rocksdb(${_tgt})
    target_include_directories(${_tgt} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
    target_compile_definitions(${_tgt} PRIVATE _GNU_SOURCE)
    target_link_libraries(${_tgt} PRIVATE pthread m)
endforeach()

# rdbwrite and rdbread are C, but rocksdb is C++, so its archive needs the C++
# runtime and the compression libraries it was built against. Linking through
# the C driver would otherwise fail on std::__throw_length_error and friends.
set_target_properties(rdbwrite rdbread PROPERTIES LINKER_LANGUAGE CXX)
foreach(_tgt ${_RDB_TARGETS})
    target_link_libraries(${_tgt} PRIVATE z dl)
endforeach()

# rocksdb's own C++ headers need C++17: slice.h uses std::string_view and
# wide_columns.h uses std::make_from_tuple.
if(_RDBSST_OK)
    set_target_properties(rdbsst PROPERTIES
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
    )
endif()

# The project adds -Wno-stringop-overread, which only exists from GCC 11. On an
# older compiler it is an unrecognized option, and with -Werror that stops the
# build. Drop it for this target rather than requiring a newer toolchain, since
# these tools have to build on whatever the affected host has.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
    foreach(_tgt ${_RDB_TARGETS})
        get_target_property(_opts ${_tgt} COMPILE_OPTIONS)
        if(_opts)
            list(REMOVE_ITEM _opts "-Wno-stringop-overread")
            set_target_properties(${_tgt} PROPERTIES COMPILE_OPTIONS "${_opts}")
        endif()
    endforeach()
    string(REPLACE "-Wno-stringop-overread" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
    string(REPLACE "-Wno-stringop-overread" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
endif()

# Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

aux_source_directory(src WRAPPER_SRC)

# The engine helpers this dispatcher needs are compiled in instead of being imported
# from the driver (see inc/wrapperLite.h).  Those three files define 15 names the engine's
# os/util also define -- taosPrintLog, taosRealPath, taosLoadDll, taosGetErrno,
# atomic_*, ... -- so they belong to this library alone; the list below exists to give
# them hidden visibility, so that a consumer which also loads the driver keeps
# resolving those names to the engine's copies.
#
# (This used to be a shared/static split, with src/wrapperLiteStatic.c forwarding the
# same tdLite* API to the engine for a taos_static archive.  That archive is gone:
# the engine's static client, for a consumer that wants no dispatcher and no .so, is
# taosnative_static.)
set(TD_TAOS_LITE_SRC
    src/wrapperLiteErr.c
    src/wrapperLiteOs.c
    src/wrapperLiteLog.c
)

if(TD_WINDOWS)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/../src/taos.rc.in "${CMAKE_CURRENT_BINARY_DIR}/taos.rc")
    add_library(${TAOS_LIB} SHARED ${WRAPPER_SRC} "${CMAKE_CURRENT_BINARY_DIR}/taos.rc")
else()
    add_library(${TAOS_LIB} SHARED ${WRAPPER_SRC})
endif()

# The self-contained helpers must stay invisible outside the shared object:
# exporting them would let them interpose the driver's copies for every consumer
# that loads both libraries, and a consumer reaching one of these helpers would then
# get wrapper state instead of the engine's.  Hidden visibility keeps the intra-DSO
# references working (other TUs in this library still bind to these definitions)
# while removing them from the export table.
# Windows needs nothing here: it exports nothing without __declspec, and MSVC has
# no -fvisibility (it reports D9002 for it).
if(NOT WIN32)
    set_source_files_properties(${TD_TAOS_LITE_SRC} PROPERTIES COMPILE_OPTIONS "-fvisibility=hidden")
endif()

# Version string for the local fallback (taos_get_client_info() and the JNI's
# getServerVersion answer from here when the driver has no taos_get_client_info).
# Assembled from the same two CMake variables as source/util/src/version.c.in so
# this library's answer is the engine's td_version by construction; the unit test
# fails loudly if this define is ever dropped.
# TD_TAOS_LITE_SHARED switches on the memory macro overrides in inc/wrapperLiteMem.h,
# which are what keep this library independent of the engine's memory pool.
target_compile_definitions(${TAOS_LIB} PRIVATE
    TD_TAOS_LITE_SHARED
    "TD_LITE_VERSION=\"${BUILD_VER_NUMBER}.${TD_EDITION}\"")

# NOTE: no version script for libtaos.so.  It is a driver dispatcher only, so it
# neither needs to export engine symbols nor may it embed them: the public C API
# is re-exported through function pointers resolved from the driver it
# dlopen()s, and the engine itself lives in libtaosnative.so (see
# source/client/CMakeLists.txt and taosnative.ver for its export whitelist).
#
# The earlier attempt hid libtaos.so's *own* statically linked copy of the
# engine.  That split the global state (tsNumOfCores stayed 0 in libtaos.so
# while the driver initialised its own copy) and broke config init with
# TSDB_CODE_OUT_OF_RANGE in every taos/taosdump/taosBenchmark process.  That copy
# no longer exists -- which is what makes hiding symbols in libtaosnative.so
# safe.

if(TD_DARWIN)
    target_compile_options(${TAOS_LIB} PRIVATE -Wno-error=deprecated-non-prototype)
    # Suppress ld64 duplicate static library warnings from deep transitive deps.
    include(CheckLinkerFlag)
    check_linker_flag(C "-Wl,-no_warn_duplicate_libraries" HAVE_NO_WARN_DUP_LIBS)
    if(HAVE_NO_WARN_DUP_LIBS)
      target_link_options(${TAOS_LIB} PRIVATE -Wl,-no_warn_duplicate_libraries)
    endif()
    set_target_properties(${TAOS_LIB} PROPERTIES INSTALL_NAME_DIR "@rpath")
    taos_configure_macos_client_rpath(${TAOS_LIB})
endif()

# jni include
INCLUDE_DIRECTORIES(jni)
if(TD_WINDOWS)
    INCLUDE_DIRECTORIES(jni/windows)
    INCLUDE_DIRECTORIES(jni/windows/win32)
    INCLUDE_DIRECTORIES(jni/windows/win32/bridge)
else()
    INCLUDE_DIRECTORIES(jni/linux)
endif()

target_include_directories(
    ${TAOS_LIB}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

# The engine headers are needed for compilation only (jniCommon.h includes
# ttypes.h, ...).  They used to arrive as a side effect of linking os/util; now
# that the wrapper no longer links them, the include paths are listed directly.
target_include_directories(${TAOS_LIB} PRIVATE
    "${TD_SOURCE_DIR}/include"
    "${TD_SOURCE_DIR}/include/os"
    "${TD_SOURCE_DIR}/include/util"
    "${TD_SOURCE_DIR}/include/common"
    # taosNativeApi.h: the driver's API table, which the dispatcher reads instead
    # of dlsym()ing the public names (see source/client/src/taosApiTable.c).
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

# libtaos.so is a driver dispatcher: it needs the engine's helpers (config dir,
# memory pool, logging, error strings...) but not a second copy of the engine.
# Linking the driver library keeps those symbols resolvable at load time (data
# relocations are eager) while removing the ~9 MB of duplicated engine statics.
#
# The path form is deliberate: naming the target would also inherit its
# INTERFACE_LINK_LIBRARIES (os/util/... static archives).  The linker satisfies
# an undefined reference from the first definition it meets, so an archive on
# this line would be preferred over libtaosnative.so and would silently
# re-embed a second copy of the engine -- including its global state, which is
# exactly the split-state bug that broke config init before.
# The thin dispatcher covers Linux and macOS; Windows keeps the previous fat one
# (it resolves every symbol at link time against import libraries, so there is no
# lazy binding to lean on and no version-script equivalent with the same effect).
# macOS has no version script either -- the equivalent (-exported_symbols_list)
# is a separately generated artifact that still needs a macOS validation run -- so
# there the dispatcher is thin but the engine symbols are not hidden yet.
if(TD_LINUX OR TD_DARWIN)
    add_dependencies(${TAOS_LIB} ${TAOS_NATIVE_LIB})
    if(TD_LINUX)
        # libtaos.so now carries its own implementations of the engine helpers it
        # used to import (src/wrapperLite*.c, built with -fvisibility=hidden so they
        # are not exported and cannot interpose the driver).  The driver therefore
        # no longer has to be in DT_NEEDED: a WebSocket-only deployment needs just
        # libtaos.so and libtaosws.so.  taosDriverInit() keeps dlopen()ing the
        # driver at run time (colocated $ORIGIN, develop path, then the system
        # search path -- see wrapperDriver.c), and add_dependencies only fixes the
        # build order.
        #
        # The rpath/--disable-new-dtags block below is kept even though it is now
        # inert for this library: it documented why the *linked* driver had to
        # resolve to $ORIGIN (and why sanitizers must see a normalized module
        # name).  It becomes load-bearing again the moment the driver is linked
        # back in, e.g. on macOS.
        #
        # The local implementations call pthread_once/pthread_mutex and
        # dlopen/dlsym.  Those live in libc from glibc 2.34 on, but the core image
        # (GCC 7, older glibc) needs them named explicitly.
        set(THREADS_PREFER_PTHREAD_FLAG ON)
        find_package(Threads REQUIRED)
        target_link_libraries(${TAOS_LIB} PRIVATE Threads::Threads ${CMAKE_DL_LIBS})
        # The driver is a hard dependency of the dispatcher, not something only our
        # own dlopen() pulls in: the dispatcher reads driver globals (gMemPoolHandle,
        # configDir, jniDebugFlag, the atomics), and data relocations are eager, so
        # the dependency has to be resolvable the moment libtaos.so is loaded.
        # Whoever resolves it also records its module name, and that recorded name is
        # what sanitizers print in leak reports -- the CI ASAN check then matches
        # frames by path, so a name built from an LD_LIBRARY_PATH entry (which the
        # test harness keeps in an unnormalized "<test dir>/../../.." form) makes
        # every frame of the driver look like TDengine source.
        # LD_LIBRARY_PATH wins over DT_RUNPATH, so ask for the old DT_RPATH form:
        # $ORIGIN is the directory holding libtaos.so, where the driver always sits
        # (build tree and install tree alike), and it is searched first.
        target_link_options(${TAOS_LIB} PRIVATE
            "LINKER:--disable-new-dtags")
        # Build and install rpath both point at $ORIGIN, where the driver sits next
        # to the dispatcher; CMake escapes the token for us ($ORIGIN in a link
        # option would reach the linker as $$ORIGIN and never match).
        set_target_properties(${TAOS_LIB} PROPERTIES
            BUILD_RPATH "$ORIGIN"
            INSTALL_RPATH "$ORIGIN"
            BUILD_RPATH_USE_ORIGIN TRUE)
    endif()
    if(TD_DARWIN)
        # macOS used to link the driver (and re-export it) so that consumers would
        # see the engine symbols.  They name libtaosnative.dylib themselves now, the
        # same way they do on Linux, which keeps the driver out of this library's
        # load commands -- the property the whole thin dispatcher exists for.  The
        # driver's own export table still lists the public API (the Mach-O
        # equivalent of taosnative.ver is a separate artifact), so every consumer
        # must keep naming the dispatcher first on its link line.
        #
        # taosConfigureMacosClientRpath() above still sets the loader path for the
        # driver this library dlopen()s at run time.
    endif()
else()
    # Windows used to fold os/util (and their transitive dependencies) into
    # taos.dll.  It builds the same self-contained helpers as the other two
    # platforms now, so the engine archives are only a *usage requirement* here:
    # consumers link them on their own line, as they were already built before,
    # and taos.dll itself neither contains engine code nor needs taosnative.dll.
    #
    # The local helpers need pthread-win32 for pthread_mutex/pthread_once;
    # Threads::Threads is empty under MSVC.
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
    target_link_libraries(${TAOS_LIB} PRIVATE Threads::Threads ${CMAKE_DL_LIBS})
    if(ext_pthread_libs)
        target_link_libraries(${TAOS_LIB} PRIVATE ${ext_pthread_libs})
    endif()
    target_link_libraries(${TAOS_LIB} INTERFACE os util)
endif()

# Consumers of the dispatcher use engine internals that live only in
# libtaosnative.so.  They can link it explicitly now: the driver no longer exports
# the public API (it hands those to the dispatcher through tdNativeApiTable), so
# it cannot compete with libtaos.so in symbol lookup no matter which library the
# linker happens to place first.  That also means no linker-specific option is
# needed here -- in particular not --copy-dt-needed-entries, which mold rejects.
# See source/client/CMakeLists.txt for the export rules.

# Consumers of libtaos.so are not only pure API users: the CLI tools (and the
# compat modules they build) include engine headers directly.  Those paths used
# to arrive as a side effect of target_link_libraries(taos PUBLIC os util), and
# dropping them broke compiles such as taosdump's compatAvro (os.h) and
# taosBenchmark (pub.h, reached through util -> TSZ).
#
# Re-advertise the same usage requirements, but only the include directories:
# naming a target in target_link_libraries would put its static archive back on
# every consumer's link line, where it can shadow libtaosnative.so.  Walking
# INTERFACE_LINK_LIBRARIES keeps the set in sync when os/util gain dependencies.
function(td_taos_collect_include_dirs out_var)
    set(_pending ${ARGN})
    set(_seen)
    set(_dirs)
    while(_pending)
        list(POP_FRONT _pending _dep)
        if(NOT TARGET "${_dep}" OR _dep IN_LIST _seen)
            continue()
        endif()
        list(APPEND _seen "${_dep}")
        get_target_property(_incs "${_dep}" INTERFACE_INCLUDE_DIRECTORIES)
        if(_incs)
            list(APPEND _dirs ${_incs})
        endif()
        get_target_property(_sys_incs "${_dep}" INTERFACE_SYSTEM_INCLUDE_DIRECTORIES)
        if(_sys_incs)
            list(APPEND _dirs ${_sys_incs})
        endif()
        get_target_property(_next "${_dep}" INTERFACE_LINK_LIBRARIES)
        if(_next)
            foreach(_n ${_next})
                if(TARGET "${_n}")
                    list(APPEND _pending "${_n}")
                endif()
            endforeach()
        endif()
    endwhile()
    if(_dirs)
        list(REMOVE_DUPLICATES _dirs)
    endif()
    set(${out_var} "${_dirs}" PARENT_SCOPE)
endfunction()

# Not needed on Windows: `INTERFACE os util` above already advertises these paths.
td_taos_collect_include_dirs(TAOS_ENGINE_INCLUDES os util)
# The dispatcher's own translation units include engine headers for compilation
# only (jniCommon.h -> ttypes.h, taosNativeApi.h), and on Windows those headers
# pull further ones from the bundled externals (os.h -> regex.h, pthread.h).  Those
# paths used to arrive through `PUBLIC os util`, which the library no longer links.
target_include_directories(${TAOS_LIB} PRIVATE ${TAOS_ENGINE_INCLUDES})
if(TD_LINUX OR TD_DARWIN)
    target_include_directories(${TAOS_LIB} INTERFACE ${TAOS_ENGINE_INCLUDES})
endif()

# if(${BUILD_TEST})
# ADD_SUBDIRECTORY(test)
# endif()

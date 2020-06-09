# Remove CMake generated temporary files
set (cmake_generated
    ${CMAKE_BINARY_DIR}/ALL_BUILD.vcxproj
    ${CMAKE_BINARY_DIR}/ALL_BUILD.vcxproj.filters
    ${CMAKE_BINARY_DIR}/CMakeCache.txt
    ${CMAKE_BINARY_DIR}/CMakeFiles
    ${CMAKE_BINARY_DIR}/CTestTestfile.cmake
    ${CMAKE_BINARY_DIR}/Continuous.vcxproj
    ${CMAKE_BINARY_DIR}/Continuous.vcxproj.filters
    ${CMAKE_BINARY_DIR}/DartConfiguration.tcl
    ${CMAKE_BINARY_DIR}/Debug
    ${CMAKE_BINARY_DIR}/Experimental.vcxproj
    ${CMAKE_BINARY_DIR}/Experimental.vcxproj.filters
    ${CMAKE_BINARY_DIR}/INSTALL.vcxproj
    ${CMAKE_BINARY_DIR}/INSTALL.vcxproj.filters
    ${CMAKE_BINARY_DIR}/Makefile
    ${CMAKE_BINARY_DIR}/Nightly.vcxproj
    ${CMAKE_BINARY_DIR}/Nightly.vcxproj.filters
    ${CMAKE_BINARY_DIR}/NightlyMemoryCheck.vcxproj
    ${CMAKE_BINARY_DIR}/NightlyMemoryCheck.vcxproj.filters
    ${CMAKE_BINARY_DIR}/RUN_TESTS.vcxproj
    ${CMAKE_BINARY_DIR}/RUN_TESTS.vcxproj.filters
    ${CMAKE_BINARY_DIR}/Testing
    ${CMAKE_BINARY_DIR}/Win32
    ${CMAKE_BINARY_DIR}/ZERO_CHECK.vcxproj
    ${CMAKE_BINARY_DIR}/ZERO_CHECK.vcxproj.filters
    ${CMAKE_BINARY_DIR}/check.vcxproj
    ${CMAKE_BINARY_DIR}/check.vcxproj.filters
    ${CMAKE_BINARY_DIR}/cmake_install.cmake
    ${CMAKE_BINARY_DIR}/dirent.sln
    ${CMAKE_BINARY_DIR}/distclean.vcxproj
    ${CMAKE_BINARY_DIR}/distclean.vcxproj.filters
    ${CMAKE_BINARY_DIR}/find
    ${CMAKE_BINARY_DIR}/find.dir
    ${CMAKE_BINARY_DIR}/find.vcxproj
    ${CMAKE_BINARY_DIR}/find.vcxproj.filters
    ${CMAKE_BINARY_DIR}/locate
    ${CMAKE_BINARY_DIR}/locate.dir
    ${CMAKE_BINARY_DIR}/locate.vcxproj
    ${CMAKE_BINARY_DIR}/locate.vcxproj.filters
    ${CMAKE_BINARY_DIR}/ls
    ${CMAKE_BINARY_DIR}/ls.dir
    ${CMAKE_BINARY_DIR}/ls.vcxproj
    ${CMAKE_BINARY_DIR}/ls.vcxproj.filters
    ${CMAKE_BINARY_DIR}/t-compile
    ${CMAKE_BINARY_DIR}/t-compile.dir
    ${CMAKE_BINARY_DIR}/t-compile.vcxproj
    ${CMAKE_BINARY_DIR}/t-compile.vcxproj.filters
    ${CMAKE_BINARY_DIR}/t-dirent
    ${CMAKE_BINARY_DIR}/t-dirent.dir
    ${CMAKE_BINARY_DIR}/t-dirent.vcxproj
    ${CMAKE_BINARY_DIR}/t-dirent.vcxproj.filters
    ${CMAKE_BINARY_DIR}/updatedb
    ${CMAKE_BINARY_DIR}/updatedb.dir
    ${CMAKE_BINARY_DIR}/updatedb.vcxproj
    ${CMAKE_BINARY_DIR}/updatedb.vcxproj.filters
)
foreach (file ${cmake_generated})
    if (EXISTS ${file})
        file (REMOVE_RECURSE ${file})
    endif()
endforeach (file)

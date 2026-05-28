if(NOT DEFINED MSVCREGEX_ROOT OR MSVCREGEX_ROOT STREQUAL "")
    message(FATAL_ERROR "[ext_msvcregex] MSVCREGEX_ROOT is required")
endif()

if(EXISTS "${MSVCREGEX_ROOT}/NMakefile")
    message(STATUS "[ext_msvcregex] using extracted root layout")
elseif(EXISTS "${MSVCREGEX_ROOT}/libgnurx-msvc-master/NMakefile")
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E copy_directory
                "${MSVCREGEX_ROOT}/libgnurx-msvc-master"
                "${MSVCREGEX_ROOT}"
        RESULT_VARIABLE copy_result
    )
    if(NOT copy_result EQUAL 0)
        message(FATAL_ERROR "[ext_msvcregex] failed to normalize legacy archive layout")
    endif()
else()
    message(FATAL_ERROR "[ext_msvcregex] NMakefile not found in extracted archive")
endif()

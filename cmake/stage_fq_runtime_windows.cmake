# Staging helper for the Windows federated-query dependency package.
#
# Invoked at build time (`cmake -P`) so DLLs produced by ExternalProjects
# (e.g. MariaDB Connector/C) exist by the time the copy runs.  Only the named
# runtime DLLs are copied into OUT_DIR.
#
# Inputs:
#   OUT_DIR       destination directory (build/bin next to taosd)
#   SEARCH_DIRS   semicolon-separated directories to scan
#   NAME_PATTERNS semicolon-separated regexes matched against the file name

if(NOT DEFINED OUT_DIR OR NOT DEFINED SEARCH_DIRS)
  return()
endif()

file(MAKE_DIRECTORY "${OUT_DIR}")
set(_copied 0)
foreach(_dir IN LISTS SEARCH_DIRS)
  if(NOT IS_DIRECTORY "${_dir}")
    continue()
  endif()
  file(GLOB _files "${_dir}/*.dll")
  foreach(_f IN LISTS _files)
    get_filename_component(_name "${_f}" NAME)
    foreach(_pat IN LISTS NAME_PATTERNS)
      if(_name MATCHES "${_pat}")
        execute_process(
          COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${_f}" "${OUT_DIR}/${_name}")
        message(STATUS "[FQ] staged ${_name} -> ${OUT_DIR}")
        math(EXPR _copied "${_copied} + 1")
        break()
      endif()
    endforeach()
  endforeach()
endforeach()
if(_copied EQUAL 0)
  message(STATUS "[FQ] no provider runtime DLLs found in: ${SEARCH_DIRS}")
endif()

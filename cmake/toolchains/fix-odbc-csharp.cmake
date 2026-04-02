# Temporarily disable C# test subdirectory in taos-connector-odbc
# because CSharp language support requires VS .NET workload which may not be installed.
file(READ "${INPUT_FILE}" _content)
string(FIND "${_content}" "# disabled by build system" _already_patched)
if(_already_patched EQUAL -1)
  string(REPLACE "add_subdirectory(cs)" "# add_subdirectory(cs) # disabled by build system" _content "${_content}")
  file(WRITE "${INPUT_FILE}" "${_content}")
endif()

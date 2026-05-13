# Set up colored output
macro(setup_colored_output)
    string(ASCII 27 Esc)
    set(ColorReset  "${Esc}[m")
    set(Red         "${Esc}[31m")
    set(Green       "${Esc}[32m")
    set(Yellow      "${Esc}[33m")
    set(Blue        "${Esc}[34m")
endmacro()

# Version check helper
macro(check_program_version name version prog ver_arg ver_regex var_type)
  find_program(${name} NO_CACHE NAMES ${prog})
  if (${name})
    set(${name}_NAME ${prog})
    if ("${var_type}" STREQUAL "OUTPUT_VARIABLE")
      execute_process(COMMAND ${prog} ${ver_arg} ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ${name}_VERSION)
    else ()
      execute_process(COMMAND ${prog} ${ver_arg} OUTPUT_QUIET ERROR_STRIP_TRAILING_WHITESPACE ERROR_VARIABLE ${name}_VERSION)
    endif ()
    if(NOT "${${name}_VERSION}" STREQUAL "")
      string(REGEX REPLACE ${ver_regex} "\\1" ${name}_VERSION_ONLY ${${name}_VERSION})

      if(NOT "${${name}_VERSION_ONLY}" STREQUAL "")
        if (${${name}_VERSION_ONLY} VERSION_GREATER_EQUAL ${version})
          set(HAVE_${name} ON)
          message(STATUS "${Green}${${name}_VERSION} found: `${${name}}`, please be noted, ${prog} v${version} and above are expected compatible${ColorReset}")
        endif()
      endif()
    endif()
  endif ()
endmacro()

# Check requirements
macro(check_requirements)
  ## check `git`
  check_program_version(GIT 2.0.0 git "--version" "git version (.*)" OUTPUT_VARIABLE)
  if (NOT HAVE_GIT)
    message(STATUS "${Yellow}"
                   "`git 2.0.0 or above` not found, git info is unavailable"
                   "${ColorReset}")
  endif ()
endmacro()
Win32 C++ builds of Avro-C
*****************************

April 2, 2012

These instructions describe some of the changes required to allow
Avro-C to compile under the Microsoft Visual C++ 2008 compiler, as
well as some limitations of the Windows build.

Status of the Windows Build:
****************************

The Windows build of Avro-C compiles under Microsoft Visual C++
2008. You can use C-Make to create the solution file (AvroC.sln) for
the build.  The solution file contains projects for the build as well
as projects for the tests. All the tests are run and pass when the
project RUN_TESTS is built from within MS Visual C++ 2008.

Limitations of Windows Build:
******************************

1. The Windows build of Avro-C is compiled using Microsoft's C++
   compiler and not Microsoft's C compiler. This is done using the /TP
   flag in the MSVC++ compiler. This flag is automatically set using
   CMakeLists.txt

   The reason to compile Win32 under C++ instead of C is that there
   are lots of places where variables are declared after statements,
   the Microsoft C compiler does not support declarations after
   statements. It is possible, that if all the declarations after
   statements were removed, that Avro-C would compile under
   Microsoft's C compiler also. I have not tried this.

2. The shared library, i.e. DLL, for avro has not been built. There
   are instructions on how to build DLLs using CMake at
   https://www.cmake.org/Wiki/BuildingWinDLL

3. Currently avropipe.c and avromod.c do not compile under Windows. In
   order for them to compile we would have to either port getopt() to
   Windows, or remove their dependency on getopt().

4. Windows cannot run version.sh to generate the version
   number. Currently, LIBAVRO_VERSION is hard coded to "22:0:0" for
   the Windows build, in the top level CMakeLists.txt.

5. Found two bugs related to improper return values under error
   conditions in Avro. These bugs were marked with #warnings in the
   code.


Instructions for Maintenance
*****************************

1. Instructions to check name mangling in Visual C++:

    In a DOS prompt go to "C:\Program Files(x86)\Microsoft Visual Studio 9.0\VC\"
    Run the program vcvarsall.bat . This will set up environment variables.

    Now go to the avro_c\build_win32\src\Debug\ directory.
    Run the command

    dumpbin /ALL avro.lib > tmp.txt

    View tmp.txt in your favorite editor. This will allow you to see
    which names are mangled and which names are not mangled.

    Every header file should start with

    #ifndef HEADER_FILE_NAME_H
    #define HEADER_FILE_NAME_H
    #ifdef __cplusplus
    extern "C" {
    #define CLOSE_EXTERN }
    #else
    #define CLOSE_EXTERN
    #endif

    and end with

    CLOSE_EXTERN
    #endif /* HEADER_FILE_NAME_H */

    This will ensure that all exported (public) functions are mangled
    using C name mangling instead of C++ name mangling.

2. All file I/O operations should have "b" for binary in the fopen
   statements. Otherwise Windows will replace LF with CRLF in binary
   data.

3. Windows does not allow writing to a file with exclusive access
   using the mode "wbx". Use the non-exclusive mode "wb" instead.

4. If the hashtable from st.c is used, the functions in the struct
   st_hash_type should be cast to HASH_FUNCTION_CAST.

5. Do not use "%zu" to print size_t. Use '"%" PRIsz' without the
   single quotes, instead.

6. MS Visual C++ 2008 does not properly expand variadic preprocessor
   macros by default. It is possible to "trick" MSVC++ to properly
   expand variadic preprocessor macros by using an extra (dummy)
   preprocessor macro, whose only purpose is to properly expand its
   input. This method is described here:

   https://stackoverflow.com/questions/2575864/the-problem-about-different-treatment-to-va-args-when-using-vs-2008-and-gcc
   See the solution described by monkeyman.

   This method is used in the macro expand_args(...) in test_avro_values.c.

7. Web site describing how to debug macro expansion in Visual C++:
   http://fneep.fiffa.net/?p=66

8. Sometimes it is necessary to declare a struct at the top of a file
   and define it at the bottom of a file. An example is
   AVRO_DATUM_VALUE_CLASS in src/datum_value.c. A C++ compiler will
   complain that the struct is defined twice. To avoid this, declare
   the struct with the modifier "extern" at the top of the file, and
   then define it at the bottom of the file. Note that it cannot be
   defined as "static" because Win32 does not like an extern struct
   mapping to a static struct.

9. Use __FUNCTION__ instead of __func__ for generating the function
   name.

10. All variables need to be explicitly cast when calling functions
    with differing function signatures

11. All pointers need to be explicitly cast when assigning to other
    pointers of a different type.

12. Do not perform pointer arithmetic on void * pointers. Cast the
    pointers to char * before performing pointer arithmetic.

13. The executable names of any examples and tests need to be set
    explicitly to include the "Debug/" directory in the path, and the
    ".exe" ending. See the CMakeLists.txt in the examples and the
    tests directory to see how this is done.

14. Do not include the headers inttypes.h or unistd.h or
    stdint.h. Instead include avro/platform.h in your C files.

15. Do not include dirent.h in your tests. When _WIN32 is defined
    include msdirent.h. See example in test_avro_schema.c.

16. If _WIN32 is defined, define snprintf() to_snprintf(), which MS
    Visual C++ recognizes.

17. MSVC++ does not recognize strtoll(). Define it to _strtoi64()
    instead.

18. Old-style C function declarations are not allowed in C++. See the
    changes in st.c and st.h -- which were converted to new-style
    function declarations.

19. Structures cannot be initialized using the .element notation for
    Win32. For example if we have a struct test_t:
        typedef struct
        {
           int a;
           int b;
        } test_t;
    Then we can initialize the struct using the syntax:
        test_t t1 = { 0, 0 };
    But we cannot use the syntax:
        test_t t2 = { .a = 0, . b = 0 };
    because Win32 does not support it.

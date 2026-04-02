###############################################################################
# MIT License
#
# Copyright (c) 2022-2023 freemine <freemine@yeah.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

project(cs_test LANGUAGES CSharp)

include(CSharpUtilities)

add_executable(cs_test
    cs_test.cs)

LIST(APPEND VS_DOTNET_REFERENCES "System")
LIST(APPEND VS_DOTNET_REFERENCES "System.Data")
LIST(APPEND VS_DOTNET_REFERENCES "System.Data.Odbc")

if (HAVE_TAOS)
    LIST(APPEND CS_TEST_MACROS "HAVE_TAOS")
endif()
if (HAVE_TAOSWS)
    LIST(APPEND CS_TEST_MACROS "HAVE_TAOSWS")
endif()

set_target_properties(cs_test PROPERTIES
    VS_DOTNET_REFERENCES "${VS_DOTNET_REFERENCES}"
    COMPILE_DEFINITIONS "${CS_TEST_MACROS}")

add_test(NAME cs_test
    COMMAND cs_test)

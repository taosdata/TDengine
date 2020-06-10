# Dirent
Dirent is a C/C++ programming interface that allows programmers to retrieve
information about files and directories under Linux/UNIX.  This project
provides Linux compatible Dirent interface for Microsoft Windows.


# Installation

Download the latest Dirent installation package from
[GitHub](https://github.com/tronkko/dirent/releases) and
unpack the installation file with 7-zip, for example.  The installation
package contains dirent.h file as well as a few example programs and
tests.


## Install Dirent for All Programs

To make dirent.h available for all C/C++ programs, simply copy the
``include/dirent.h`` file to the system include directory.  System include
directory contains header files such as assert.h and windows.h.  In Visual
Studio 2008, for example, the system include may be found at
``C:\Program Files\Microsoft Visual Studio 9.0\VC\include``.

Everything you need is included in the single dirent.h file, and you can
start using Dirent immediately -- there is no need to add files to your
Visual Studio project.


## Embed Dirent into Your Own Project

If you wish to distribute dirent.h alongside with your own source code, then
copy ``include/dirent.h`` file to a new sub-directory within your project and
add that directory to include path on Windows while omitting the directory
under Linux/UNIX.  This allows your project to be compiled against native
dirent.h on Linux/UNIX while substituting the functionality on Microsoft
Windows.


## Examples

The installation package contains four example programs:

Program  | Purpose
-------- | -----------------------------------------------------------------
ls       | List files in a directory, e.g. ls "c:\Program Files"
find     | Find files in subdirectories, e.g. find "c:\Program Files\CMake"
updatedb | Build database of files in a drive, e.g. updatedb c:\
locate   | Locate a file from database, e.g. locate notepad.exe

To build the example programs, first install [CMake](https://cmake.org/).
Then, with CMake installed, open command prompt and create a temporary
directory ``c:\temp\dirent`` for the build files as

```
c:\
mkdir temp
mkdir temp\dirent
cd temp\dirent
```

Generate build files as

```
cmake d:\dirent
```

where ``d:\dirent`` is the root directory of the Dirent package (containing
this README.md and LICENSE file).

Once CMake is finished, open Visual Studio, load the generated dirent.sln file
from the build directory and build the solution.  Once the build completes, run
the example programs from the command prompt as

```
cd Debug
ls .
find .
updatedb c:\
locate cmd.exe
```


# Copying

Dirent may be freely distributed under the MIT license.  See the
[LICENSE](LICENSE) file for details.


# Alternatives to Dirent

I ported Dirent to Microsoft Windows in 1998 when only a few alternatives
were available.  However, the situation has changed since then and nowadays
both [Cygwin](http://www.cygwin.com) and [MingW](http://www.mingw.org)
allow you to compile a great number of UNIX programs in Microsoft Windows.
They both provide a full dirent API as well as many other UNIX APIs.  MingW
can even be used for commercial applications!

# Microsoft Windows build scripts

**Currently supported build systems:**

* msbuild
* nmake

## msbuild

msbuild or regular Microsoft Visual Studio solutions / projects are represented as 
\*.sln and \*.vcxproj files. The solution file is capable to build release or debug 
version of sasl2.dll. Sasl plugins are splitted into two sets. Some of them will be
statically linked into sasl2.dll and others have separate solution files. This is 
done to simplify configuration and compilation with 3rdparty tools like Conan
package manager. And there are some plugins which won't be compiled at all at the
moment but may be support for them will be added later.

There are 3 solution files:

* **core** builds sasl2.dll with some static plugins included.
* **sasldb** builds sasldb library and corresponding dynamic plugin
* **gssapi** builds gssapi dynamic plugin

**IMPORTANT:** Only x64 configuration was tested with current Visual Studio projects.

#### Dependencies

Pay attention to cyrus-sasl.props file and its SaslDependencyRoot property. 
It's where it's looking for dependencies. You can put your OpenSSL installation in there as well
as any other dependency. Just make sure it's exacty in "lib" and "include" directories to make
it compile

**core** solution depends only on OpenSSL. You can change openssl.props file if putting
OpenSSL into SaslDependencyRoot is not an option for you. In this case openssl.props may look 
something like this

```xml
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="4.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <ImportGroup Label="PropertySheets" />
  <PropertyGroup Label="UserMacros" />
  <ItemDefinitionGroup>
    <ClCompile>
      <AdditionalIncludeDirectories>C:\path\to\openssl\install\include;%(AdditionalIncludeDirectories)</AdditionalIncludeDirectories>
    </ClCompile>
    <Link>
      <AdditionalLibraryDirectories>C:\path\to\openssl\install\lib;%(AdditionalLibraryDirectories)</AdditionalLibraryDirectories>
      <AdditionalDependencies>libeay32.lib;%(AdditionalDependencies)</AdditionalDependencies>
    </Link>
  </ItemDefinitionGroup>
  <ItemGroup />
</Project>
```
In any case it's required to point to the correct name of the OpenSSL library (see libeay32.lib).
Also make sure previous values are preserved by including `%(something)` as in the example above.

---

**sasldb** solution depends on [LMDB](https://github.com/LMDB/lmdb) library. Again you can
play with cyrus-sasl.props and its SaslDependencyRoot or just change paths and libs to 
whatever you want in [sasldb.props](sasldb.props). If the version from upstream repository doesn't build
for you (even from stable branches), you can try next fork https://github.com/Ri0n/lmdb

---

**gssapiv2** plugin depends on one of Kerberos realization. But only MIT Kerberos was tested with msbuild.
As always put dependencies to the location pointed by cyrus-sasl.props or change gssapiv2 project file.

#### Compilation

Basically there 3 ways to compile with msbuild.

1. Just open any of solution files in Visual Studio and build it (assuming the dependency are
   already in correct locations)
2. Use msbuild command line util (pretty much the same as 1 but from command line)
3. Use [Conan package manager](https://conan.io) (will download dependencies from a cloud and build just cyrus-sasl.
   Or after the sources will be built automatically on CI, so one can just download binaries)

Regarding solution files there are to options:

1. cyrus-sasl-all-in-one.sln builds everything and depends on everything
2. cyrus-sasl-*.sln (other sln files) build something specific.
  * cyrus-sasl-core.sln builds sasl2 library with some static plugins included
  * cyrus-sasl-common.sln builds just common library useful for plugins development
  * cyrus-sasl-sasldb.sln builds sasldb library and corresponding plugin
  * cyrus-sasl-gssapiv2.sln builds gssapiv2 plugin

Build with Conan is not that hard too. I need it installed and added to PATH,
then from console in one of win32\conan subdirectory try something like

```cmd
> conan create . yourname/stable
```
For more information about Conan please read their
[documentation](https://docs.conan.io/en/latest)

#### Questions

The Visual Studio solution, project files and property sheets were
written by [Sergey Ilinykh](mailto:rion4ik@gmail.com).
Feel free to mail and ask questions.

## nmake

TBD

nmake makefiles weren't updated for awhile and most likely won't build.
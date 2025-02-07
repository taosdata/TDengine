# Avro C# [![Build Status](https://travis-ci.org/apache/avro.svg?branch=master)](https://travis-ci.org/apache/avro) [![NuGet Package](https://img.shields.io/nuget/v/Apache.Avro.svg)](https://www.nuget.org/packages/Apache.Avro)

 [![Avro](https://avro.apache.org/images/avro-logo.png)](http://avro.apache.org/)

 ## Install

 Install the Apache.Avro package from NuGet:

 ```
Install-Package Apache.Avro
```

## Build & Test

1. Install [.NET SDK 5.0+](https://dotnet.microsoft.com/download/dotnet-core)
2. `dotnet test`

## Project Target Frameworks

| Project         | Type       | .NET Framework 4.0 | .NET Standard 2.0  | .NET Standard 2.1 | .NET Core 3.1 | .NET 5.0  |
|:---------------:|:----------:|:------------------:|:------------------:|:-----------------:|:-------------:|:---------:|
| Avro.codegen    | Exe        |                    |                    |                   | ✔️            |✔️        |
| Avro.ipc        | Library    | ✔️                 | ✔️                |                   |               |           |
| Avro.ipc.test   | Unit Tests | ✔️                 |                    |                   |               |           |
| Avro.main       | Library    |                    | ✔️                 | ✔️               |               |           |
| Avro.msbuild    | Library    | ✔️                | ✔️                 |                   |               |           |
| Avro.perf       | Exe        | ✔️                |                    |                   |                |✔️        |
| Avro.test       | Unit Tests | ✔️                |                    |                   | ✔️            |✔️         |

## Dependency package version strategy

1. Use [`versions.props`](./versions.props) to specify package versions. `PackageReference` elements in `.csproj` files should use only version properties defined in [`versions.props`](./versions.props).

2. By updating the versions in our libraries, we require users of the library to update to a version equal to or greater than the version we reference. For example, if a user were to reference an older version of Newtonsoft.Json, they would be forced to update to a newer version before they could use a new version of the Avro library.
In short, we should only update the version of the dependencies in our libraries if we absolutely must for functionality that we require. We leave it up to the users of the library as to whether or not they want the latest and greatest of a particularly dependency. We're only going to require the bare minimum.

## Notes

The [LICENSE](./LICENSE) and [NOTICE](./NOTICE) files in the lang/csharp source directory are used to build the binary distribution. The [LICENSE.txt](../../LICENSE.txt) and [NOTICE.txt](../../NOTICE.txt) information for the Avro C# source distribution is in the root directory.

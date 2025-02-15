# Apache Avro Build Instructions

## Requirements

The following packages must be installed before Avro can be built:

 - Java: 11, 17 and 21 with the appropriate toolchain config, Maven 3.9.6 or better, protobuf-compile
 - PHP: php8, phpunit, php8-gmp
 - Python 3: 3.7 or greater, tox (tox will install other dependencies as needed)
 - C: gcc, cmake, asciidoc, source-highlight, Jansson, pkg-config
 - C++: cmake 3.7.2 or greater, g++, flex, bison, libboost-dev
 - C#: .NET Core 2.2 SDK
 - JavaScript: Node 12.x+, nodejs, npm
 - Ruby: Ruby 2.7 or greater, ruby-dev, gem, bundler, snappy
 - Perl: Perl 5.24.1 or greater, gmake, Module::Install,
   Module::Install::ReadmeFromPod, Module::Install::Repository,
   Math::BigInt, JSON::MaybeXS, Try::Tiny, Regexp::Common, Encode,
   Object::Tiny, Compress::ZLib, Error::Simple, Test::More,
   Test::Exception, Test::Pod
 - Apache Ant 1.7
 - md5sum, sha1sum, used by top-level dist target

## Using docker

It can be simpler to use a Docker image with all of the requirements already
installed. If you have Docker installed on your host machine, you can build
inside a container by running:

```bash
./build.sh docker
docker@539f6535c9db:~/avro$ cd lang/java/
docker@539f6535c9db:~/avro/lang/java$ ./build.sh test
[INFO] Scanning for projects...
```

When this completes you will be in a shell running in the
container. Building the image the first time may take a while (20
minutes or more) since dependencies must be downloaded and
installed. However subsequent invocations are much faster as the
cached image is used.

The working directory in the container is mounted from your host. This
allows you to access the files in your Avro development tree from the
Docker container.

There are some additional `DOCKER_` environment variables described in 
[build.sh](./build.sh) that can be used to interact with the image using
the build script. Some examples:

```bash
# Rebuild the docker image without using the build cache.
DOCKER_BUILD_XTRA_ARGS=--no-cache ./build.sh docker

# Build a docker image with a specific tag (for an RC or poc, for example)
DOCKER_IMAGE_NAME=avro-build:1.10.1-rc1 ./build.sh docker

# Run a command and return.
DOCKER_RUN_ENTRYPOINT="mvn --version" ./build.sh docker
```

## Developing inside a Container (Visual Studio Code Devcontainer)

Requirement:
 - [Visual Studio Code](https://code.visualstudio.com/)
 - [Remote Development extension pack](https://aka.ms/vscode-remote/download/extension)
 - Docker
   - Windows: (Docker Desktop)[https://www.docker.com/products/docker-desktop]
   - macOS: (Docker Desktop)[https://www.docker.com/products/docker-desktop]
   - Linux: (Docker CE/EE)[https://docs.docker.com/install/#supported-platforms] and (Docker Compose)[https://docs.docker.com/compose/install]

Useful links:
 - (Developing inside a Container)[https://code.visualstudio.com/docs/remote/containers]
 - (Going further with Dev Containers)[https://microsoft.github.io/code-with-engineering-playbook/developer-experience/going-further/]

## Building

Once the requirements are installed (or from the Docker container),
build.sh can be used as follows:

```
./build.sh test # runs tests for all languages
./build.sh dist # creates all release distribution files in dist/
./build.sh clean # removes all generated artifacts
```

## Testing

Testing is done with the same Docker container as mentioned in the building
step. The difference is that it will do clean run of the full test suite:

```bash
./build.sh docker-test
```

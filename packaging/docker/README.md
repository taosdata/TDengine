# TAOS DATABASE Docker version

# Realse docker version install package
To release a docker version install package, change directory to 
_taosdata/tools/docker_ and run:
```shell
bash release_docker.sh
```
Then it will generate a tar file in _release_ file.

# Install TAOS DATA
To install taosdata, uncompress the _tar_ file in release directory and
run _install.sh_
```shell
./install.sh         # Install mnode and dnode
./install.sh all     # Install mnode and dnode
./install.sh mnode   # Install mnode
./install.sh dnode   # Install dnode
```

# Check the services
To check if taosdata run correctly, use _docker_ commands.
```shell
docker container ls
```

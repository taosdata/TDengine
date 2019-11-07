<!-- *This file shows the relsease and install process for TAOS database* -->

# Directory hierarchy

rootDir/
   |
   |--taosdata/
   |
   |--build/
   |
   |--release/
        |
        |--taos-version-releasetime-user.tar.gz


# Release a new version

To release a new version, run the **release.sh** in __taosdata/tools/__ directory. Then the script will
generate a directory called **release/** as shown above. In the **release** directory, there
is a compressed file to install the database. When releasing a new version, you can assign
a new version number or choose to use the old one.


# Install TAOS database

To install TAOS database, unzip the **taos-version-releasetime-use.tar.gz** file. For
example: **tar -zxf taosdata-version-releasetime-use.tar.gz**. Then a directory is
generated with the same name as the compressed file except the extension. Then enter
that directory, run **install.sh** to install the database. 

# Check service status

Because the installation using **systemd** to manage the serivce, people can use systemd to check
the service. Alias below are recommended to add to you startup script if you want to check
the service status.

```bash
alias statm="sudo systemctl status taosd"     # check the status of taosd
alias startm="sudo systemctl start taosd"     # start taosd service
alias stopm="sudo systemctl stop taosd"       # stop taosd service
```

# Uninstall TAOS database

To uninsall TAOS database, just type **rmtaos**.

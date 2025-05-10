# TDengine Enterprise Installation and Operation Guide

## 1. Installation
If you want to specify the installation path, please follow these steps:

1. Extract the installation package and enter the extracted directory:
```bash
tar xf TDengine-enterprise-3.3.6.5-Linux-x64.tar.gz && cd TDengine-enterprise-3.3.6.5
```

2. Place `install_spec.sh`
Make sure the `install_spec.sh` script is located within the `TDengine-enterprise-3.3.6.5` directory. If it isn't, move it to this directory.

3. Grant the `install_spec.sh` script the `755` permission:
```bash
chmod 755 install_spec.sh
```

4. Run the installation script. Use the `-d` option to specify the `~/tdengine` installation directory and `-e no` for non-interactive mode:
```bash
./install_spec.sh -d ~/tdengine -e no
```

5. Move the `tdsvc.sh` script to the `~/tdengine` directory and grant it the `755` permission:
```bash
mv tdsvc.sh ~/tdengine && chmod 755 ~/tdengine/tdsvc.sh
```

6. To source env for taos bin and lib,you should restart your shell or run:
```bash
source $HOME/.bashrc (sh, bash, zsh)
```  

## 2. Start and Stop Operations
All the configuration files are in the `cfg` folder of the installation directory, now it is `~/tdengine/cfg/`.

### Start All Components
```bash
./tdsvc.sh start-all ~/tdengine/cfg/
```

### Stop All Components
```bash
./tdsvc.sh stop-all
```

### Start a Specific Component (e.g., server)
```bash
./tdsvc.sh start taosd ~/tdengine/cfg/
```

### Stop a Specific Component (e.g., server)
```bash
./tdsvc.sh stop taosd
``` 
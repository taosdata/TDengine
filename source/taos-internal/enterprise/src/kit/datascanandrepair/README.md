## taosscan
A tool used to scan and report TDengine data files and repair the files (optionally).

### Usage
Run the command below to get usage help:
```
./taosscan -?
```

_taosscan_ will scan the data directory, and generate a **report** and a **repair**
directory. The report directory contains the report about the scan result. If it is
empty, it means the data directory has no error. If errors happens in a vnode, a 
corresponding vnode directory is created in report directory and a corresponding 
suggested recoverd files will be created in repair directory.

### Examples:
```
./taosscan
```
Scan the default directory _/var/lib/taos_ and generate _report_ and _repair_ directory.

```
./taosscan /home/user1/data
```
Set the scan directory

```
./taosscan -K /home/usr1/data
```
Keep all scan results including those are correct.

```
./taosscan -I /home/usr1/data
```
Force to replace the suggested files to the real data directory. Take care to use this option.


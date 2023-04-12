# OPC plugin for tasx

`OPC` is opc connector plugin for taosx, it's part of taosx.

## Build

`OPC` connector is developed by Golang. To build opc connector, the **golang 1.19** and above is required.

### go build

change to opc plugin directory and execute:

```
go mod tidy
go build
```

### Build by `make`

Or change to opc plugin directory and execute:

```
make
```

### Build for OPC-DA

OPC-DA is only for Microsoft Windows. So a windows env is required.

In `CMD`, and run:

```
set GOOS=windows
set GOARCH=amd64

go build \
    -ldflags "-s -w -X 'collector/version.Version=$(version)'" \
    -o dist/opc-collector_windows_amd64.exe
```

for 64-bit. Or

```
set GOOS=windows 
set GOARCH=386 
go build -ldflags \
    "-s -w -X 'collector/version.Version=$(version)'" \
    -o dist/opc-collector_windows_386.exe
```

for 32-bit.

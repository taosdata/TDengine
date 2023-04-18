all: clean \
	darwin_amd64 \
	darwin_arm64 \
	linux_amd64 \
	linux_arm \
	linux_arm_64 \
	windows \
	windows_386

version = "1.0.0"

clean:
	echo "Cleaning dist folder"
	rm -f dist/*
	if [ ! -d dist ]; then mkdir dist; fi

darwin_amd64:
	echo "Building for darwin amd64"
	GOOS=darwin GOARCH=amd64 go build -ldflags "-s -w -X 'collector/version.Version=$(version)'" -o dist/opc-collector_darwin_amd64

darwin_arm64:
	echo "Building for darwin arm64"
	GOOS=darwin GOARCH=arm64 go build -ldflags "-s -w -X 'collector/version.Version=$(version)'" -o dist/opc-collector_darwin_arm64

linux_amd64:
	echo "Building for linux amd64"
	GOOS=linux GOARCH=amd64 go build -ldflags "-s -w -X 'collector/version.Version=$(version)'" -o dist/opc-collector_linux_amd64

linux_arm:
	echo "Building for linux arm"
	GOOS=linux GOARCH=arm go build -ldflags "-s -w -X 'collector/version.Version=$(version)'" -o dist/opc-collector_linux_arm

linux_arm_64:
	echo "Building for linux arm64"
	GOOS=linux GOARCH=arm64 go build -ldflags "-s -w -X 'collector/version.Version=$(version)'" -o dist/opc-collector_linux_arm64

windows:
	echo "Building for windows"
	GOOS=windows GOARCH=amd64 go build -ldflags "-s -w -X 'collector/version.Version=$(version)'" -o dist/opc-collector_windows_amd64.exe

windows_386:
	echo "Building for windows 386"
	GOOS=windows GOARCH=386 go build -ldflags "-s -w -X 'collector/version.Version=$(version)'" -o dist/opc-collector_windows_386.exe

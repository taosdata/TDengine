all: clean \
	darwin_amd64 \
	darwin_arm64 \
	linux_amd64 \
	linux_arm \
	linux_arm_64 \
	windows \
	windows_386

version = "1.0.0"
build_date = $(shell date +%Y-%m-%d\ %H:%M:%S)
commit_id = $(shell git rev-parse HEAD)

clean:
	echo "Cleaning dist folder"
	rm -rf dist/*
	if [ ! -d dist ]; then mkdir dist; fi

darwin_amd64:
	echo "Building for darwin amd64"
	if [ ! -d dist/darwin_amd64 ]; then mkdir -p dist/darwin_amd64; fi
	GOOS=darwin GOARCH=amd64 go build \
		-ldflags "-s -w -X 'collector/version.Version=$(version)' -X 'collector/version.BuildAt=$(build_date)' -X 'collector/version.CommitID=$(commit_id)'" \
		-o dist/darwin_amd64/opc-collector

darwin_arm64:
	echo "Building for darwin arm64"
	if [ ! -d dist/darwin_arm64 ]; then mkdir -p dist/darwin_arm64; fi
	GOOS=darwin GOARCH=arm64 go build \
		-ldflags "-s -w -X 'collector/version.Version=$(version)' -X 'collector/version.BuildAt=$(build_date)' -X 'collector/version.CommitID=$(commit_id)'" \
		-o dist/darwin_arm64/opc-collector

linux_amd64:
	echo "Building for linux amd64"
	if [ ! -d dist/linux_amd64 ]; then mkdir -p dist/linux_amd64; fi
	GOOS=linux GOARCH=amd64 go build \
		-ldflags "-s -w -X 'collector/version.Version=$(version)' -X 'collector/version.BuildAt=$(build_date)' -X 'collector/version.CommitID=$(commit_id)'" \
		-o dist/linux_amd64/opc-collector

linux_arm:
	echo "Building for linux arm"
	if [ ! -d dist/linux_arm ]; then mkdir -p dist/linux_arm; fi
	GOOS=linux GOARCH=arm go build \
		-ldflags "-s -w -X 'collector/version.Version=$(version)' -X 'collector/version.BuildAt=$(build_date)' -X 'collector/version.CommitID=$(commit_id)'" \
		-o dist/linux_arm/opc-collector

linux_arm_64:
	echo "Building for linux arm64"
	if [ ! -d dist/linux_arm64 ]; then mkdir -p dist/linux_arm64; fi
	GOOS=linux GOARCH=arm64 go build \
		-ldflags "-s -w -X 'collector/version.Version=$(version)' -X 'collector/version.BuildAt=$(build_date)' -X 'collector/version.CommitID=$(commit_id)'" \
		-o dist/linux_arm64/opc-collector

windows:
	echo "Building for windows"
	if [ ! -d dist/windows_amd64 ]; then mkdir -p dist/windows_amd64; fi
	GOOS=windows GOARCH=amd64 go build \
		-ldflags "-s -w -X 'collector/version.Version=$(version)' -X 'collector/version.BuildAt=$(build_date)' -X 'collector/version.CommitID=$(commit_id)'" \
		-o dist/windows_amd64/opc-collector.exe

windows_386:
	echo "Building for windows 386"
	if [ ! -d dist/windows_386 ]; then mkdir -p dist/windows_386; fi
	GOOS=windows GOARCH=386 go build \
		-ldflags "-s -w -X 'collector/version.Version=$(version)' -X 'collector/version.BuildAt=$(build_date)' -X 'collector/version.CommitID=$(commit_id)'" \
		-o dist/windows_386/opc-collector.exe

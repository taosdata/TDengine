#!/bin/bash
cargo make build-all-with-agent
go env -w GOPROXY=https://goproxy.cn,direct
go env -w GO111MODULE=on
cargo make plugins
cargo make install-locally


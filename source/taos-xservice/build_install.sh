#!/bin/bash
cargo make build-all-with-agent
go env -w GOPROXY=${GOPROXY:-https://nexus.tdengine.net/repository/goproxy/,direct}
go env -w GO111MODULE=on
cargo make plugins
cargo make install-locally


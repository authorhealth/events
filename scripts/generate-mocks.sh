#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"
cd ..

find . -name "mock_*.go" -delete

go tool github.com/vektra/mockery/v2 --log-level=error

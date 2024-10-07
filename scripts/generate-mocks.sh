#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"
cd ..

find . -name "mock_*.go" -delete

mockery --quiet

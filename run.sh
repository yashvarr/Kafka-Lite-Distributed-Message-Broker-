#!/bin/bash
# run.sh - Script to build and run the mini-Kafka project

set -e # Exit early if any commands fail

(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
  CC=gcc-13 CXX=g++-13 cmake -B build -S .
  cmake --build ./build
)

exec $(dirname $0)/build/kafka "$@"

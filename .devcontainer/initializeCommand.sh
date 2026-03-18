#!/bin/bash

mkdir -p ${HOME}/.m2
mkdir -p ${HOME}/scratch
touch ${HOME}/.devcontainers/trino-bashhistory
WORK_DIR=$(dirname "${BASH_SOURCE[0]}")/.work
rm -rf $WORK_DIR
mkdir -p $WORK_DIR

cp /usr/local/share/ca-certificates/*.crt $WORK_DIR/

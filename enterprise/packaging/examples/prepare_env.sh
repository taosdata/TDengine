#!/bin/bash

echo "Preparing Env"

echo "topdir is "$1

if [ -e '$1' ]; then
    echo -e "Please specify top dir to prepare environment"
    exit 1
else
    echo -e "Go to top dir first"
    cd $1
    ls -l
fi

echo "Preparing Env Done!"

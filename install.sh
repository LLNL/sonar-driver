#!/usr/bin/env bash

# Make sure we are in the same dir as this executable
cd "$( dirname "${BASH_SOURCE[0]}" )"

if [[ "${BASH_SOURCE[0]}" != "${0}" ]]
then
    echo "Installing..."
else
    echo "This script must be sourced!"
    exit 1
fi

make install
source ./venv/bin/activate
export PATH=$(pwd)/bin:$PATH

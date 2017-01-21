#!/bin/bash

# This script installs a working Anaconda-based test environment
# for cdb_query.

if [ $(uname -a | cut -d ' ' -f1) == 'Darwin' ]; then
    OS="MacOSX"
else
    OS="Linux"
fi

if ! type "conda" > /dev/null; then
    echo "conda not found, installing"
    curl https://repo.continuum.io/miniconda/Miniconda2-latest-$OS-x86_64.sh -O
    bash Miniconda2-latest-$OS-x86_64.sh -b -p $HOME/miniconda_test
    source $HOME/miniconda_test/bin/activate $HOME/miniconda_test
    rm Miniconda2-latest-$OS-x86_64.sh
else
    echo "conda found, using system installation"
fi


DIST_LIST="2.7"
for DIST in ${DIST_LIST}; do
    conda update -q conda
    conda info -a
    conda env create --file ci/requirements-py27.yml
done

echo "Installation complete."

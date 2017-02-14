#!/bin/bash

# This script installs a working Anaconda-based test environment
# for cdb_query.
DIST="2.7"
CHANNEL="forge"
while getopts ":d:hs" opt; do
  case $opt in
    h)
      echo "Usage: create_test_env.sh -d DIST -o -h"
      echo " -d DIST: DIST = 2.7, 3.3, 3.4, 3.5 or 3.6. Default: 2.7."
      echo " -s : Use a slightly older but more compatible build."
      echo " -h : Display this message"
      exit 1
      ;;
    s)
      CHANNEL="defaults"
      ;;
    d)
      DIST=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done


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


DIST_LIST="$DIST"
for DIST in ${DIST_LIST}; do
    conda update -q conda
    conda info -a
    FILE="ci/requirements-py${DIST}_${CHANNEL}.yml"
    if [ ! -f $FILE ]; then
        echo "Distribution $DIST is not available"
    fi
    conda env create --file $FILE
done

echo "Installation complete."

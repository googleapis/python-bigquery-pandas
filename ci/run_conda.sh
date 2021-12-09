#!/bin/bash
# Copyright (c) 2017 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e -x
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# Install dependencies using (micro)mamba
# https://github.com/mamba-org/micromamba-docker
REQ="ci/requirements-${PYTHON}-${PANDAS}"
micromamba install -q pandas=$PANDAS python=${PYTHON} -n base -c conda-forge;
micromamba install -q --file "$REQ.conda" -n base -c conda-forge;
micromamba list
micromamba info

micromamba activate base
python setup.py develop --no-deps

# Run the tests
$DIR/run_tests.sh

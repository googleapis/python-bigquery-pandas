#!/bin/bash
# Copyright (c) 2017 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e -x
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# Install dependencies using Conda

conda config --set always_yes yes --set changeps1 no
conda config --add channels conda-forge
conda update -q conda
conda info -a
conda create -q -n test-environment python=$PYTHON
source activate test-environment
REQ="ci/requirements-${PYTHON}-${PANDAS}"

# TODO: Migrate the mamba with https://github.com/mamba-org/micromamba-docker
conda install -q --file "$REQ.conda";
conda install -q pandas=$PANDAS;

python setup.py develop --no-deps

# Run the tests
$DIR/run_tests.sh

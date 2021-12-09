#!/bin/bash
# Copyright (c) 2017 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e -x
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# Install dependencies using Conda

source activate test-environment
REQ="ci/requirements-${PYTHON}-${PANDAS}"
micromamba install -q --file "$REQ.conda";
micromamba install -q pandas=$PANDAS;
micromamba list
micromamba info

python setup.py develop --no-deps

# Run the tests
$DIR/run_tests.sh

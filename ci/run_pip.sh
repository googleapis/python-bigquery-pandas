#!/bin/bash
# Copyright (c) 2017 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

set -e -x
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# Install test requirements
pip install coverage pytest pytest-cov flake8 codecov

REQ="testing/constraints-${PYTHON}"
pip install -e '.[tqdm]' -c "$REQ"

$DIR/run_tests.sh

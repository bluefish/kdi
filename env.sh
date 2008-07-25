#!/bin/bash

ROOT=$(dirname $(readlink -f ${BASH_SOURCE[0]}))

prepend_path()
{
  if ! eval test -z "\"\${$1##*:$2:*}\"" -o -z "\"\${$1%%*:$2}\"" -o -z "\"\${$1##$2:*}\"" -o -z "\"\${$1##$2}\"" ; then
    eval "$1=$2:\$$1"
  fi
}

echo "Setting up environment from: $ROOT"
prepend_path PATH "$ROOT/src/python"
prepend_path PATH "$ROOT/bin"
prepend_path LD_LIBRARY_PATH "$ROOT/lib"
prepend_path PYTHONPATH "$ROOT/src/python"
prepend_path PYTHONPATH "$ROOT/lib/python"

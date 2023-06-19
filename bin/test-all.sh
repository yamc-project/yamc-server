#!/bin/bash
current_env=$(pyenv version | awk '{print $1}')
cdir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )

echo "* The current directory is $cdir"

pyenv virtualenv env-test || exit 1
pyenv local env-test || exit 1

OLDPYTHONPATH=$PYTHONPATH
export PYTHONPATH=""

function cleanup {
  echo "* Cleaning up..."
  #echo "...restoring pyenv to: $current_env"
  pyenv local $current_env || exit 1
  #echo "...deleting env-test"
  pyenv virtualenv-delete -f env-test
  #echo "...retoring old PYTHONPATH $OLDPYTHONPATH"
  export PYTHONPATH=$OLDPYTHONPATH
  unset OLDPYTHONPATH 
}

trap cleanup EXIT

pip install --upgrade pip
pip install wheel

yamc_server=$(find $cdir/dist | grep yamc_server)
echo "* package: $yamc_server"

pip install $yamc_server || exit 1

find $cdir/dist | grep "whl" | egrep -v "yamc_server|yamc_gpio" | \
while read line; do
  echo "* package: $line"
  pip install $line || exit 1
done || exit 1

echo "* Done: SUCCESS"

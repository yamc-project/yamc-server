#!/bin/bash
current_env=$(pyenv version | awk '{print $1}')
cdir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )

mkdir -p $cdir/logs/test
LOGFILE="$cdir/logs/build/test-$(date +%y%m%d_%H%M%S).out"

echo "* Testing installation of packages, the log file is ${LOGFILE}"
echo "* The current directory is $cdir" | tee -a ${LOGFILE}

pyenv virtualenv env-test >>${LOGFILE} 2>&1 || exit 1
pyenv local env-test >>${LOGFILE} 2>&1 || exit 1

OLDPYTHONPATH=$PYTHONPATH
export PYTHONPATH=""

function cleanup {
  echo "* Cleaning up" | tee -a ${LOGFILE}
  #echo "...restoring pyenv to: $current_env"
  pyenv local $current_env >>${LOGFILE} 2>&1 || exit 1
  #echo "...deleting env-test"
  pyenv virtualenv-delete -f env-test >>${LOGFILE} 2>&1
  #echo "...retoring old PYTHONPATH $OLDPYTHONPATH"
  export PYTHONPATH=$OLDPYTHONPATH
  unset OLDPYTHONPATH 
}

trap cleanup EXIT

pip install --upgrade pip >>${LOGFILE} 2>&1
pip install wheel >>${LOGFILE} 2>&1

yamc_server=$(find $cdir/dist | grep yamc_server)
echo "* package: $yamc_server" | tee -a ${LOGFILE}

pip install $yamc_server >>${LOGFILE} 2>&1 || exit 1

find $cdir/dist | grep "whl" | egrep -v "yamc_server|yamc_gpio" | \
while read line; do
  echo "* package: $line" | tee -a ${LOGFILE}
  pip install $line >>${LOGFILE} 2>&1 || exit 1
done || exit 1

echo "* Done: INSTALLATION SUCCESS" | tee -a ${LOGFILE}

#!/bin/bash
pdir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )

mkdir -p $pdir/yamc-server/logs/build
LOGFILE="$pdir/yamc-server/logs/build/build-$(date +%y%m%d_%H%M%S).out"

pwd=$(pwd)

rm -fr $pdir/yamc-server/dist/*

echo "* Building the packages, the log file is ${LOGFILE}" | tee -a ${LOGFILE}
echo "* The parent directory is $pdir" | tee -a ${LOGFILE}

find $pdir/ -depth 1 -type d -name 'yamc*' | grep -v 'yamc-server' | \
while read line; do
  echo "* Building $line" | tee -a ${LOGFILE}
  cd $line
  rm -fr dist/*
  make build >>${LOGFILE} 2>&1 || exit 1
  cp dist/* $pdir/yamc-server/dist
done

echo "* Building yamc-server" | tee -a ${LOGFILE}
cd $pdir/yamc-server
make build >>${LOGFILE} 2>&1

echo "* Done: BUILD SUCCESS" | tee -a ${LOGFILE}

cd $pwd

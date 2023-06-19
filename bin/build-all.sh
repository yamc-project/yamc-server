#!/bin/bash
pdir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd )

pwd=$(pwd)

rm -fr $pdir/yamc-server/dist/*

echo "* The parent directory is $pdir"

find $pdir/ -depth 1 -type d -name 'yamc*' | grep -v 'yamc-server' | \
while read line; do
  echo "* Building $line"
  cd $line
  rm -fr dist/*
  make build || exit 1
  cp dist/* $pdir/yamc-server/dist
done

cd $pdir/yamc-server
make build

cd $pwd

#!/bin/bash
pdir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )

ls $pdir/plugins | \
while read line; do
  echo "* Building $line"
  cd $pdir/plugins/$line 
  rm -fr dist
  python setup.py sdist 
  cp dist/*.tar.gz $pdir/dist
done


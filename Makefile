# Makefile for res2-service
# uses version from git with commit hash

help:
	@echo "make <target>"
	@echo "build	build yamc-server."
	@echo "clean	clean all temporary directories."
	@echo "all      build yamc plugins and the server."
	@echo ""

build:
	python setup.py egg_info sdist	

check:
	pylint yamc 

clean:
	rm -fr build
	rm -fr dist
	rm -fr yamc/*.egg-info

format:
	black yamc

all:
	bin/build-all.sh


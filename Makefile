# Makefile for yamc-server
# uses version from git with commit hash

help:
	@echo "make <target>"
	@echo "build	build yamc-server."
	@echo "clean	clean all temporary directories."
	@echo "all      build yamc plugins and the server."
	@echo ""

build:
	python setup.py bdist_wheel
	rm -fr build	

check:
	pylint yamc 

clean:
	rm -fr build
	rm -fr dist
	rm -fr yamc/*.egg-info
	rm -fr yamc/*.dist-info

format:
	black yamc

all:
	bin/build-all.sh

test:
	bin/build-all.sh && bin/test-all.sh


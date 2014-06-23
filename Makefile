PREFIX=/usr/local/dzmq
BASE=$(shell pwd)
BUILD_SLASH="${BASE}/build/"
TARGET="${BASE}/build/target"

all:
	mkdir -p ${TARGET}; cd ${TARGET}; cmake $(PWD)/src; make

debug:
	mkdir -p ${TARGET}; cd ${TARGET}; cmake -DDEFINE_DEBUG=ON $(PWD)/src; make

clean:
	rm -rf build

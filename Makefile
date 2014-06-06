PREFIX=/usr/local/dzmq

all:
	mkdir -p build/target
	cd build/target; cmake $(PWD)/src; make; mv dzmq $(PWD)/build/dzmq

debug:
	mkdir -p build/target
	cd build/target; cmake -DDEFINE_DEBUG=ON $(PWD)/src; make; mv dzmq $(PWD)/build/dzmq

clean:
	rm -rf build

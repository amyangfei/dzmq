OS = $(shell uname)

LIBS = -lzmq -lczmq
OBJS = dzutil.o dzlog.o dzspinlock.o dzservice.o mdp_client.o mdp_worker.o dzbroker.o dztester.o main.o
HIREDIS = hiredis/net.o hiredis/sds.o hiredis/hiredis.o

ifeq ($(OS),Darwin)
	CFLAGS = -Wall
else
	CFLAGS = -Wall -std=c99 -pedantic -D_GNU_SOURCE
endif

all: ${OBJS}
	cd ./hiredis && $(MAKE) $@
	cd ..
	gcc ${CFLAGS} -o main ${OBJS} ${HIREDIS} ${LIBS}
clean:
# rm -f main ${OBJS} ${HIREDIS}
	rm -f main ${OBJS}


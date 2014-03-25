OS = $(shell uname)

LIBS = -lzmq -lczmq
OBJS = dzutil.o dzlog.o dzspinlock.o dzservice.o mdp_client.o mdp_worker.o dzbroker.o dztester.o main.o

ifeq ($(OS),Darwin)
	CFLAGS = -Wall
else
	CFLAGS =  -Wall -std=c99 -pedantic -D_GNU_SOURCE
endif

all: ${OBJS}
	gcc ${CFLAGS} -o main ${OBJS} ${LIBS}
clean:
	rm -f main ${OBJS}


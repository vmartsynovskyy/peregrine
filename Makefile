ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
LDFLAGS=-L $(ROOT_DIR)/core/bliss-0.73/ -lbliss -L/usr/local/lib -lpthread -latomic -L$(LD_LIBRARY_PATH) -ltbb -lzmq
CFLAGS=-O3 -std=c++2a -Wall -Wextra -Wpedantic -fPIC -fconcepts -I$(ROOT_DIR)/core/
OBJ=core/roaring.o core/DataGraph.o core/PO.o core/utils.o core/PatternGenerator.o $(ROOT_DIR)/core/showg.o
OUTDIR=bin/
CC=g++

all: bliss fsm count test existence-query convert_data worker count_master fsm_master existence-query_master

core/roaring.o: core/roaring/roaring.c
	gcc -c core/roaring/roaring.c -o $@ -O3 -Wall -Wextra -Wpedantic -fPIC 

%.o: %.cc
	$(CC) -c $? -o $@ $(CFLAGS)

worker: apps/worker.cc $(OBJ) bliss
	$(CC) apps/worker.cc $(OBJ) -o $(OUTDIR)/$@ $(LDFLAGS) $(CFLAGS)

count_master: apps/count_master.cc $(OBJ) bliss
	$(CC) apps/count_master.cc $(OBJ) -o $(OUTDIR)/$@ $(LDFLAGS) $(CFLAGS)

fsm_master: apps/fsm_master.cc $(OBJ) bliss
	$(CC) apps/fsm_master.cc $(OBJ) -o $(OUTDIR)/$@ $(LDFLAGS) $(CFLAGS)

fsm: apps/fsm.cc $(OBJ) bliss
	$(CC) apps/fsm.cc $(OBJ) -o $(OUTDIR)/$@ $(LDFLAGS) $(CFLAGS)

existence-query: apps/existence-query.cc $(OBJ) bliss
	$(CC) apps/existence-query.cc $(OBJ) -o $(OUTDIR)/$@ $(LDFLAGS) $(CFLAGS)

existence-query_master: apps/existence-query_master.cc $(OBJ) bliss
	$(CC) apps/existence-query_master.cc $(OBJ) -o $(OUTDIR)/$@ $(LDFLAGS) $(CFLAGS)

count: apps/count.cc $(OBJ) bliss
	$(CC) apps/count.cc $(OBJ) -o $(OUTDIR)/$@ $(LDFLAGS) $(CFLAGS)

test: core/test.cc $(OBJ) core/DataConverter.o bliss
	$(CC) core/test.cc -DTESTING $(OBJ) core/DataConverter.o -o $(OUTDIR)/$@ $(LDFLAGS) -lUnitTest++ $(CFLAGS)

convert_data: core/convert_data.cc core/DataConverter.o core/utils.o
	$(CC) -o $(OUTDIR)/$@ $? -L/usr/local/lib -lpthread -latomic -L$(LD_LIBRARY_PATH) -ltbb $(CFLAGS)

bliss:
	make -C ./core/bliss-0.73

clean:
	make -C ./core/bliss-0.73 clean
	rm -f core/*.o bin/*

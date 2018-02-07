## Makefile for pg_page_verification

# ./server/storage/checksum_impl.h
#
# Note: you may need to redefine
# Assert() as empty to compile this successfully externally.

# Requires pg_config to have a valid INCLUDEDIR-SERVER value
INC_DIR=$(shell pg_config --includedir-server)

CFLAGS = -g -std=gnu89 -I$(INC_DIR)

pg_page_verification: pg_page_verification.c
	$(CC) $(CFLAGS) -o $@ $< $(LIBS)

install:
	install -m 755

clean:
	rm pg_page_verification

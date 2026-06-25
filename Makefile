LZ4_DIR := vendor/lz4
LZ4_LIB := $(LZ4_DIR)/lib/liblz4.a

.PHONY: all lib spec clean

all: lib

# Build the vendored LZ4 static library that the Crystal bindings link against.
lib: $(LZ4_LIB)

$(LZ4_LIB): $(LZ4_DIR)/lib/lz4frame.c
	$(MAKE) -C $(LZ4_DIR)/lib liblz4.a

# Ensure the submodule is checked out before building.
$(LZ4_DIR)/lib/lz4frame.c:
	git submodule update --init --recursive

spec: lib
	crystal spec

clean:
	$(MAKE) -C $(LZ4_DIR)/lib clean

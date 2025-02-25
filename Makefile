CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
LDFLAGS = -L/usr/lib -lssl -lcrypto
INCLUDES = -I/usr/include
OBJ = project.o utils.o concurrent.o indpendent.o
DEPS = project.h utils.h concurrent.h indpendent.h
TARGET = project

# Default rule
all: $(TARGET)

# Compile the main project binary
$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# Compile C files into object files
%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) $(INCLUDES) -c -o $@ $<

# Clean up compiled files
clean:
	rm -f $(OBJ) $(TARGET)

.PHONY: all clean

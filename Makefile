CC = gcc
CFLAGS = -Wall -Wextra -O3 -g
LDFLAGS = -L/usr/lib -lssl -lcrypto -pthread -lm
INCLUDES = -I/usr/include
OBJ = project.o utils.o concurrent.o independent.o
DEPS = project.h utils.h concurrent.h independent.h
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

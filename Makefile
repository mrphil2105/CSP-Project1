CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
OBJ = project.o
DEPS = project.h
TARGET = project

# Default rule
all: $(TARGET)

# Compile the main project binary
$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^

# Compile C files into object files
%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c -o $@ $<

# Clean up compiled files
clean:
	rm -f $(OBJ) $(TARGET)

.PHONY: all clean

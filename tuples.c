#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include "tuples.h"  

// Generate tuples (16 bytes per tuple)
tuple_t *generate_tuples(int count) {
    if (count <= 0 || count > MAX_TUPLES)
        return NULL;
    size_t total_bytes = count * 16;
    unsigned char *buffer = malloc(total_bytes);
    if (!buffer)
        return NULL;
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0) {
        perror("open /dev/urandom");
        free(buffer);
        return NULL;
    }
    size_t offset = 0;
    while (offset < total_bytes) {
        ssize_t bytes_read = read(fd, buffer + offset, total_bytes - offset);
        if (bytes_read < 0) {
            if (errno == EINTR)
                continue;
            perror("read /dev/urandom");
            close(fd);
            free(buffer);
            return NULL;
        }
        offset += bytes_read;
    }
    close(fd);
    return (tuple_t *)buffer;
}

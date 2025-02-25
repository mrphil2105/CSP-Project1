#include <stdlib.h>
#include <stdio.h>

#ifdef __linux__
#include <sys/random.h>
#endif

#include "project.h"
#include "concurrent.h"
#include "independent.h"

const int MAX_TUPLES = 32000000;

#ifndef __linux__
ssize_t getrandom(unsigned char *buffer, size_t length, unsigned int flags) {
    if (length <= 0 || length > MAX_TUPLES * 16) {
        return -1;
    }
    for (int i = 0; i < length; i++) {
        buffer[i] = rand();
    }
    return length;
}
#endif

tuple_t *generate_tuples(int count) {
    if (count <= 0 || count > MAX_TUPLES) {
        return NULL;
    }
    unsigned char *buffer = malloc(count * 16);
    if (buffer == NULL) {
        return NULL;
    }
    getrandom(buffer, count * 16, 0);
    return (tuple_t *)buffer;
}

int main(int argc, char *argv[]) {
    printf("generating tuples...\n");
    int count = 20;
    tuple_t *tuples = generate_tuples(count);
    for (int i = 0; i < count; i++) {
        printf("key: ");
        for (int j = 0; j < 8; j++) {
            printf("%02X", tuples[i].key[j]);
        }
        printf(", value: ");
        for (int j = 0; j < 8; j++) {
            printf("%02X", tuples[i].value[j]);
        }
        printf("\n");
    }
    run_concurrent(tuples, count, 4, 8);
    run_independent(tuples, count, 4);
    return 0;
}

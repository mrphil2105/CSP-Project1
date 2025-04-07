#ifndef PROJECT_H
#define PROJECT_H

#define PARTITION_MULTIPLIER 2
#define MAX_TUPLES (1 << 28)  // Example limit; adjust as needed

typedef struct {
    unsigned char key[8];
    unsigned char value[8];
} tuple_t;

#endif

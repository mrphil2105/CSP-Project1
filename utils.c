#include "utils.h"
#include <time.h>
#include <stdint.h>
#include <string.h>
#include <math.h>

// A simple MurmurHash3 32-bit implementation for an 8-byte key.
uint32_t murmurhash3_32(const void *key, int len, uint32_t seed) {
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 4;
    uint32_t h1 = seed;
    
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    
    const uint32_t *blocks = (const uint32_t *)data;
    for (int i = 0; i < nblocks; i++) {
        uint32_t k1 = blocks[i];
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> (32 - 15));
        k1 *= c2;
        
        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> (32 - 13));
        h1 = h1 * 5 + 0xe6546b64;
    }
    
    const uint8_t *tail = data + nblocks * 4;
    uint32_t k1 = 0;
    switch(len & 3) {
        case 3: k1 ^= tail[2] << 16; // fall through
        case 2: k1 ^= tail[1] << 8;  // fall through
        case 1: k1 ^= tail[0];
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >> (32 - 15));
                k1 *= c2;
                h1 ^= k1;
    }
    
    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    
    return h1;
}

int hash_to_partition(const unsigned char *key, int partition_count) {
    uint32_t hash = murmurhash3_32(key, 8, 42);
    return (int)(hash % partition_count);
}

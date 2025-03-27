#include <math.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int count;
    int crossing;
} pthread_barrier_t;

// Helper: Convert 8 bytes to a uint64_t (not used in hashing below)
uint64_t bytes_to_long(const unsigned char *bytes) {
    uint64_t result = 0;
    for (int i = 0; i < 8; i++) {
        result = (result << 8) | bytes[i];
    }
    return result;
}

// Returns current time in seconds
double get_time_in_seconds() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

// MurmurHash3 32-bit implementation
// Processes key in 4-byte blocks and a tail for remaining bytes.
// Designed for an 8-byte key in your case.
uint32_t murmurhash3_32(const void *key, int len, uint32_t seed) {
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 4;
    uint32_t h1 = seed;
    
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    
    // Process blocks of 4 bytes.
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
    
    // Process remaining bytes (the "tail").
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
    
    // Finalization mix - force all bits of a hash block to avalanche.
    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    
    return h1;
}


int hash_to_partition(const unsigned char *key, int partition_count) {
    // Interpret the first 8 bytes of key as an unsigned long long.
    unsigned long long k = 0;
    // Copy the 8 bytes into k (assumes tuple_t key is at least 8 bytes).
    memcpy(&k, key, sizeof(k));
    return (int)(k % partition_count);
}


int pthread_barrier_init(pthread_barrier_t *barrier, void *attr, int count) {
    (void)attr;  // Unused parameter
    if (count <= 0) return -1;
    barrier->count = count;
    barrier->crossing = 0;
    pthread_mutex_init(&barrier->mutex, NULL);
    pthread_cond_init(&barrier->cond, NULL);
    return 0;
}

int pthread_barrier_wait(pthread_barrier_t *barrier) {
    pthread_mutex_lock(&barrier->mutex);
    barrier->crossing++;

    if (barrier->crossing >= barrier->count) {
        barrier->crossing = 0;
        pthread_cond_broadcast(&barrier->cond);
        pthread_mutex_unlock(&barrier->mutex);
        return 1;  // Special return value to indicate last thread
    } else {
        while (pthread_cond_wait(&barrier->cond, &barrier->mutex) != 0);
        pthread_mutex_unlock(&barrier->mutex);
        return 0;
    }
}

int pthread_barrier_destroy(pthread_barrier_t *barrier) {
    pthread_mutex_destroy(&barrier->mutex);
    pthread_cond_destroy(&barrier->cond);
    return 0;
}
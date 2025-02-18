#include <openssl/sha.h>

uint64_t bytes_to_long(const unsigned char *bytes) {
    uint64_t result = 0;
    for (int i = 0; i < 8; i++) {
        result = (result << 8) | bytes[i];
    }
    return result;
}

int hash_to_partition(const unsigned char *key, int partition_count) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(key, 8, hash);
    return bytes_to_long(hash) % partition_count;
}

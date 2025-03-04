#include <stdlib.h>
#include <stdio.h>


#ifdef __linux__
#include <sys/random.h>
#endif

#include "project.h"
#include "concurrent.h"
#include "independent.h"
#include "utils.h"

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

int main(int argc, char* argv[]) {
    // Let's use 2^24 = 16 million tuples as in the paper's experiments
    const int tuple_count = 1 << 24;

    // Generate input tuples
    //printf("Generating %d tuples...\n", tuple_count);
    tuple_t *tuples = generate_tuples(tuple_count);
    if (!tuples) {
        fprintf(stderr, "Error generating tuples.\n");
        return -1;
    }

    // Parameters
    int thread_options[] = {1, 2}; // can add more: 4, 8, 16, 32
    int num_thread_options = sizeof(thread_options) / sizeof(thread_options[0]);

    // Hash bits go from 0 to 2 (or 0 to 18 as needed)
    int min_hash_bits = 0;
    int max_hash_bits = 2; // adjust as necessary

    // Print CSV header
    // Format: Threads,HashBits,TimeSeconds,Throughput(MT/s)
    printf("Threads,HashBits,TimeSeconds,Throughput(MT/s)\n");

    // Loop over each thread count
    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];

        // Loop over each hash bits setting
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            double start_time = get_time_in_seconds();

            // Run the partitioning experiment
            int rc = run_independent(tuples, tuple_count, thread_count, hb);
            if (rc != 0) {
                fprintf(stderr, "Error in run_independent\n");
                continue;
            }

            double end_time = get_time_in_seconds();
            double elapsed = end_time - start_time;

            // Calculate throughput in Millions of Tuples per Second (MT/s)
            double throughput = (double)tuple_count / elapsed / 1e6;

            // Print CSV row: Threads,HashBits,TimeSeconds,Throughput(MT/s)
            printf("%d,%d,%.4f,%.2f\n", thread_count, hb, elapsed, throughput);
        }
    }

    // Clean up
    free(tuples);
    return 0;
}


// int main(int argc, char *argv[]) {
//     printf("generating tuples...\n");
//     int count = 20;
//     tuple_t *tuples = generate_tuples(count);
//     for (int i = 0; i < count; i++) {
//         printf("key: ");
//         for (int j = 0; j < 8; j++) {
//             printf("%02X", tuples[i].key[j]);
//         }
//         printf(", value: ");
//         for (int j = 0; j < 8; j++) {
//             printf("%02X", tuples[i].value[j]);
//         }
//         printf("\n");
//     }
//     run_concurrent(tuples, count, 4, 8);
//     run_independent(tuples, count, 4);
//     return 0;
// }

#include <stdio.h>
#include <stdlib.h>
#include "independent.h"
#include "project.h"
#include "utils.h"
#include "tuples.h"  // For generate_tuples

#define TUPLE_COUNT (1 << 24)  // ~16 million tuples

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <THREAD_COUNT> <HASHBITS>\n", argv[0]);
        return -1;
    }
    int thread_count = atoi(argv[1]);
    int hash_bits = atoi(argv[2]);

    // Generate tuples.
    tuple_t *tuples = generate_tuples(TUPLE_COUNT);
    if (!tuples) {
        fprintf(stderr, "Error generating tuples.\n");
        return -1;
    }

    // Calculate per-thread parameters.
    int partitions_per_thread = 1 << hash_bits;
    int total_partitions = thread_count * partitions_per_thread;
    int effective_capacity = (TUPLE_COUNT / partitions_per_thread) * PARTITION_MULTIPLIER;

    // Allocate global buffers.
    tuple_t *indep_big_block = malloc(thread_count * TUPLE_COUNT * PARTITION_MULTIPLIER * sizeof(tuple_t));
    if (!indep_big_block) {
        free(tuples);
        return -1;
    }
    tuple_t **global_indep_buffers = malloc(total_partitions * sizeof(tuple_t *));
    int *global_indep_indexes = calloc(total_partitions, sizeof(int));
    if (!global_indep_buffers || !global_indep_indexes) {
        free(tuples);
        free(indep_big_block);
        return -1;
    }
    for (int thr = 0; thr < thread_count; thr++) {
        for (int part = 0; part < partitions_per_thread; part++) {
            int idx = thr * partitions_per_thread + part;
            global_indep_buffers[idx] = indep_big_block +
                thr * (TUPLE_COUNT * PARTITION_MULTIPLIER) +
                part * effective_capacity;
            global_indep_indexes[idx] = 0;
        }
    }

    // Run experiment.
    double throughput = 0.0;
    if (run_independent_timed(tuples, TUPLE_COUNT, thread_count, hash_bits,
                              global_indep_buffers, global_indep_indexes, effective_capacity, &throughput) != 0) {
        fprintf(stderr, "Error in independent run with %d threads and %d hashbits\n", thread_count, hash_bits);
    } else {
        // Print a CSV result to STDOUT.
        printf("Threads,HashBits,Throughput\n");
        printf("%d,%d,%.2f\n", thread_count, hash_bits, throughput);
    }

    // Cleanup.
    free(tuples);
    free(indep_big_block);
    free(global_indep_buffers);
    free(global_indep_indexes);
    return 0;
}

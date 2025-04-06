#include <stdio.h>
#include <stdlib.h>
#include "concurrent.h"
#include "project.h"
#include "utils.h"
#include "tuples.h"  // For generate_tuples

#define TUPLE_COUNT (1 << 24)  // ~16 million tuples

int thread_options[] = {1, 2, 4, 8, 16, 32};
int min_hash_bits = 1;
int max_hash_bits = 18;

int main(int argc, char *argv[]) {
    // Check for the PREFIX environment variable.
    const char *prefix = getenv("PREFIX");
    if (prefix == NULL) {
        fprintf(stderr, "PREFIX environment variable not set\n");
        return -1;
    }

    int num_thread_options = sizeof(thread_options) / sizeof(thread_options[0]);

    // Generate tuples.
    tuple_t *tuples = generate_tuples(TUPLE_COUNT);
    if (!tuples) {
        fprintf(stderr, "Error generating tuples.\n");
        return -1;
    }

    // Allocate result arrays.
    double **conc_results = malloc(num_thread_options * sizeof(double *));
    if (!conc_results) {
        free(tuples);
        return -1;
    }
    for (int i = 0; i < num_thread_options; i++) {
        conc_results[i] = calloc(max_hash_bits - min_hash_bits + 1, sizeof(double));
    }

    // Pre-allocate memory for concurrent experiments.
    int worst_conc_partitions = 1 << max_hash_bits;
    int worst_conc_capacity = (TUPLE_COUNT / (1 << max_hash_bits)) * PARTITION_MULTIPLIER;
    size_t worst_total_conc = (size_t)worst_conc_partitions * worst_conc_capacity;
    tuple_t *conc_big_block = malloc(worst_total_conc * sizeof(tuple_t));
    if (!conc_big_block) {
        free(tuples);
        return -1;
    }
    tuple_t **global_conc_buffers = malloc(worst_conc_partitions * sizeof(tuple_t *));
    int *global_conc_indexes = calloc(worst_conc_partitions, sizeof(int));

    // Run Concurrent Experiments (each combination runs once).
    for (int t_idx = 0; t_idx < num_thread_options; t_idx++) {
        int thread_count = thread_options[t_idx];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            int effective_capacity = (TUPLE_COUNT / (1 << hb)) * PARTITION_MULTIPLIER;
            // Set up global buffers and indexes for each partition.
            int total_partitions = 1 << hb;
            for (int i = 0; i < total_partitions; i++) {
                global_conc_buffers[i] = conc_big_block + i * effective_capacity;
                global_conc_indexes[i] = 0;
            }
            double throughput = 0.0;
            if (run_concurrent_timed(tuples, TUPLE_COUNT, thread_count, total_partitions,
                                     global_conc_buffers, global_conc_indexes,
                                     effective_capacity, &throughput) != 0) {
                fprintf(stderr, "Error in concurrent run with %d threads and hb=%d\n", thread_count, hb);
                continue;
            }
            conc_results[t_idx][hb - min_hash_bits] = throughput;
        }
    }

    // Print results.
    printf("Concurrent Experiment Results (Throughput MT/s):\n");
    printf("Threads,HashBits,Throughput\n");
    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            printf("%d,%d,%.2f\n", thread_count, hb, conc_results[i][hb - min_hash_bits]);
        }
    }

    // Cleanup.
    free(tuples);
    for (int i = 0; i < num_thread_options; i++) {
        free(conc_results[i]);
    }
    free(conc_results);
    free(global_conc_indexes);
    free(global_conc_buffers);
    free(conc_big_block);

    return 0;
}

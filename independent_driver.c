#include <stdio.h>
#include <stdlib.h>
#include "independent.h"
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
    double **indep_results = malloc(num_thread_options * sizeof(double *));
    if (!indep_results) {
        free(tuples);
        return -1;
    }
    for (int i = 0; i < num_thread_options; i++) {
        indep_results[i] = calloc(max_hash_bits - min_hash_bits + 1, sizeof(double));
    }

    // Pre-allocate memory for independent experiments.
    int max_thread_count = 0;
    for (int i = 0; i < num_thread_options; i++) {
        if (thread_options[i] > max_thread_count)
            max_thread_count = thread_options[i];
    }
    size_t worst_total_indep = (size_t)max_thread_count * TUPLE_COUNT * PARTITION_MULTIPLIER;
    tuple_t *indep_big_block = malloc(worst_total_indep * sizeof(tuple_t));
    if (!indep_big_block) {
        free(tuples);
        return -1;
    }
    int worst_indep_partitions = max_thread_count * (1 << max_hash_bits);
    tuple_t **global_indep_buffers = malloc(worst_indep_partitions * sizeof(tuple_t *));
    int *global_indep_indexes = calloc(worst_indep_partitions, sizeof(int));

    // Run Independent Experiments (each combination runs once).
    for (int t_idx = 0; t_idx < num_thread_options; t_idx++) {
        int thread_count = thread_options[t_idx];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            int partitions_per_thread = 1 << hb;
            int effective_capacity = (TUPLE_COUNT / partitions_per_thread) * PARTITION_MULTIPLIER;
            for (int thr = 0; thr < thread_count; thr++) {
                for (int part = 0; part < partitions_per_thread; part++) {
                    int index = thr * partitions_per_thread + part;
                    global_indep_buffers[index] = indep_big_block 
                        + thr * (TUPLE_COUNT * PARTITION_MULTIPLIER)
                        + part * effective_capacity;
                    global_indep_indexes[index] = 0;
                }
            }
            double throughput = 0;
            if (run_independent_timed(tuples, TUPLE_COUNT, thread_count, hb,
                                      global_indep_buffers, global_indep_indexes,
                                      effective_capacity, &throughput) != 0) {
                fprintf(stderr, "Error in independent run with %d threads and hb=%d\n", thread_count, hb);
                continue;
            }
            indep_results[t_idx][hb - min_hash_bits] = throughput;
        }
    }
    
    // Print results to stdout.
    printf("Independent Experiment Results (Throughput MT/s):\n");
    printf("Threads,HashBits,Throughput\n");
    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            printf("%d,%d,%.2f\n", thread_count, hb, indep_results[i][hb - min_hash_bits]);
        }
    }

    // Cleanup.
    free(tuples);
    for (int i = 0; i < num_thread_options; i++) {
        free(indep_results[i]);
    }
    free(indep_results);
    free(global_indep_indexes);
    free(global_indep_buffers);
    free(indep_big_block);
    return 0;
}

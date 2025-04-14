#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include "affinity.h"
#include "independent.h"
#include "tuples.h"  // For tuple_t definition
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// Structure for per-thread arguments.
typedef struct {
    int thread_id;
    tuple_t *tuples;
    int tuples_index;       // Start index (inclusive)
    int tuples_length;      // End index (exclusive)
    int partition_count;    // Number of partitions (1 << hash_bits)
    tuple_t **partition_buffers; // This thread's slice of the global partition buffers.
    int *partition_sizes;   // This thread's slice of the global partition sizes.
    int estimated_per_partition; // Maximum estimated capacity per partition.
    // Per-thread timing (recorded just before and after processing tuples).
    struct timespec start;
    struct timespec end;
} thread_args_t;

// Thread function that processes a slice of tuples and records per-thread timing.
void *write_independent_output(void *void_args) {
    if (!void_args)
        return NULL;
    thread_args_t *args = (thread_args_t *)void_args;

    // Set thread affinity for this thread.
    set_affinity(args->thread_id);

    // Record the start time immediately before processing.
    clock_gettime(CLOCK_MONOTONIC_RAW, &args->start);

    if (!args->tuples || !args->partition_buffers)
        return NULL;

    // Process tuples in the half-open range [tuples_index, tuples_length)
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition_id = hash_to_partition(args->tuples[i].key, args->partition_count);
        int idx = args->partition_sizes[partition_id];
        if (idx >= args->estimated_per_partition) {
            fprintf(stderr, "Thread %d: Partition %d overflow (idx=%d, cap=%d)\n",
                    args->thread_id, partition_id, idx, args->estimated_per_partition);
            continue;
        }
        args->partition_buffers[partition_id][idx] = args->tuples[i];
        args->partition_sizes[partition_id]++;
    }
    // Record the end time immediately after finishing processing.
    clock_gettime(CLOCK_MONOTONIC_RAW, &args->end);
    return NULL;
}

// Runs the partitioning using pthreads.
// After joining, each thread’s processing time (in ms) is computed and averaged.
// The throughput is computed as follows:
//      throughput = ((double)tuple_count / (avg_time_in_seconds)) / 1e6
int run_independent_timed(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits,
                          tuple_t **global_partition_buffers, int *global_partition_sizes,
                          int global_capacity, double *throughput) {
    if (!tuples)
        return -1;
    
    int partition_count = 1 << hash_bits;
    int effective_capacity = (tuple_count / partition_count) * PARTITION_MULTIPLIER;
    if (effective_capacity > global_capacity)
        effective_capacity = global_capacity;
    int total_threads = thread_count;
    int base_segment_size = tuple_count / total_threads;  // using half-open intervals

    // Reset the global partition sizes.
    int used_partitions = total_threads * partition_count;
    for (int i = 0; i < used_partitions; i++) {
        global_partition_sizes[i] = 0;
    }

    // Allocate arrays for thread structures and their arguments.
    thread_args_t *args = malloc(total_threads * sizeof(thread_args_t));
    pthread_t *threads = malloc(total_threads * sizeof(pthread_t));
    if (!args || !threads) {
        fprintf(stderr, "Error allocating memory for thread structures.\n");
        free(args);
        free(threads);
        return -1;
    }

    // Create threads.
    for (int i = 0; i < total_threads; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == total_threads - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].estimated_per_partition = effective_capacity;
        // Each thread gets its slice of the global buffers.
        args[i].partition_buffers = global_partition_buffers + (i * partition_count);
        args[i].partition_sizes = global_partition_sizes + (i * partition_count);

        if (pthread_create(&threads[i], NULL, write_independent_output, &args[i]) != 0) {
            fprintf(stderr, "Error creating thread %d\n", i);
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            free(args);
            free(threads);
            return -1;
        }
    }

    // Join all threads.
    for (int i = 0; i < total_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Calculate average per-thread processing time (in milliseconds)
    long total_time_ms = 0;
    for (int i = 0; i < total_threads; i++) {
        long thread_time_ms = (args[i].end.tv_sec - args[i].start.tv_sec) * 1000 +
                              (args[i].end.tv_nsec - args[i].start.tv_nsec) / 1000000;
        total_time_ms += thread_time_ms;
    }
    double avg_time_sec = (total_time_ms / (double)total_threads) / 1000.0; // convert ms to s

    *throughput = ((double)tuple_count / avg_time_sec) / 1e6;  // Throughput in millions of tuples/sec

    free(args);
    free(threads);
    return 0;
}

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
#include "thpool.h"  // Thread pool header

typedef struct {
    int thread_id;
    tuple_t *tuples;
    int tuples_index;
    int tuples_length;
    int partition_count;
    // Each thread’s slice of the global independent partition buffers.
    tuple_t **partition_buffers;
    // Each thread’s slice of the global independent indexes.
    int *partition_sizes;
    int estimated_per_partition;
} thread_args_t;

void *write_independent_output(void *void_args) {
    if (!void_args)
        return NULL;
    thread_args_t *args = (thread_args_t *)void_args;

    // Optionally set affinity.
    set_affinity(args->thread_id);

    if (!args->tuples || !args->partition_buffers)
        return NULL;

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
    return NULL;
}

void write_independent_output_wrapper(void *arg) {
    write_independent_output(arg);
}

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
    int base_segment_size = tuple_count / total_threads;

    // Reset the partition sizes.
    int used_partitions = total_threads * partition_count;
    for (int i = 0; i < used_partitions; i++) {
        global_partition_sizes[i] = 0;
    }

    // Initialize the thread pool.
    threadpool thpool = thpool_init(total_threads);
    if (!thpool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return -1;
    }

    thread_args_t args[total_threads];

    double start_time = get_time_in_seconds();
    for (int i = 0; i < total_threads; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == total_threads - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].estimated_per_partition = effective_capacity;
        args[i].partition_buffers = global_partition_buffers + (i * partition_count);
        args[i].partition_sizes = global_partition_sizes + (i * partition_count);
        thpool_add_work(thpool, write_independent_output_wrapper, (void *)&args[i]);
    }
    thpool_wait(thpool);
    double end_time = get_time_in_seconds();

    double total_time = end_time - start_time;
    *throughput = ((double)tuple_count / total_time) / 1e6;

    thpool_destroy(thpool);
    return 0;
}

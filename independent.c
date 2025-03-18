#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // for sysconf()

// Each thread writes to its own isolated output.
typedef struct {
    int thread_id;
    tuple_t *tuples;
    int tuples_index;
    int tuples_length;
    int partition_count;
    // Pointer to the thread's segment of the global partition buffer pointers.
    tuple_t **partition_buffers;
    // Pointer to the thread's segment of the global partition sizes.
    int *partition_sizes;
    // Allowed capacity per partition (estimated)
    int estimated_per_partition;
    // Time taken for the writing phase (seconds)
    double thread_time;
} thread_args_t;

void *write_independent_output(void *void_args) {
    if (!void_args)
        return NULL;
    thread_args_t *args = (thread_args_t *)void_args;
    if (!args->tuples || !args->partition_buffers)
        return NULL;

    double start = get_time_in_seconds();
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition_id = hash_to_partition((unsigned char *)&args->tuples[i].key, args->partition_count);
        int idx = args->partition_sizes[partition_id];
        if (idx >= args->estimated_per_partition) {
            fprintf(stderr, "Thread %d: Partition %d overflow (idx=%d, cap=%d)\n", args->thread_id, partition_id, idx,
                    args->estimated_per_partition);
            continue;
        }
        args->partition_buffers[partition_id][idx] = args->tuples[i];
        args->partition_sizes[partition_id]++;
    }
    double end = get_time_in_seconds();
    args->thread_time = end - start;
    printf("Thread %d finished\n", args->thread_id);
    return NULL;
}

/// run_independent_timed() performs the partitioning phase (after allocations) and computes throughput.
/// The overall throughput is computed as the sum over threads of:
///    (segment_size / thread_time) / 1e6   [in million tuples per second].
/// It returns 0 on success and sets *throughput to the computed overall throughput.
int run_independent_timed(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits, double *throughput) {
    if (!tuples)
        return -1;

    int partition_count = 1 << hash_bits;
    // Estimate capacity per partition (as given).
    int estimated_per_partition = (tuple_count / partition_count) * 2;
    if (estimated_per_partition < 1)
        estimated_per_partition = 1;

    // Total partitions across all threads.
    int total_partitions = thread_count * partition_count;
    size_t total_block_size = total_partitions * estimated_per_partition * sizeof(tuple_t);

    // Allocate one contiguous block for all partition data.
    tuple_t *big_block = malloc(total_block_size);
    if (!big_block) {
        fprintf(stderr, "Allocation failed for big_block\n");
        return -1;
    }

    // Allocate one contiguous array for partition buffer pointers.
    tuple_t **global_partition_buffers = malloc(total_partitions * sizeof(tuple_t *));
    if (!global_partition_buffers) {
        free(big_block);
        return -1;
    }

    // Allocate one contiguous array for partition sizes.
    int *global_partition_sizes = calloc(total_partitions, sizeof(int));
    if (!global_partition_sizes) {
        free(big_block);
        free(global_partition_buffers);
        return -1;
    }

    // Initialize global partition arrays.
    for (int i = 0; i < total_partitions; i++) {
        global_partition_buffers[i] = big_block + (i * estimated_per_partition);
        global_partition_sizes[i] = 0;
    }

    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    int base_segment_size = tuple_count / thread_count;

    // Start threads (allocations are done, so now only the writing phase is timed per thread).
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);

        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].estimated_per_partition = estimated_per_partition;
        // Each thread gets its slice of the global arrays.
        args[i].partition_buffers = global_partition_buffers + (i * partition_count);
        args[i].partition_sizes = global_partition_sizes + (i * partition_count);
        printf("Starting thread %d\n", args[i].thread_id);

        if (pthread_create(&threads[i], NULL, write_independent_output, &args[i]) != 0) {
            fprintf(stderr, "Thread creation failed for thread %d\n", i + 1);
            return -1;
        }
    }

    // Wait for threads to complete.
    double overall_throughput = 0.0;
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
        int segment = args[i].tuples_length - args[i].tuples_index;
        // Compute throughput for this thread in million tuples/sec.
        double thread_tp = ((double)segment / args[i].thread_time) / 1e6;
        overall_throughput += thread_tp;
    }

    *throughput = overall_throughput;

    free(big_block);
    free(global_partition_buffers);
    free(global_partition_sizes);

    return 0;
}

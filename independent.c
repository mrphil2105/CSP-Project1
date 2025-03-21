#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

typedef struct {
    int thread_id;
    tuple_t *tuples;
    int tuples_index;
    int tuples_length;
    int partition_count;
    // This thread's slice of the global independent partition buffers.
    tuple_t **partition_buffers;
    // This thread's slice of the global independent indexes.
    int *partition_sizes;
    int estimated_per_partition; // Effective capacity for this run.
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
        if (idx > args->estimated_per_partition) {
            fprintf(stderr, "Thread %d: Partition %d overflow (idx=%d, cap=%d)\n",
                    args->thread_id, partition_id, idx, args->estimated_per_partition);
            continue;
        }
        args->partition_buffers[partition_id][idx] = args->tuples[i];
        args->partition_sizes[partition_id]++;
    }
    double end = get_time_in_seconds();
    args->thread_time = end - start;
    //printf("Independent thread %d finished\n", args->thread_id);
    return NULL;
}

int run_independent_timed(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits,
                          tuple_t **global_partition_buffers, int *global_partition_sizes,
                          int global_capacity, double *throughput) {
    if (!tuples)
        return -1;
    int partition_count = 1 << hash_bits;
    // Compute effective capacity for this run based on current hb.
    int effective_capacity = (tuple_count / (1 << hash_bits)) * PARTITION_MULTIPLIER;
    if (effective_capacity > global_capacity)
        effective_capacity = global_capacity;
    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    int base_segment_size = tuple_count / thread_count;
    double overall_throughput = 0.0;
    int used_partitions = thread_count * partition_count;
    for (int i = 0; i < used_partitions; i++) {
        global_partition_sizes[i] = 0;
    }
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].estimated_per_partition = effective_capacity;
        // Each thread gets its slice of the global independent arrays.
        args[i].partition_buffers = global_partition_buffers + (i * partition_count);
        args[i].partition_sizes = global_partition_sizes + (i * partition_count);
        if (pthread_create(&threads[i], NULL, write_independent_output, &args[i]) != 0) {
            fprintf(stderr, "Independent thread creation failed for thread %d\n", i + 1);
            return -1;
        }
    }
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
        int seg = args[i].tuples_length - args[i].tuples_index;
        double thread_tp = ((double)seg / args[i].thread_time) / 1e6;
        overall_throughput += thread_tp;
    }
    *throughput = overall_throughput;
    return 0;
}

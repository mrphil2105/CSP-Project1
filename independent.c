#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifdef __linux__
#include <sched.h>
#endif

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
    int estimated_per_partition; // Effective capacity for this run.
} thread_args_t;

void *write_independent_output(void *void_args) {
    if (!void_args)
        return NULL;
    thread_args_t *args = (thread_args_t *)void_args;
#ifdef __linux__
    int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (num_cores < 1)
        num_cores = 1;
    int cpu_id = (args->thread_id - 1) % 32;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        perror("pthread_setaffinity_np");
    }
#endif
    if (!args->tuples || !args->partition_buffers)
        return NULL;
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition_id = hash_to_partition((unsigned char *)&args->tuples[i].key, args->partition_count);
        int idx = args->partition_sizes[partition_id];
        if (idx >= args->estimated_per_partition) {
            // Skip if the buffer is full.
            continue;
        }
        args->partition_buffers[partition_id][idx] = args->tuples[i];
        args->partition_sizes[partition_id]++;
    }
    printf("Independent thread %d finished\n", args->thread_id);
    return NULL;
}

int run_independent_timed(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits,
                          tuple_t **global_partition_buffers, int *global_partition_sizes,
                          int global_capacity, double *throughput) {
    if (!tuples)
        return -1;
    int partition_count = 1 << hash_bits;
    // Effective capacity per buffer is based on the expected number of tuples per partition.
    int effective_capacity = (tuple_count / (1 << hash_bits)) * PARTITION_MULTIPLIER;
    if (effective_capacity > global_capacity)
        effective_capacity = global_capacity;
    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    int base_segment_size = tuple_count / thread_count;
    
    // Initialize each thread's partition sizes.
    int total_fragments = thread_count * partition_count;
    for (int i = 0; i < total_fragments; i++) {
        global_partition_sizes[i] = 0;
    }
    
    double start = get_time_in_seconds();
    
    // Launch partitioning threads.
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].estimated_per_partition = effective_capacity;
        // Each thread gets its slice of the global arrays.
        args[i].partition_buffers = global_partition_buffers + (i * partition_count);
        args[i].partition_sizes = global_partition_sizes + (i * partition_count);
        if (pthread_create(&threads[i], NULL, write_independent_output, &args[i]) != 0) {
            fprintf(stderr, "Independent thread creation failed for thread %d\n", i + 1);
            return -1;
        }
    }
    
    // Wait for all threads to complete.
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    double end = get_time_in_seconds();
    double partition_time = end - start;
    
    // Throughput in million tuples per second.
    *throughput = ((double)tuple_count / partition_time) / 1e6;
    printf("Independent partitioning time: %.6f sec, Throughput: %.2f MT/s\n", partition_time, *throughput);
    
    return 0;
}

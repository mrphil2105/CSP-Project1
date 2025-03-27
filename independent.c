#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include "thpool.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifdef __linux__
#include <sched.h>

#endif

typedef struct thpool_* threadpool;
// Structure for passing arguments to each partitioning job.
typedef struct {
    int thread_id;
    tuple_t *tuples;
    int tuples_index;
    int tuples_length;
    int partition_count;
    // Pointer to this thread's slice of the global partition buffers.
    tuple_t **partition_buffers;
    // Pointer to this thread's slice of the global partition sizes.
    int *partition_sizes;
    int estimated_per_partition; // Effective capacity for this run.
    pthread_barrier_t *barrier;
} thread_args_t;



// Original worker function with a void* return type.
void *write_independent_output(void *void_args) {
    if (!void_args)
        return NULL;
    thread_args_t *args = (thread_args_t *)void_args;
    if (!args->tuples || !args->partition_buffers)
        return NULL;
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition_id = hash_to_partition((unsigned char *)&args->tuples[i].key, args->partition_count);
        int idx = args->partition_sizes[partition_id];
        if (idx >= args->estimated_per_partition) {
            // Skip if the buffer is full.
            printf("Skipping! Partition %d buffer is full\n", partition_id);
            continue;
        }
        args->partition_buffers[partition_id][idx] = args->tuples[i];
        args->partition_sizes[partition_id]++;
    }
    printf("Independent thread %d finished\n", args->thread_id);
    return NULL;
}

// Wrapper that matches the thread pool's expected signature (returns void)
void write_independent_output_wrapper(void *args) {
    // Call the original function and ignore its return value.
    write_independent_output(args);
}

int run_independent_timed(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits,
    tuple_t **global_partition_buffers, int *global_partition_sizes, 
    int global_capacity, double *throughput) {
    
    if (!tuples) return -1;
    int partition_count = 1 << hash_bits;
    
    // Calculate effective capacity per partition buffer.
    int effective_capacity = (tuple_count / partition_count) * PARTITION_MULTIPLIER;
    if (effective_capacity > global_capacity)
        effective_capacity = global_capacity;

    printf("run_independent_timed: hash_bits=%d, partition_count=%d, "
        "effective_capacity=%d, tuple_count=%d\n",
        hash_bits, partition_count, effective_capacity, tuple_count);


    // Initialize thread pool outside timing scope.
    threadpool pool = thpool_init(thread_count);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return -1;
    }

    thread_args_t args[thread_count];
    int base_segment_size = tuple_count / thread_count;
    
    // Reset partition sizes.
    int total_fragments = thread_count * partition_count;
    memset(global_partition_sizes, 0, total_fragments * sizeof(int));

    // Create a barrier to synchronize threads.
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, thread_count);

    // ***** Warm-up run *****
    // Enqueue jobs for warm-up (results will be discarded).
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].estimated_per_partition = effective_capacity;
        args[i].partition_buffers = global_partition_buffers + (i * partition_count);
        args[i].partition_sizes = global_partition_sizes + (i * partition_count);
        args[i].barrier = &barrier;
        thpool_add_work(pool, write_independent_output_wrapper, (void *)&args[i]);
    }
    // Wait for warm-up run to complete.
    thpool_wait(pool);
    
    // Reset partition sizes after warm-up so that the measured run starts fresh.
    memset(global_partition_sizes, 0, total_fragments * sizeof(int));

    // ***** Measured run *****
    double start = get_time_in_seconds();
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].estimated_per_partition = effective_capacity;
        args[i].partition_buffers = global_partition_buffers + (i * partition_count);
        args[i].partition_sizes = global_partition_sizes + (i * partition_count);
        args[i].barrier = &barrier;
        thpool_add_work(pool, write_independent_output_wrapper, (void *)&args[i]);
    }
    // Wait until all partitioning jobs are finished.
    thpool_wait(pool);
    double end = get_time_in_seconds();
    double partition_time = end - start;
    
    // Compute throughput in million tuples per second.
    *throughput = ((double)tuple_count / partition_time) / 1e6;
    printf("Independent partitioning time: %.6f sec, Throughput: %.2f MT/s\n", partition_time, *throughput);

    // Cleanup.
    pthread_barrier_destroy(&barrier);
    thpool_destroy(pool);

    return 0;
}

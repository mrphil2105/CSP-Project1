#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>  // for sysconf()

#include "project.h"
#include "utils.h"

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
    // The allowed number of tuples per partition (estimated)
    int estimated_per_partition;
} thread_args_t;

void *write_independent_output(void *void_args) {
    if (!void_args)
        return NULL;
    thread_args_t *args = (thread_args_t *)void_args;
    if (!args->tuples || !args->partition_buffers)
        return NULL;
    
    
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        // Use hash_to_partition with proper casting (key is an unsigned char pointer)
        int partition_id = hash_to_partition((unsigned char *)&args->tuples[i].key, args->partition_count);
        int idx = args->partition_sizes[partition_id];
        if (idx >= args->estimated_per_partition) {
            fprintf(stderr, "Thread %d: Partition %d overflow (idx=%d, capacity=%d)\n",
                    args->thread_id, partition_id, idx, args->estimated_per_partition);
            continue;
        }
        args->partition_buffers[partition_id][idx] = args->tuples[i];
        args->partition_sizes[partition_id]++;
    }
    
    printf("thread %d finished\n", args->thread_id);
    return NULL;
}

int run_independent(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits) {
    if (!tuples)
        return -1;
    
    // Number of partitions per thread.
    int partition_count = 1 << hash_bits;
    // Calculate estimated capacity per partition (using your formula).
    int estimated_per_partition = (tuple_count / partition_count) * 2;
    if (estimated_per_partition < 1)
        estimated_per_partition = 1;
    
    // Total number of partitions across all threads.
    int total_partitions = thread_count * partition_count;
    size_t total_block_size = total_partitions * estimated_per_partition * sizeof(tuple_t);
    
    // Allocate one contiguous block for all partition data.
    tuple_t *big_block = malloc(total_block_size);
    if (!big_block) {
        fprintf(stderr, "Memory allocation failed for the big block\n");
        return -1;
    }
    
    // Allocate one contiguous array for all partition buffer pointers.
    tuple_t **global_partition_buffers = malloc(total_partitions * sizeof(tuple_t *));
    if (!global_partition_buffers) {
        fprintf(stderr, "Memory allocation failed for global_partition_buffers\n");
        free(big_block);
        return -1;
    }
    
    // Allocate one contiguous array for all partition sizes.
    int *global_partition_sizes = calloc(total_partitions, sizeof(int));
    if (!global_partition_sizes) {
        fprintf(stderr, "Memory allocation failed for global_partition_sizes\n");
        free(big_block);
        free(global_partition_buffers);
        return -1;
    }
    
    // Initialize global partition pointers and sizes.
    for (int i = 0; i < total_partitions; i++) {
        global_partition_buffers[i] = big_block + (i * estimated_per_partition);
        global_partition_sizes[i] = 0;
    }
    
    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    int base_segment_size = tuple_count / thread_count;
    
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].estimated_per_partition = estimated_per_partition;
        
        // Instead of allocating separately, assign each thread its portion of the global arrays.
        args[i].partition_buffers = global_partition_buffers + (i * partition_count);
        args[i].partition_sizes = global_partition_sizes + (i * partition_count);
        
        printf("starting thread %d\n", args[i].thread_id);
        if (pthread_create(&threads[i], NULL, write_independent_output, &args[i]) != 0) {
            fprintf(stderr, "Thread creation failed for thread %d\n", i + 1);
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            free(big_block);
            free(global_partition_buffers);
            free(global_partition_sizes);
            return -1;
        }
    }
    
    // Join all threads.
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    
    free(big_block);
    free(global_partition_buffers);
    free(global_partition_sizes);
    
    return 0;
}

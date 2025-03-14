#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#include "project.h"
#include "utils.h"

#define INITIAL_CAPACITY 8  // Start small to reduce allocation overhead

// Each thread writes to its own isolated output.
typedef struct {
    int thread_id;
    tuple_t *tuples;
    int tuples_index;
    int tuples_length;
    int partition_count;
    int output_count;
    tuple_t **partition_buffers;
    int *partition_sizes;
    int *partition_capacities;
} thread_args_t;

void *write_independent_output(void *void_args) {
    if (void_args == NULL) {
        return NULL;
    }
    thread_args_t *args = (thread_args_t *)void_args;
    if (args->tuples == NULL || args->partition_buffers == NULL)
        return NULL;
    
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition_id = hash_to_partition(args->tuples[i].key, args->partition_count);
        int current_size = args->partition_sizes[partition_id];
        
        // If the partition buffer is full, double its capacity
        if (current_size >= args->partition_capacities[partition_id]) {
            int new_capacity = args->partition_capacities[partition_id] * 2;
            tuple_t *new_buffer = realloc(args->partition_buffers[partition_id],
                                          sizeof(tuple_t) * new_capacity);
            if (new_buffer == NULL) {
                fprintf(stderr, "Realloc failed for thread %d, partition %d\n",
                        args->thread_id, partition_id);
                continue;  // Skip writing this tuple; or handle error as needed
            }
            args->partition_buffers[partition_id] = new_buffer;
            args->partition_capacities[partition_id] = new_capacity;
        }
        
        // Write the tuple into the partition buffer.
        args->partition_buffers[partition_id][current_size] = args->tuples[i];
        args->partition_sizes[partition_id]++;
    }
    
    return NULL;
}

int run_independent(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits) {
    if (!tuples) {
        return -1;
    }
    
    // Number of partitions is 2^hash_bits.
    int partition_count = 1 << hash_bits;
    pthread_t threads[thread_count];
    thread_args_t args[thread_count];

    // Divide the input among threads.
    int base_segment_size = tuple_count / thread_count;
    
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].output_count = 0;
        
        // Allocate arrays for partition buffers, sizes, and capacities.
        args[i].partition_buffers = malloc(sizeof(tuple_t*) * partition_count);
        args[i].partition_sizes = calloc(partition_count, sizeof(int));
        args[i].partition_capacities = malloc(sizeof(int) * partition_count);
        if (!args[i].partition_buffers || !args[i].partition_sizes || !args[i].partition_capacities) {
            fprintf(stderr, "Memory allocation failed for thread %d partition arrays\n", i + 1);
            return -1;
        }
        
        // Instead of estimating a huge capacity, start with a small fixed size.
        for (int p = 0; p < partition_count; p++) {
            args[i].partition_capacities[p] = INITIAL_CAPACITY;
            args[i].partition_buffers[p] = malloc(sizeof(tuple_t) * INITIAL_CAPACITY);
            if (!args[i].partition_buffers[p]) {
                fprintf(stderr, "Memory allocation failed for thread %d partition %d\n", i + 1, p);
                return -1;
            }
        }
        
        // Launch the thread.
        if (pthread_create(&threads[i], NULL, write_independent_output, &args[i]) != 0) {
            fprintf(stderr, "Thread creation failed for thread %d\n", i + 1);
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            return -1;
        }
    }
    
    // Join all threads.
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Free resources.
    for (int i = 0; i < thread_count; i++) {
        for (int p = 0; p < partition_count; p++) {
            free(args[i].partition_buffers[p]);
        }
        free(args[i].partition_buffers);
        free(args[i].partition_sizes);
        free(args[i].partition_capacities);
    }
    
    return 0;
}

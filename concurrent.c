#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "project.h"
#include "utils.h"

typedef struct {
    int thread_id;
    tuple_t *tuples;
    int tuples_index;
    int tuples_length;
    int partition_count;
    tuple_t **partitions;
    int *partition_indexes;
    pthread_mutex_t *partition_mutexes;
} thread_args_t;

// This function writes tuples into partitions.
// It uses the preallocated contiguous memory provided by allocate_partitions,
// so no reallocation is necessary.
void *write_to_partitions(void *void_args) {
    if (void_args == NULL) {
        return NULL;
    }
    thread_args_t *args = (thread_args_t *)void_args;
    if (args->tuples == NULL || args->partitions == NULL ||
        args->partition_indexes == NULL || args->partition_mutexes == NULL) {
        return NULL;
    }
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition = hash_to_partition(args->tuples[i].key, args->partition_count);
        pthread_mutex_lock(&args->partition_mutexes[partition]);
        int partition_index = args->partition_indexes[partition]++;
        pthread_mutex_unlock(&args->partition_mutexes[partition]);
        args->partitions[partition][partition_index] = args->tuples[i];
    }
    //printf("thread %d finished\n", args->thread_id);
    return NULL;
}

// Allocate a contiguous block for all partitions.
// The allocation is done once, and each partition is assigned a slice of the block.
tuple_t **allocate_partitions(int partition_count, int tuple_count) {
    int estimated_per_partition = (tuple_count / partition_count) * 2;
    size_t buffer_size = partition_count * sizeof(tuple_t *) +
                         partition_count * estimated_per_partition * sizeof(tuple_t);
    unsigned char *buffer = malloc(buffer_size);
    if (buffer == NULL) {
        return NULL;
    }
    tuple_t **partitions = (tuple_t **)buffer;
    buffer += partition_count * sizeof(tuple_t *);
    for (int i = 0; i < partition_count; i++) {
        partitions[i] = (tuple_t *)buffer;
        buffer += estimated_per_partition * sizeof(tuple_t);
    }
    return partitions;
}

int run_concurrent(tuple_t *tuples, int tuple_count, int thread_count, int partition_count) {
    if (!tuples) {
        return -1;
    }
    
    // Preallocate partitions in one contiguous block.
    tuple_t **partitions = allocate_partitions(partition_count, tuple_count);
    if (!partitions) {
        return -1;
    }
    
    // Initialize partition indexes.
    int *partition_indexes = malloc(sizeof(int) * partition_count);
    if (partition_indexes == NULL) {
        fprintf(stderr, "Failed to allocate memory for partition_indexes\n");
        free(partitions);
        return -1;
    }
    for (int i = 0; i < partition_count; i++) {
        partition_indexes[i] = 0;
    }

    // Create an array of mutexes for each partition.
    pthread_mutex_t *partition_mutexes = malloc(sizeof(pthread_mutex_t) * partition_count);
    if (partition_mutexes == NULL) {
        fprintf(stderr, "Failed to allocate memory for partition_mutexes\n");
        free(partition_indexes);
        free(partitions);
        return -1;
    }
    for (int i = 0; i < partition_count; i++) {
        if (pthread_mutex_init(&partition_mutexes[i], NULL) != 0) {
            fprintf(stderr, "Mutex init failed for partition %d\n", i);
            for (int j = 0; j < i; j++) {
                pthread_mutex_destroy(&partition_mutexes[j]);
            }
            free(partition_mutexes);
            free(partition_indexes);
            free(partitions);
            return -1;
        }   
    }
    
    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    int segment_size = tuple_count / thread_count;
    
    // Launch threads to work on disjoint segments of the tuples.
    for (int i = 0; i < thread_count; i++) {
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = segment_size * i;
        args[i].tuples_length = (i == thread_count - 1) ? tuple_count : segment_size * (i + 1);
        args[i].partition_count = partition_count;
        args[i].partitions = partitions;
        args[i].partition_indexes = partition_indexes;
        args[i].partition_mutexes = partition_mutexes;
        
        //printf("starting thread %d\n", args[i].thread_id);
        if (pthread_create(&threads[i], NULL, write_to_partitions, &args[i]) != 0) {
            fprintf(stderr, "Thread creation failed for thread %d\n", args[i].thread_id);
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            free(partitions);
            free(partition_indexes);
            free(partition_mutexes);
            return -1;
        }
    }
    
    // Wait for all threads to finish.
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Clean up mutexes.
    for (int i = 0; i < partition_count; i++) {
        pthread_mutex_destroy(&partition_mutexes[i]);
    }
    
    // Free allocated resources.
    free(partitions);
    free(partition_indexes);
    free(partition_mutexes);
    
    return 0;
}

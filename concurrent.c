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
    printf("thread %d finished\n", args->thread_id);
    return NULL;
}

tuple_t **allocate_partitions(int partition_count, int tuple_count) {
    int estimated_per_partition = (tuple_count / partition_count) + 16;
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
    
    // Allocate partitions with worst-case capacity per partition.
    tuple_t **partitions = allocate_partitions(partition_count, tuple_count);
    if (!partitions) {
        return -1;
    }
    
    // Local array for partition indexes; each starts at 0.
    int partition_indexes[partition_count];
    for (int i = 0; i < partition_count; i++) {
        partition_indexes[i] = 0;
    }
    
    // Local array for mutexes (one per partition).
    pthread_mutex_t partition_mutexes[partition_count];
    for (int i = 0; i < partition_count; i++) {
        if (pthread_mutex_init(&partition_mutexes[i], NULL) != 0) {
            fprintf(stderr, "Mutex init failed for partition %d\n", i);
            for (int j = 0; j < i; j++) {
                pthread_mutex_destroy(&partition_mutexes[j]);
            }
            free(partitions);
            return -1;
        }
    }
    
    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    int segment_size = tuple_count / thread_count;
    
    // Launch threads over disjoint segments of the tuple array.
    for (int i = 0; i < thread_count; i++) {
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = segment_size * i;
        args[i].tuples_length = (i == thread_count - 1) ? tuple_count : segment_size * (i + 1);
        args[i].partition_count = partition_count;
        args[i].partitions = partitions;
        args[i].partition_indexes = partition_indexes;
        args[i].partition_mutexes = partition_mutexes;
        
        printf("starting thread %d\n", args[i].thread_id);
        if (pthread_create(&threads[i], NULL, write_to_partitions, &args[i]) != 0) {
            fprintf(stderr, "Thread creation failed for thread %d\n", args[i].thread_id);
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            free(partitions);
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
    
    // In a complete implementation you might further process or return partitions.
    free(partitions);
    
    return 0;
}

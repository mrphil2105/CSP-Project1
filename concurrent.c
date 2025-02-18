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
    if (args->tuples == NULL || args->partitions == NULL) {
        return NULL;
    }
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition = hash_to_partition(args->tuples[i].key, args->partition_count);
        pthread_mutex_t partition_mutex = args->partition_mutexes[partition];
        pthread_mutex_lock(&partition_mutex);
        int partition_index = args->partition_indexes[partition]++;
        pthread_mutex_unlock(&partition_mutex);
        args->partitions[partition][partition_index] = args->tuples[i];
    }
    printf("thread %d finished\n", args->thread_id);
    return NULL;
}

tuple_t **allocate_partitions(int partition_count, int tuple_count) {
    int buffer_size = sizeof(tuple_t *) * partition_count + tuple_count * partition_count;
    unsigned char *buffer = malloc(buffer_size);
    if (buffer == NULL) {
        return NULL;
    }
    tuple_t **partitions = (tuple_t **)buffer;
    buffer += sizeof(tuple_t *) * partition_count;
    for (int i = 0; i < partition_count; i++) {
        partitions[i] = (tuple_t *)buffer;
        buffer += tuple_count;
    }
    return partitions;
}

int run_concurrent(tuple_t *tuples, int tuple_count, int thread_count, int partition_count) {
    if (!tuples) {
        return -1;
    }
    tuple_t **partitions = allocate_partitions(partition_count, tuple_count);
    if (!partitions) {
        return -1;
    }
    int partition_indexes[partition_count];
    for (int i = 0; i < partition_count; i++) {
        partition_indexes[i] = 0;
    }
    pthread_mutex_t partition_mutexes[partition_count];
    for (int i = 0; i < partition_count; i++) {
        if (pthread_mutex_init(&partition_mutexes[i], NULL) != 0) {
            printf("mutex init failed\n");
            for (int j = 0; j < i; j++) {
                pthread_mutex_destroy(&partition_mutexes[j]);
            }
            return -1;
        }
    }
    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    for (int i = 0; i < thread_count; i++) {
        int segment_size = tuple_count / thread_count;
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = segment_size * i;
        args[i].tuples_length = i == tuple_count - 1 ? tuple_count : segment_size * (i + 1);
        args[i].partition_count = partition_count;
        args[i].partitions = partitions;
        args[i].partition_indexes = partition_indexes;
        args[i].partition_mutexes = partition_mutexes;
        printf("starting thread %d\n", args[i].thread_id);
        if (pthread_create(&threads[i], NULL, &write_to_partitions, &args[i]) != 0) {
            printf("thread create failed\n");
            for (int j = 0; j < i; j++) {
                pthread_join(threads[i], NULL);
            }
            return -1;
        }
    }
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    return 0;
}

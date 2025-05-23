#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include "affinity.h"
#include "concurrent.h"
#include "tuples.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct {
    int thread_id;
    tuple_t *tuples;
    int tuples_index;
    int tuples_length;
    int partition_count;
    tuple_t **partitions;
    int *partition_indexes;
    pthread_mutex_t *partition_mutexes;
    struct timespec start;
    struct timespec end;
} thread_args_t;

void *write_to_partitions(void *void_args) {
    if (!void_args) return NULL;
    thread_args_t *args = (thread_args_t *)void_args;

    set_affinity(args->thread_id);

    if (!args->tuples || !args->partitions || !args->partition_indexes || !args->partition_mutexes)
        return NULL;

    clock_gettime(CLOCK_MONOTONIC, &args->start);
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition = hash_to_partition(args->tuples[i].key, args->partition_count);
        pthread_mutex_lock(&args->partition_mutexes[partition]);
        int idx = args->partition_indexes[partition]++;
        pthread_mutex_unlock(&args->partition_mutexes[partition]);
        args->partitions[partition][idx] = args->tuples[i];
    }
    clock_gettime(CLOCK_MONOTONIC, &args->end);

    return NULL;
}

int run_concurrent_timed(tuple_t *tuples, int tuple_count, int thread_count, int partition_count,
                         tuple_t **global_partition_buffers, int *global_partition_indexes,
                         int global_capacity, double *throughput) {
    if (!tuples) return -1;

    int effective_capacity = (tuple_count / partition_count) * PARTITION_MULTIPLIER;
    if (effective_capacity > global_capacity)
        effective_capacity = global_capacity;

    for (int i = 0; i < partition_count; i++)
        global_partition_indexes[i] = 0;

    pthread_mutex_t *mutexes = malloc(partition_count * sizeof(pthread_mutex_t));
    if (!mutexes) return -1;

    for (int i = 0; i < partition_count; i++)
        pthread_mutex_init(&mutexes[i], NULL);

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
        args[i].partitions = global_partition_buffers;
        args[i].partition_indexes = global_partition_indexes;
        args[i].partition_mutexes = mutexes;
        pthread_create(&threads[i], NULL, write_to_partitions, &args[i]);
    }

    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    long avg_time = 0;
    for (int i = 0; i < thread_count; i++) {
        long thread_time = (args[i].end.tv_sec - args[i].start.tv_sec) * 1000 +
                           (args[i].end.tv_nsec - args[i].start.tv_nsec) / 1000000;
        avg_time += thread_time;
    }
    avg_time /= thread_count;

    *throughput = ((double)tuple_count / (avg_time / 1000.0)) / 1e6;

    for (int i = 0; i < partition_count; i++)
        pthread_mutex_destroy(&mutexes[i]);
    free(mutexes);

    return 0;
}

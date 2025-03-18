#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
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
    double thread_time; // Per-thread processing time
} thread_args_t;

void *write_to_partitions(void *void_args) {
    if (!void_args)
        return NULL;
    thread_args_t *args = (thread_args_t *)void_args;
    if (!args->tuples || !args->partitions || !args->partition_indexes || !args->partition_mutexes)
        return NULL;

    double start = get_time_in_seconds();
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition = hash_to_partition(args->tuples[i].key, args->partition_count);
        pthread_mutex_lock(&args->partition_mutexes[partition]);
        int partition_index = args->partition_indexes[partition]++;
        pthread_mutex_unlock(&args->partition_mutexes[partition]);
        args->partitions[partition][partition_index] = args->tuples[i];
    }
    double end = get_time_in_seconds();
    args->thread_time = end - start;
    printf("Thread %d finished\n", args->thread_id);
    return NULL;
}

tuple_t **allocate_partitions(int partition_count, int tuple_count) {
    int estimated_per_partition = (tuple_count / partition_count) * 2;
    size_t buffer_size =
        partition_count * sizeof(tuple_t *) + partition_count * estimated_per_partition * sizeof(tuple_t);
    unsigned char *buffer = malloc(buffer_size);
    if (!buffer)
        return NULL;
    tuple_t **partitions = (tuple_t **)buffer;
    buffer += partition_count * sizeof(tuple_t *);
    for (int i = 0; i < partition_count; i++) {
        partitions[i] = (tuple_t *)buffer;
        buffer += estimated_per_partition * sizeof(tuple_t);
    }
    return partitions;
}

int run_concurrent_timed(tuple_t *tuples, int tuple_count, int thread_count, int partition_count, double *throughput) {
    if (!tuples)
        return -1;

    // Allocate partitions outside the timed region.
    tuple_t **partitions = allocate_partitions(partition_count, tuple_count);
    if (!partitions)
        return -1;

    int *partition_indexes = malloc(sizeof(int) * partition_count);
    if (!partition_indexes) {
        free(partitions);
        return -1;
    }
    for (int i = 0; i < partition_count; i++)
        partition_indexes[i] = 0;

    pthread_mutex_t *partition_mutexes = malloc(sizeof(pthread_mutex_t) * partition_count);
    if (!partition_mutexes) {
        free(partition_indexes);
        free(partitions);
        return -1;
    }
    for (int i = 0; i < partition_count; i++) {
        if (pthread_mutex_init(&partition_mutexes[i], NULL) != 0) {
            for (int j = 0; j < i; j++)
                pthread_mutex_destroy(&partition_mutexes[j]);
            free(partition_mutexes);
            free(partition_indexes);
            free(partitions);
            return -1;
        }
    }

    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    int segment_size = tuple_count / thread_count;

    for (int i = 0; i < thread_count; i++) {
        int start_index = segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + segment_size);
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].partitions = partitions;
        args[i].partition_indexes = partition_indexes;
        args[i].partition_mutexes = partition_mutexes;
        printf("Starting thread %d\n", args[i].thread_id);

        if (pthread_create(&threads[i], NULL, write_to_partitions, &args[i]) != 0) {
            fprintf(stderr, "Thread creation failed for thread %d\n", args[i].thread_id);
            for (int j = 0; j < i; j++)
                pthread_join(threads[j], NULL);
            free(partition_mutexes);
            free(partition_indexes);
            free(partitions);
            return -1;
        }
    }

    double overall_throughput = 0.0;
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
        int segment = args[i].tuples_length - args[i].tuples_index;
        double thread_tp = ((double)segment / args[i].thread_time) / 1e6;
        overall_throughput += thread_tp;
    }

    *throughput = overall_throughput;

    for (int i = 0; i < partition_count; i++) {
        pthread_mutex_destroy(&partition_mutexes[i]);
    }
    free(partition_mutexes);
    free(partition_indexes);
    free(partitions);

    return 0;
}

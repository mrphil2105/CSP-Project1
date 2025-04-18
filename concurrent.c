#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include "affinity.h"
#include "concurrent.h"
#include "tuples.h"  // In case tuple_t definition is needed
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include "thpool.h"  // Thread pool header

typedef struct {
    int thread_id;
    tuple_t *tuples;
    int tuples_index;
    int tuples_length;
    int partition_count;
    // Shared partitions.
    tuple_t **partitions;
    int *partition_indexes;
    pthread_mutex_t *partition_mutexes;
} thread_args_t;

void *write_to_partitions(void *void_args) {
    if (!void_args)
        return NULL;
    thread_args_t *args = (thread_args_t *)void_args;

    // Optionally set affinity.
    set_affinity(args->thread_id);
    if (!args->tuples || !args->partitions || !args->partition_indexes || !args->partition_mutexes)
        return NULL;

    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition = hash_to_partition(args->tuples[i].key, args->partition_count);
        pthread_mutex_lock(&args->partition_mutexes[partition]);
        int idx = args->partition_indexes[partition]++;
        pthread_mutex_unlock(&args->partition_mutexes[partition]);
        args->partitions[partition][idx] = args->tuples[i];
    }
    return NULL;
}

void write_to_partitions_wrapper(void *arg) {
    write_to_partitions(arg);
}

int run_concurrent_timed(tuple_t *tuples, int tuple_count, int thread_count, int partition_count,
                         tuple_t **global_partition_buffers, int *global_partition_indexes,
                         int global_capacity, double *throughput) {
    if (!tuples)
        return -1;
    int effective_capacity = (tuple_count / partition_count) * PARTITION_MULTIPLIER;
    if (effective_capacity > global_capacity)
        effective_capacity = global_capacity;
    int total_threads = thread_count;
    int base_segment_size = tuple_count / total_threads;

    // Reset partition indexes.
    for (int i = 0; i < partition_count; i++) {
        global_partition_indexes[i] = 0;
    }

    // Allocate and initialize mutexes.
    pthread_mutex_t *mutexes = malloc(partition_count * sizeof(pthread_mutex_t));
    if (!mutexes) {
        fprintf(stderr, "Failed to allocate mutexes.\n");
        return -1;
    }
    for (int i = 0; i < partition_count; i++) {
        if (pthread_mutex_init(&mutexes[i], NULL) != 0) {
            fprintf(stderr, "Mutex initialization failed for partition %d\n", i);
            for (int j = 0; j < i; j++)
                pthread_mutex_destroy(&mutexes[j]);
            free(mutexes);
            return -1;
        }
    }

    // Initialize the thread pool.
    threadpool thpool = thpool_init(total_threads);
    if (!thpool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        for (int i = 0; i < partition_count; i++)
            pthread_mutex_destroy(&mutexes[i]);
        free(mutexes);
        return -1;
    }

    thread_args_t args[total_threads];

    double start_time = get_time_in_seconds();
    for (int i = 0; i < total_threads; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == total_threads - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].partitions = global_partition_buffers;
        args[i].partition_indexes = global_partition_indexes;
        args[i].partition_mutexes = mutexes;
        thpool_add_work(thpool, write_to_partitions_wrapper, (void *)&args[i]);
    }
    thpool_wait(thpool);
    double end_time = get_time_in_seconds();

    double total_time = end_time - start_time;
    *throughput = ((double)tuple_count / total_time) / 1e6;

    thpool_destroy(thpool);
    for (int i = 0; i < partition_count; i++) {
        pthread_mutex_destroy(&mutexes[i]);
    }
    free(mutexes);
    return 0;
}

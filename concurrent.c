#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include "concurrent.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
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
    // In concurrent mode, all threads share the same global partitions.
    tuple_t **partitions;
    int *partition_indexes;
    pthread_mutex_t *partition_mutexes;
} thread_args_t;

void *write_to_partitions(void *void_args) {
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
    if (!args->tuples || !args->partitions || !args->partition_indexes || !args->partition_mutexes)
        return NULL;
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition = hash_to_partition(args->tuples[i].key, args->partition_count);
        pthread_mutex_lock(&args->partition_mutexes[partition]);
        int idx = args->partition_indexes[partition]++;
        pthread_mutex_unlock(&args->partition_mutexes[partition]);
        args->partitions[partition][idx] = args->tuples[i];
    }
    printf("Concurrent thread %d finished\n", args->thread_id);
    return NULL;
}

int run_concurrent_timed(tuple_t *tuples, int tuple_count, int thread_count, int partition_count,
                         tuple_t **global_partition_buffers, int *global_partition_indexes,
                         int global_capacity, double *throughput) {
    if (!tuples)
        return -1;
    
    // Compute effective capacity for this run.
    int effective_capacity = (tuple_count / partition_count) * PARTITION_MULTIPLIER;
    if (effective_capacity > global_capacity)
        effective_capacity = global_capacity;
    
    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    int base_segment_size = tuple_count / thread_count;
    
    // Reset the indexes for each partition.
    for (int i = 0; i < partition_count; i++) {
        global_partition_indexes[i] = 0;
    }
    
    // Allocate and initialize mutexes for each partition.
    pthread_mutex_t *mutexes = malloc(partition_count * sizeof(pthread_mutex_t));
    if (!mutexes) {
        fprintf(stderr, "Failed to allocate mutexes.\n");
        return -1;
    }
    for (int i = 0; i < partition_count; i++) {
        if (pthread_mutex_init(&mutexes[i], NULL) != 0) {
            fprintf(stderr, "Mutex initialization failed for partition %d\n", i);
            for (int j = 0; j < i; j++) {
                pthread_mutex_destroy(&mutexes[j]);
            }
            free(mutexes);
            return -1;
        }
    }
    
    double start = get_time_in_seconds();
    
    // Create threads.
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].partitions = global_partition_buffers; // Shared among threads.
        args[i].partition_indexes = global_partition_indexes; // Shared.
        args[i].partition_mutexes = mutexes; // Shared mutexes.
        
        if (pthread_create(&threads[i], NULL, write_to_partitions, &args[i]) != 0) {
            fprintf(stderr, "Concurrent thread creation failed for thread %d\n", args[i].thread_id);
            for (int j = 0; j < partition_count; j++) {
                pthread_mutex_destroy(&mutexes[j]);
            }
            free(mutexes);
            return -1;
        }
    }
    
    // Wait for all threads to complete.
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    double end = get_time_in_seconds();
    double total_time = end - start;
    
    // Compute throughput in million tuples per second.
    *throughput = ((double)tuple_count / total_time) / 1e6;
    printf("Concurrent partitioning time: %.6f sec, Throughput: %.2f MT/s\n", total_time, *throughput);
    
    // Cleanup mutexes.
    for (int i = 0; i < partition_count; i++) {
        pthread_mutex_destroy(&mutexes[i]);
    }
    free(mutexes);
    
    return 0;
}

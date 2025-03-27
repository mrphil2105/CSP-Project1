#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include "concurrent.h"
#include <pthread.h>
#include "thpool.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#ifdef __linux__
#include <sched.h>
#endif

typedef struct thpool_* threadpool;

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
    pthread_barrier_t *barrier;
} thread_args_t;

/* Worker function that partitions tuples concurrently */
void *write_to_partitions(void *void_args) {
    if (!void_args) return NULL;
    thread_args_t *args = (thread_args_t *)void_args;

    // Wait for all threads to be ready before starting the work.
    pthread_barrier_wait(args->barrier);

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

// Wrapper that matches the thread pool's expected signature (returns void).
void write_to_partitions_wrapper(void *args) {
    write_to_partitions(args);
}

int run_concurrent_timed(tuple_t *tuples, int tuple_count, int thread_count, int partition_count,
                         tuple_t **global_partition_buffers, int *global_partition_indexes,
                         int global_capacity, double *throughput) {
    if (!tuples) return -1;
    
    // Compute effective capacity for this run.
    int effective_capacity = (tuple_count / partition_count) * PARTITION_MULTIPLIER;
    if (effective_capacity > global_capacity)
        effective_capacity = global_capacity;
    
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
    
    // Initialize the thread pool.
    threadpool pool = thpool_init(thread_count);
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        for (int i = 0; i < partition_count; i++) {
            pthread_mutex_destroy(&mutexes[i]);
        }
        free(mutexes);
        return -1;
    }
    
    thread_args_t args[thread_count];
    int base_segment_size = tuple_count / thread_count;
    
    // Create a barrier to synchronize all threads.
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, thread_count);

    /* ----------------------
       1) Warm-up Run
       ---------------------- */
    // Reset the indexes for each partition before warm-up.
    for (int i = 0; i < partition_count; i++) {
        global_partition_indexes[i] = 0;
    }

    // Enqueue partitioning jobs for warm-up.
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index   = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id        = i + 1;
        args[i].tuples           = tuples;
        args[i].tuples_index     = start_index;
        args[i].tuples_length    = end_index;
        args[i].partition_count  = partition_count;
        args[i].partitions       = global_partition_buffers;
        args[i].partition_indexes= global_partition_indexes;
        args[i].partition_mutexes= mutexes;
        args[i].barrier          = &barrier;
        
        thpool_add_work(pool, write_to_partitions_wrapper, (void *)&args[i]);
    }
    // Wait until the warm-up run finishes.
    thpool_wait(pool);

    /* ----------------------
       2) Timed Run
       ---------------------- */
    // Reset indexes again so the timed run starts fresh.
    for (int i = 0; i < partition_count; i++) {
        global_partition_indexes[i] = 0;
    }

    // Measure only the partitioning step in this run.
    double start = get_time_in_seconds();

    // Enqueue partitioning jobs (timed run).
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index   = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id        = i + 1;
        args[i].tuples           = tuples;
        args[i].tuples_index     = start_index;
        args[i].tuples_length    = end_index;
        args[i].partition_count  = partition_count;
        args[i].partitions       = global_partition_buffers;
        args[i].partition_indexes= global_partition_indexes;
        args[i].partition_mutexes= mutexes;
        args[i].barrier          = &barrier;
        
        thpool_add_work(pool, write_to_partitions_wrapper, (void *)&args[i]);
    }

    // Wait for all jobs in the timed run to complete.
    thpool_wait(pool);

    double end = get_time_in_seconds();
    double total_time = end - start;
    
    // Compute throughput in million tuples per second.
    *throughput = ((double)tuple_count / total_time) / 1e6;
    printf("Concurrent partitioning time: %.6f sec, Throughput: %.2f MT/s\n",
           total_time, *throughput);
    
    // Destroy the thread pool.
    thpool_destroy(pool);
    
    // Cleanup mutexes.
    for (int i = 0; i < partition_count; i++) {
        pthread_mutex_destroy(&mutexes[i]);
    }
    free(mutexes);
    
    // Destroy the barrier.
    pthread_barrier_destroy(&barrier);
    
    return 0;
}

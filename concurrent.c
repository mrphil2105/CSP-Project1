#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include "concurrent.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <numa.h>
#include <numaif.h>
#ifdef __linux__
#include <pthread.h>
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
    double thread_time;
} thread_args_t;

void *write_to_partitions(void *void_args) {
    if (!void_args)
        return NULL;
    thread_args_t *args = (thread_args_t *)void_args;

    #ifdef __linux__

    // Get the number of NUMA nodes available
    int num_nodes = numa_max_node() + 1;
    //printf("Detected %d NUMA nodes.\n", num_nodes);

    // Get the number of available cores in the system.
    int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (num_cores < 1) {
        num_cores = 1;  // Fallback in case of an error.
    }
    //printf("Detected %d cores.\n", num_cores);

    // Dynamically assign the thread to a NUMA node and CPU.
    int node = args->thread_id % num_nodes; // Dynamically assign based on num_nodes
    int cpu_id = args->thread_id % num_cores; // Assign to one of the available cores

    //printf("Thread %d: Assigning to NUMA node %d, CPU %d\n", args->thread_id, node, cpu_id);

    // Set NUMA affinity for the thread.
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    
    // Allocate CPU mask for NUMA node
    struct bitmask *cpus = numa_allocate_cpumask();
    numa_node_to_cpus(node, cpus);

    // Assign the correct CPU based on the NUMA node.
    if (numa_bitmask_isbitset(cpus, cpu_id)) {
            CPU_SET(cpu_id, &cpuset);  // Set the CPU
        } else {
            fprintf(stderr, "CPU %d not available in NUMA node %d\n", cpu_id, node);
        }

    // Free the NUMA bitmask after use.
    numa_free_cpumask(cpus);

    // Set the CPU affinity for this thread.
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        perror("pthread_setaffinity_np");
    }

    // Check if the thread is assigned to the correct CPU
    cpu_set_t assigned_set;
    CPU_ZERO(&assigned_set);
    if (pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &assigned_set) == 0) {
        //printf("Thread %d assigned to CPUs: ", args->thread_id);
        for (int i = 0; i < num_cores; i++) {  // Check only the available cores
            if (CPU_ISSET(i, &assigned_set)) {
                printf("%d ", i);
            }
        }
        //printf("\n");
    } else {
        perror("pthread_getaffinity_np");
    }

#endif

    if (!args->tuples || !args->partitions || !args->partition_indexes || !args->partition_mutexes)
        return NULL;
    double start = get_time_in_seconds();
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition = hash_to_partition(args->tuples[i].key, args->partition_count);
        pthread_mutex_lock(&args->partition_mutexes[partition]);
        int idx = args->partition_indexes[partition]++;
        pthread_mutex_unlock(&args->partition_mutexes[partition]);
        args->partitions[partition][idx] = args->tuples[i];
    }
    double end = get_time_in_seconds();
    args->thread_time = end - start;
    //printf("Concurrent thread %d finished\n", args->thread_id);
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
    double overall_throughput = 0.0;
    
    // Reset the indexes for the partitions used (only current partition_count).
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
    
    // Create threads, passing the shared mutexes array.
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].partitions = global_partition_buffers; // Shared by all threads.
        args[i].partition_indexes = global_partition_indexes; // Shared.
        args[i].partition_mutexes = mutexes; // Shared mutexes.
        
        if (pthread_create(&threads[i], NULL, write_to_partitions, &args[i]) != 0) {
            fprintf(stderr, "Concurrent thread creation failed for thread %d\n", args[i].thread_id);
            // Cleanup mutexes before returning.
            for (int j = 0; j < partition_count; j++) {
                pthread_mutex_destroy(&mutexes[j]);
            }
            free(mutexes);
            return -1;
        }
    }
    
    // Wait for threads to finish and accumulate throughput.
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
        int seg = args[i].tuples_length - args[i].tuples_index;
        double thread_tp = ((double)seg / args[i].thread_time) / 1e6;
        overall_throughput += thread_tp;
    }
    *throughput = overall_throughput;
    
    // Destroy mutexes and free the mutex array.
    for (int i = 0; i < partition_count; i++) {
        pthread_mutex_destroy(&mutexes[i]);
    }
    free(mutexes);
    
    return 0;
}

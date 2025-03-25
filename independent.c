#define _GNU_SOURCE
#include "project.h"
#include "utils.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
    // This thread's slice of the global independent partition buffers.
    tuple_t **partition_buffers;
    // This thread's slice of the global independent indexes.
    int *partition_sizes;
    int estimated_per_partition; // Effective capacity for this run.
    double thread_time;
} thread_args_t;

void *write_independent_output(void *void_args) {
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
    if (!args->tuples || !args->partition_buffers)
        return NULL;
    double start = get_time_in_seconds();
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        int partition_id = hash_to_partition((unsigned char *)&args->tuples[i].key, args->partition_count);
        int idx = args->partition_sizes[partition_id];
        if (idx >= args->estimated_per_partition) {
            // In a real scenario you might want to report overflow or reallocate,
            // but here we simply skip if the buffer is full.
            continue;
        }
        args->partition_buffers[partition_id][idx] = args->tuples[i];
        args->partition_sizes[partition_id]++;
    }
    double end = get_time_in_seconds();
    args->thread_time = end - start;
    printf("Independent thread %d finished\n", args->thread_id);
    return NULL;
}

/*
 * consolidate_independent_output()
 *   For each logical partition, merge the fragments produced by all threads into a single contiguous buffer.
 *   global_partition_buffers: array of pointers (size: thread_count * partition_count)
 *   global_partition_sizes: array of ints (same size) holding the number of tuples written per fragment.
 *   partition_count: number of logical partitions (per run)
 *   thread_count: number of threads used.
 * 
 * Returns the consolidation time (in seconds).
 */
double consolidate_independent_output(int thread_count, int partition_count,
                                      tuple_t **global_partition_buffers, int *global_partition_sizes) {
    double cons_start = get_time_in_seconds();
    // For each logical partition p:
    for (int p = 0; p < partition_count; p++) {
        // Calculate total number of tuples in partition p from all threads.
        int total_tuples = 0;
        for (int t = 0; t < thread_count; t++) {
            int index = t * partition_count + p;
            total_tuples += global_partition_sizes[index];
        }
        // Allocate a contiguous buffer to hold all tuples for partition p.
        tuple_t *consolidated_buffer = malloc(total_tuples * sizeof(tuple_t));
        if (!consolidated_buffer) {
            fprintf(stderr, "Failed to allocate consolidation buffer for partition %d\n", p);
            continue; // Skip consolidation for this partition if allocation fails.
        }
        int offset = 0;
        // Merge each thread's fragment into the consolidated buffer.
        for (int t = 0; t < thread_count; t++) {
            int index = t * partition_count + p;
            int frag_size = global_partition_sizes[index];
            if (frag_size > 0) {
                memcpy(consolidated_buffer + offset,
                       global_partition_buffers[index],
                       frag_size * sizeof(tuple_t));
                offset += frag_size;
            }
        }
        // Optionally, pass consolidated_buffer to the next operator here.
        // For our measurement, we simply free the buffer.
        free(consolidated_buffer);
    }
    double cons_end = get_time_in_seconds();
    return cons_end - cons_start;
}

/*
 * run_independent_timed() performs the partitioning phase (writing) and then the consolidation step.
 * It computes overall throughput (in million tuples/sec) that includes both phases.
 *
 * global_partition_buffers: array of pointers for all thread-specific buffers.
 * global_partition_sizes: corresponding array of counters.
 * global_capacity: maximum capacity per buffer.
 */
int run_independent_timed(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits,
                          tuple_t **global_partition_buffers, int *global_partition_sizes,
                          int global_capacity, double *throughput) {
    if (!tuples)
        return -1;
    int partition_count = 1 << hash_bits;
    // Compute effective capacity for this run based on current hash bits.
    int effective_capacity = (tuple_count / (1 << hash_bits)) * PARTITION_MULTIPLIER;
    if (effective_capacity > global_capacity)
        effective_capacity = global_capacity;
    pthread_t threads[thread_count];
    thread_args_t args[thread_count];
    int base_segment_size = tuple_count / thread_count;
    
    // Initialize the partition sizes for all threads.
    int total_fragments = thread_count * partition_count;
    for (int i = 0; i < total_fragments; i++) {
        global_partition_sizes[i] = 0;
    }
    
    double part_start = get_time_in_seconds();
    
    // Start partitioning threads.
    for (int i = 0; i < thread_count; i++) {
        int start_index = base_segment_size * i;
        int end_index = (i == thread_count - 1) ? tuple_count : (start_index + base_segment_size);
        args[i].thread_id = i + 1;
        args[i].tuples = tuples;
        args[i].tuples_index = start_index;
        args[i].tuples_length = end_index;
        args[i].partition_count = partition_count;
        args[i].estimated_per_partition = effective_capacity;
        // Each thread gets its slice of the global arrays.
        args[i].partition_buffers = global_partition_buffers + (i * partition_count);
        args[i].partition_sizes = global_partition_sizes + (i * partition_count);
        if (pthread_create(&threads[i], NULL, write_independent_output, &args[i]) != 0) {
            fprintf(stderr, "Independent thread creation failed for thread %d\n", i + 1);
            return -1;
        }
    }
    // Wait for all threads to finish partitioning.
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    double part_end = get_time_in_seconds();
    double partition_time = part_end - part_start;
    
    // Consolidation step: merge the fragmented output for each logical partition.
    double consolidation_time = consolidate_independent_output(thread_count, partition_count,
                                                               global_partition_buffers, global_partition_sizes);
    
    double total_time = partition_time + consolidation_time;
    
    // Compute overall throughput: total tuples processed divided by total time.
    *throughput = ((double)tuple_count / total_time) / 1e6;
    
    // Optional: Print timing details for debugging.
    printf("Partitioning time: %.6f sec, Consolidation time: %.6f sec, Total time: %.6f sec\n",
           partition_time, consolidation_time, total_time);
    
    return 0;
}

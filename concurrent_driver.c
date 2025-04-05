#include <stdio.h>
#include <stdlib.h>
#include "concurrent.h"
#include "project.h"
#include "utils.h"
#include "tuples.h"      // For generate_tuples
#include "threadpool.h"  // New thread pool header

#define TUPLE_COUNT (1 << 24)  // ~16 million tuples

int thread_options[] = {1, 2, 4, 8, 16, 32};
int min_hash_bits = 1;
int max_hash_bits = 18;

/*
 * Define a structure for a concurrent experiment task.
 * For concurrent experiments, each task corresponds to processing one partition.
 */
typedef struct {
    int partition_id;
    int total_partitions;
    tuple_t *tuples;
    size_t tuple_count;
    tuple_t *buffer;
    int *index_ptr;
    int capacity;
    double throughput;
} concurrent_task_t;

/*
 * Worker function for concurrent experiments.
 * This calls your concurrent workload function (for example, process_concurrent_partition)
 * that should perform processing for one partition.
 */
void *concurrent_worker(void *arg) {
    concurrent_task_t *task = (concurrent_task_t *)arg;
    process_concurrent_partition(task->partition_id,
                                 task->total_partitions,
                                 task->tuples,
                                 task->tuple_count,
                                 task->buffer,
                                 task->index_ptr,
                                 task->capacity,
                                 &task->throughput);
    return NULL;
}

/*
 * Aggregate throughput across all partitions.
 */
double aggregate_concurrent_throughput(concurrent_task_t tasks[], int num_tasks) {
    double total = 0.0;
    for (int i = 0; i < num_tasks; i++) {
         total += tasks[i].throughput;
    }
    return total;
}

int main(int argc, char *argv[]) {
    // Check for the PREFIX environment variable.
    const char *prefix = getenv("PREFIX");
    if (prefix == NULL) {
         fprintf(stderr, "PREFIX environment variable not set\n");
         return -1;
    }

    int num_thread_options = sizeof(thread_options) / sizeof(thread_options[0]);

    // Generate tuples.
    tuple_t *tuples = generate_tuples(TUPLE_COUNT);
    if (!tuples) {
         fprintf(stderr, "Error generating tuples.\n");
         return -1;
    }

    // Allocate result arrays.
    double **conc_results = malloc(num_thread_options * sizeof(double *));
    if (!conc_results) {
         free(tuples);
         return -1;
    }
    for (int i = 0; i < num_thread_options; i++) {
         conc_results[i] = calloc(max_hash_bits - min_hash_bits + 1, sizeof(double));
    }

    // Pre-allocate memory for concurrent experiments.
    int worst_conc_partitions = 1 << max_hash_bits;
    int worst_conc_capacity = (TUPLE_COUNT / (1 << max_hash_bits)) * PARTITION_MULTIPLIER;
    size_t worst_total_conc = (size_t)worst_conc_partitions * worst_conc_capacity;
    tuple_t *conc_big_block = malloc(worst_total_conc * sizeof(tuple_t));
    if (!conc_big_block) {
         free(tuples);
         return -1;
    }
    tuple_t **global_conc_buffers = malloc(worst_conc_partitions * sizeof(tuple_t *));
    int *global_conc_indexes = calloc(worst_conc_partitions, sizeof(int));

    // Run concurrent experiments.
    for (int t_idx = 0; t_idx < num_thread_options; t_idx++) {
        int thread_count = thread_options[t_idx];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            int total_partitions = 1 << hb;
            int effective_capacity = (TUPLE_COUNT / total_partitions) * PARTITION_MULTIPLIER;

            // Set up the global buffers and indexes for each partition.
            for (int i = 0; i < total_partitions; i++) {
                global_conc_buffers[i] = conc_big_block + i * effective_capacity;
                global_conc_indexes[i] = 0;
            }

            // Create a thread pool with 'thread_count' worker threads.
            threadpool_t *pool = threadpool_create(thread_count);
            if (pool == NULL) {
                fprintf(stderr, "Failed to create threadpool for %d threads\n", thread_count);
                continue;
            }

            // For concurrent experiments, submit one task per partition.
            concurrent_task_t tasks[total_partitions];
            for (int i = 0; i < total_partitions; i++) {
                tasks[i].partition_id = i;
                tasks[i].total_partitions = total_partitions;
                tasks[i].tuples = tuples;
                tasks[i].tuple_count = TUPLE_COUNT;
                tasks[i].buffer = global_conc_buffers[i];
                tasks[i].index_ptr = &global_conc_indexes[i];
                tasks[i].capacity = effective_capacity;
                tasks[i].throughput = 0.0;
                threadpool_submit(pool, concurrent_worker, (void *)&tasks[i]);
            }

            // Wait for all tasks to finish.
            threadpool_wait(pool);

            // Aggregate throughput from all partition tasks.
            double throughput = aggregate_concurrent_throughput(tasks, total_partitions);
            conc_results[t_idx][hb - min_hash_bits] = throughput;

            // Destroy the thread pool.
            threadpool_destroy(pool);
        }
    }

    // Print results.
    printf("Concurrent Experiment Results (Throughput MT/s):\n");
    printf("Threads,HashBits,Throughput\n");
    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            printf("%d,%d,%.2f\n", thread_count, hb, conc_results[i][hb - min_hash_bits]);
        }
    }

    // Cleanup.
    free(tuples);
    for (int i = 0; i < num_thread_options; i++) {
        free(conc_results[i]);
    }
    free(conc_results);
    free(global_conc_indexes);
    free(global_conc_buffers);
    free(conc_big_block);

    return 0;
}
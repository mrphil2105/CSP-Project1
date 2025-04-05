#include <stdio.h>
#include <stdlib.h>
#include "independent.h"
#include "project.h"
#include "utils.h"
#include "tuples.h"      // For generate_tuples
#include "threadpool.h"  // New thread pool header

// Use the same tuple count as before.
#define TUPLE_COUNT (1 << 24)  // ~16 million tuples

int thread_options[] = {1, 2, 4, 8, 16, 32};
int min_hash_bits = 1;
int max_hash_bits = 18;

/* 
 * Define a struct that holds the parameters for one independent‐experiment task.
 * Each task corresponds to one “worker” (previously a thread) processing a portion of
 * the overall tuples, updating its assigned partitions.
 */
typedef struct {
    int thread_id;
    int thread_count;
    int hash_bits;
    int partitions_per_thread;    // = 1 << hash_bits
    tuple_t *tuples;
    size_t tuple_count;
    // Each worker gets its slice of the global independent buffers and indexes.
    tuple_t **buffers;            
    int *indexes; 
    int capacity;                 // effective capacity for these partitions.
    double throughput;            // A local throughput result.
} independent_task_t;

/*
 * The worker function that will be submitted to the thread pool.
 * You should implement process_independent_workload() (or call into your existing code)
 * to do the actual work for the given thread.
 */
void *independent_worker(void *arg) {
    independent_task_t *task = (independent_task_t *)arg;
    // This function should process the workload for this task.
    // For example, process_independent_workload() could be defined in independent.c.
    process_independent_workload(task->thread_id,
                                 task->thread_count,
                                 task->hash_bits,
                                 task->partitions_per_thread,
                                 task->tuples,
                                 task->tuple_count,
                                 task->buffers,
                                 task->indexes,
                                 task->capacity,
                                 &task->throughput);
    return NULL;
}

/*
 * Aggregate throughput values obtained from each task.
 * (You can change the aggregation method as needed.)
 */
double aggregate_independent_throughput(independent_task_t tasks[], int num_tasks) {
    double total = 0.0;
    for (int i = 0; i < num_tasks; i++) {
         total += tasks[i].throughput;
    }
    return total;
}

int main(int argc, char *argv[]) {
    // Check for the PREFIX environment variable.
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
    double **indep_results = malloc(num_thread_options * sizeof(double *));
    if (!indep_results) {
         free(tuples);
         return -1;
    }
    for (int i = 0; i < num_thread_options; i++) {
         indep_results[i] = calloc(max_hash_bits - min_hash_bits + 1, sizeof(double));
    }

    // Pre-allocate memory for independent experiments.
    int max_thread_count = 0;
    for (int i = 0; i < num_thread_options; i++) {
         if (thread_options[i] > max_thread_count)
             max_thread_count = thread_options[i];
    }
    size_t worst_total_indep = (size_t)max_thread_count * TUPLE_COUNT * PARTITION_MULTIPLIER;
    tuple_t *indep_big_block = malloc(worst_total_indep * sizeof(tuple_t));
    if (!indep_big_block) {
         free(tuples);
         return -1;
    }
    int worst_indep_partitions = max_thread_count * (1 << max_hash_bits);
    tuple_t **global_indep_buffers = malloc(worst_indep_partitions * sizeof(tuple_t *));
    int *global_indep_indexes = calloc(worst_indep_partitions, sizeof(int));

    // Run independent experiments for each thread count and each hash bits value.
    for (int t_idx = 0; t_idx < num_thread_options; t_idx++) {
        int thread_count = thread_options[t_idx];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            int partitions_per_thread = 1 << hb;
            int effective_capacity = (TUPLE_COUNT / partitions_per_thread) * PARTITION_MULTIPLIER;

            // For each thread, set up its partitions.
            for (int thr = 0; thr < thread_count; thr++) {
                for (int part = 0; part < partitions_per_thread; part++) {
                    int index = thr * partitions_per_thread + part;
                    global_indep_buffers[index] = indep_big_block +
                        thr * (TUPLE_COUNT * PARTITION_MULTIPLIER) +
                        part * effective_capacity;
                    global_indep_indexes[index] = 0;
                }
            }

            // Create a thread pool with 'thread_count' workers.
            threadpool_t *pool = threadpool_create(thread_count);
            if (pool == NULL) {
                fprintf(stderr, "Failed to create threadpool for %d threads\n", thread_count);
                continue;
            }

            // Create an array of tasks—one per thread.
            independent_task_t tasks[thread_count];
            for (int thr = 0; thr < thread_count; thr++) {
                tasks[thr].thread_id = thr;
                tasks[thr].thread_count = thread_count;
                tasks[thr].hash_bits = hb;
                tasks[thr].partitions_per_thread = partitions_per_thread;
                tasks[thr].tuples = tuples;
                tasks[thr].tuple_count = TUPLE_COUNT;
                // Each task gets its slice of the global buffers and indexes.
                tasks[thr].buffers = &global_indep_buffers[thr * partitions_per_thread];
                tasks[thr].indexes = &global_indep_indexes[thr * partitions_per_thread];
                tasks[thr].capacity = effective_capacity;
                tasks[thr].throughput = 0.0;

                // Submit the task to the thread pool.
                threadpool_submit(pool, independent_worker, (void *)&tasks[thr]);
            }

            // Wait until all submitted tasks are complete.
            threadpool_wait(pool);

            // Aggregate the throughputs from the individual tasks.
            double throughput = aggregate_independent_throughput(tasks, thread_count);
            indep_results[t_idx][hb - min_hash_bits] = throughput;

            // Tear down the thread pool.
            threadpool_destroy(pool);
        }
    }

    // Print results.
    printf("Independent Experiment Results (Throughput MT/s):\n");
    printf("Threads,HashBits,Throughput\n");
    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            printf("%d,%d,%.2f\n", thread_count, hb, indep_results[i][hb - min_hash_bits]);
        }
    }

    // Cleanup.
    free(tuples);
    for (int i = 0; i < num_thread_options; i++) {
        free(indep_results[i]);
    }
    free(indep_results);
    free(global_indep_indexes);
    free(global_indep_buffers);
    free(indep_big_block);

    return 0;
}
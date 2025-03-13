#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#include "project.h"
#include "utils.h"
//Each thread write in its own isolated output
typedef struct{
    int thread_id;
    tuple_t *tuples;
    int tuples_index;
    int tuples_length;
    tuple_t *output;
    int partition_count;
    int output_count;
    tuple_t **partition_buffers;
    int *partition_sizes;
    int *partition_capacities;
} thread_args_t;

void *write_independent_output(void *void_args) {
  if (void_args == NULL) {
      return NULL;
  }
  thread_args_t *args = (thread_args_t *)void_args;
  if (args->tuples == NULL || args->partition_buffers == NULL) return NULL;
  
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(args->thread_id - 1, &cpuset);

  tuple_t *all_tuples = args->tuples;
  int start_idx       = args->tuples_index;
  int end_idx         = args->tuples_length;
  int partition_count = args->partition_count;

  int processed = 0;
  for (int i = start_idx; i < end_idx; i++) {
      int partition_id = hash_to_partition(all_tuples[i].key, partition_count);
      int current_size = args->partition_sizes[partition_id];

      // Check if we need to grow the partition buffer
      if (current_size >= args->partition_capacities[partition_id]) {
          // Double the capacity
          int new_capacity = args->partition_capacities[partition_id] * 2;
          tuple_t *new_buffer = realloc(args->partition_buffers[partition_id],
                                        sizeof(tuple_t) * new_capacity);
          if (new_buffer == NULL) {
              fprintf(stderr, "Realloc failed for thread %d, partition %d\n",
                      args->thread_id, partition_id);
              continue; // or handle error appropriately
          }
          args->partition_buffers[partition_id] = new_buffer;
          args->partition_capacities[partition_id] = new_capacity;
      }
      
      // Now safe to write the tuple
      args->partition_buffers[partition_id][current_size] = all_tuples[i];
      args->partition_sizes[partition_id] = current_size + 1;
      processed++;
  }
  
  args->output_count = processed;
  //printf("Thread %d finished, processed %d tuples\n", args->thread_id, processed);
  
  return NULL;
}

int run_independent(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits) {
  if(!tuples){
    return -1;
  }

  // Number of partitions is 2^hash_bits
  int partition_count = 1 << hash_bits;

  pthread_t threads[thread_count];
  thread_args_t args[thread_count];

  // Divide the input among threads
  int base_segment_size = tuple_count / thread_count;
  
  for (int i = 0; i < thread_count; i++) {
    int start_index = base_segment_size * i;
        // Let the last thread take any remainder
    int end_index = (i == thread_count - 1) 
                    ? tuple_count 
                    : (start_index + base_segment_size);  

    args[i].thread_id       = i + 1;
    args[i].tuples          = tuples;
    args[i].tuples_index    = start_index;
    args[i].tuples_length   = end_index;
    args[i].partition_count = partition_count;
    args[i].output_count    = 0;

    // Allocate arrays to store pointers and sizes for each partition
    args[i].partition_buffers   = (tuple_t **)malloc(sizeof(tuple_t*) * partition_count);
    args[i].partition_sizes     = (int *)calloc(partition_count, sizeof(int));
    args[i].partition_capacities = (int *)malloc(sizeof(int) * partition_count);
    
    if (!args[i].partition_buffers || !args[i].partition_sizes || !args[i].partition_capacities) {
      fprintf(stderr, "Memory allocation failed for thread %d partition arrays\n", i+1);
      return -1;
    }
    
    int tuples_for_this_thread = end_index - start_index;
        int estimated_per_partition = (tuples_for_this_thread / partition_count) + 16;

        for (int p = 0; p < partition_count; p++) {
            args[i].partition_capacities[p] = estimated_per_partition;
            args[i].partition_buffers[p] = (tuple_t *)malloc(sizeof(tuple_t) * estimated_per_partition);
            if (!args[i].partition_buffers[p]) {
                fprintf(stderr, "Memory allocation failed for thread %d partition %d\n", i+1, p);
                return -1;
            }
    }

    // Launch the thread
    //printf("Starting thread %d...\n", args[i].thread_id);
    if (pthread_create(&threads[i], NULL, write_independent_output, &args[i]) != 0) {
        fprintf(stderr, "Thread creation failed for thread %d\n", i+1);
        // Join already-launched threads before returning
        for (int j = 0; j < i; j++) {
            pthread_join(threads[j], NULL);
        }
        return -1;
    }
  }

  // Join all threads
  //printf("Joining threads...\n");
  for (int i = 0; i < thread_count; i++) {
      pthread_join(threads[i], NULL);
  }

  //Free resources
  for (int i = 0; i < thread_count; i++) {
    for (int p = 0; p < args[i].partition_count; p++) {
        free(args[i].partition_buffers[p]);
    }
    free(args[i].partition_buffers);
    free(args[i].partition_sizes);
    free(args[i].partition_capacities);
}

  return 0;
}


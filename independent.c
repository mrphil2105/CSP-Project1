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
    int output_count;
} thread_args_t;

void *write_independent_output(void *void_args) {
    if (void_args == NULL) {
        return NULL;
    }

    thread_args_t *args = (thread_args_t *)void_args;

    if (args->tuples == NULL || args->output == NULL) {
        return NULL;
    }

    int output_index = 0;
    for (int i = args->tuples_index; i < args->tuples_length; i++) {
        args->output[output_index++] = args->tuples[i];  // Sequential write
    }

    args->output_count = output_index;
    printf("Thread %d finished, processed %d tuples\n", args->thread_id, output_index);
    return NULL;
}
int run_independent(tuple_t *tuples, int tuple_count, int thread_count) {
  if(!tuples){
    return -1;
  }

  pthread_t threads[thread_count];
  thread_args_t args[thread_count];
  
  for (int i = 0; i < thread_count; i++) {
    int segment_size = tuple_count / thread_count;
    int start_index = segment_size * i;
    int end_index = (i == thread_count - 1) ? tuple_count : segment_size * (i + 1);
    int output_size = end_index - start_index;
    
    // Allocate independent output buffer per thread
    args[i].output = malloc(sizeof(tuple_t) * output_size);
    if (!args[i].output) {
      printf("Memory allocation failed for thread %d output\n", i + 1);
      return -1;
    }
    args[i].thread_id = i + 1;
    args[i].tuples = tuples;
    args[i].tuples_index = start_index;
    args[i].tuples_length = end_index;
    args[i].output_count = 0;

    printf("Starting thread %d...\n", args[i].thread_id);
    if (pthread_create(&threads[i], NULL, &write_independent_output, &args[i]) != 0) {
      for (int j = 0; j < i; j++) {
        pthread_join(threads[j], NULL);
      }
      printf("Thread creation failed. Threads joined.\n");
      return -1;
    }
  }
   printf("Joining threads...");
   for (int i = 0; i < thread_count; i++) {
      pthread_join(threads[i], NULL);
   }

    //We need to do somethnig with the results?
    //Free resources
    //for (int i = 0; i < thread_count; i++) {
    //    free(thread_outputs[i]);
    //}
    //free(thread_outputs);

    return 0;
}


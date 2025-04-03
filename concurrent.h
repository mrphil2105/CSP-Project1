#ifndef CONCURRENT_H
#define CONCURRENT_H

#include "project.h"

int run_concurrent_timed(tuple_t *tuples, int tuple_count, int thread_count, int partition_count,
                         tuple_t **global_partition_buffers, int *global_partition_indexes,
                         int global_capacity, double *throughput);

#endif

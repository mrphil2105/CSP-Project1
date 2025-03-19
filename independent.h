#ifndef INDEPENDENT_H
#define INDEPENDENT_H

#include "project.h"

int run_independent_timed(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits,
                          tuple_t **global_partition_buffers, int *global_partition_sizes,
                          int global_capacity, double *throughput);

#endif // INDEPENDENT_H

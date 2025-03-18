#ifndef CONCURRENT_H
#define CONCURRENT_H

#include "project.h"

int run_concurrent(tuple_t *tuples, int tuple_count, int thread_count, int partition_count);
int run_independent_timed(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits, double *throughput);

#endif // CONCURRENT_H

#ifndef INDEPENDENT_H
#define INDEPENDENT_H

#include "project.h"

int run_independent(tuple_t *tuples, int tuple_count, int thread_count, int hash_bits);
int run_concurrent_timed(tuple_t *tuples, int tuple_count, int thread_count, int partition_count, double *throughput);

#endif // INDEPENDENT_H

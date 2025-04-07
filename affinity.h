#ifndef AFFINITY_H
#define AFFINITY_H

#include <stdio.h>

#ifdef CPU_AFFINITY
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
static inline void set_affinity(int thread_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int num_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    int cpu = thread_id % num_cpus;
    CPU_SET(cpu, &cpuset);
    int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (ret != 0) {
        fprintf(stderr, "Failed to set CPU affinity for thread %d\n", thread_id);
    }
}
#elif defined(NUMA_BINDING)
#include <numa.h>
#include <numaif.h>
#include <unistd.h>
#include <pthread.h>
static inline void set_affinity(int thread_id) {
    int num_nodes = numa_max_node() + 1;
    int node = thread_id % num_nodes;
    if (numa_run_on_node(node) != 0) {
        fprintf(stderr, "Failed to bind thread %d to NUMA node %d\n", thread_id, node);
    }
}
#else
static inline void set_affinity(int thread_id) {
    (void) thread_id;  // No affinity is set in default configuration.
}
#endif

#endif

#include <stdio.h>
#include <stdlib.h>
#include "concurrent.h"
#include "project.h"
#include "utils.h"
#include "tuples.h"

#define TUPLE_COUNT (1 << 24)

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <THREAD_COUNT> <HASHBITS>\n", argv[0]);
        return -1;
    }
    int thread_count = atoi(argv[1]);
    int hash_bits = atoi(argv[2]);

    tuple_t *tuples = generate_tuples(TUPLE_COUNT);
    if (!tuples) {
        fprintf(stderr, "Error generating tuples.\n");
        return -1;
    }

    int total_partitions = 1 << hash_bits;
    int effective_capacity = (TUPLE_COUNT / total_partitions) * PARTITION_MULTIPLIER;

    tuple_t *conc_big_block = malloc(total_partitions * effective_capacity * sizeof(tuple_t));
    if (!conc_big_block) {
        free(tuples);
        return -1;
    }

    tuple_t **global_conc_buffers = malloc(total_partitions * sizeof(tuple_t *));
    int *global_conc_indexes = calloc(total_partitions, sizeof(int));
    if (!global_conc_buffers || !global_conc_indexes) {
        free(tuples);
        free(conc_big_block);
        return -1;
    }

    for (int i = 0; i < total_partitions; i++) {
        global_conc_buffers[i] = conc_big_block + i * effective_capacity;
        global_conc_indexes[i] = 0;
    }

    double throughput = 0.0;
    if (run_concurrent_timed(tuples, TUPLE_COUNT, thread_count, total_partitions,
                             global_conc_buffers, global_conc_indexes, effective_capacity, &throughput) != 0) {
        fprintf(stderr, "Error in concurrent run with %d threads and %d hashbits\n", thread_count, hash_bits);
    } else {
        printf("Threads,HashBits,Throughput\n");
        printf("%d,%d,%.2f\n", thread_count, hash_bits, throughput);
    }

    free(tuples);
    free(conc_big_block);
    free(global_conc_buffers);
    free(global_conc_indexes);

    return 0;
}

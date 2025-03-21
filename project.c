#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef __linux__
#include <sys/random.h>
#endif
#include "concurrent.h"
#include "independent.h"
#include "project.h"
#include "utils.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

const int MAX_TUPLES = 32000000;

// Generate random bytes for tuples.
// This function is only used on non-Linux systems.
#ifndef __linux__
ssize_t getrandom(unsigned char *buffer, size_t length, unsigned int flags) {
    if (length <= 0 || length > MAX_TUPLES * 16)
        return -1;
    for (size_t i = 0; i < length; i++)
        buffer[i] = rand();
    return length;
}
#endif

// Generate tuples (16 bytes per tuple)
tuple_t *generate_tuples(int count) {
    if (count <= 0 || count > MAX_TUPLES)
        return NULL;
    size_t total_bytes = count * 16;
    unsigned char *buffer = malloc(total_bytes);
    if (!buffer)
        return NULL;
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0) {
        perror("open /dev/urandom");
        free(buffer);
        return NULL;
    }
    size_t offset = 0;
    while (offset < total_bytes) {
        ssize_t bytes_read = read(fd, buffer + offset, total_bytes - offset);
        if (bytes_read < 0) {
            if (errno == EINTR)
                continue;
            perror("read /dev/urandom");
            close(fd);
            free(buffer);
            return NULL;
        }
        offset += bytes_read;
    }
    close(fd);
    return (tuple_t *)buffer;
}

int main(int argc, char *argv[]) {
    // Experiment parameters.
    const int tuple_count = 1 << 24;  // ~16 million tuples
    const int num_runs = 5;           // Number of runs for averaging
    
    int thread_options[] = {1, 2, 4, 8, 16, 32};
    int num_thread_options = sizeof(thread_options) / sizeof(thread_options[0]);
    int min_hash_bits = 1;  
    int max_hash_bits = 18; 

    // Generate random tuples.
    tuple_t *tuples = generate_tuples(tuple_count);
    if (!tuples) {
        fprintf(stderr, "Error generating tuples.\n");
        return -1;
    }
    
    // Allocate result arrays.
    double **indep_results = malloc(num_thread_options * sizeof(double *));
    double **conc_results  = malloc(num_thread_options * sizeof(double *));
    if (!indep_results || !conc_results) {
        fprintf(stderr, "Memory allocation failed for result arrays.\n");
        free(tuples);
        return -1;
    }
    // Initialize result arrays (for each thread option).
    for (int i = 0; i < num_thread_options; i++) {
        indep_results[i] = calloc(max_hash_bits - min_hash_bits + 1, sizeof(double));
        conc_results[i]  = calloc(max_hash_bits - min_hash_bits + 1, sizeof(double));
    }
    
    // Pre-allocating memory for independent experiments.
    int max_thread_count = 0;
    for (int i = 0; i < num_thread_options; i++) {
        if (thread_options[i] > max_thread_count)
            max_thread_count = thread_options[i];
    }
    size_t worst_total_indep = (size_t)max_thread_count * tuple_count * PARTITION_MULTIPLIER;
    tuple_t *indep_big_block = malloc(worst_total_indep * sizeof(tuple_t));
    if (!indep_big_block) {
        fprintf(stderr, "Failed to allocate independent big block.\n");
        free(tuples);
        return -1;
    }
    int worst_indep_partitions = max_thread_count * (1 << max_hash_bits);
    tuple_t **global_indep_buffers = malloc(worst_indep_partitions * sizeof(tuple_t *));
    if (!global_indep_buffers) {
        fprintf(stderr, "Failed to allocate global independent buffers.\n");
        free(tuples);
        free(indep_big_block);
        return -1;
    }
    int *global_indep_indexes = calloc(worst_indep_partitions, sizeof(int));
    if (!global_indep_indexes) {
        fprintf(stderr, "Failed to allocate global independent indexes.\n");
        free(tuples);
        free(indep_big_block);
        free(global_indep_buffers);
        return -1;
    }
    
    // Pre-allocating memory for concurrent experiments.
    int worst_conc_partitions = 1 << max_hash_bits;
    int worst_conc_capacity = (tuple_count / (1 << max_hash_bits)) * PARTITION_MULTIPLIER;
    size_t worst_total_conc = (size_t)worst_conc_partitions * worst_conc_capacity;
    tuple_t *conc_big_block = malloc(worst_total_conc * sizeof(tuple_t));
    if (!conc_big_block) {
        fprintf(stderr, "Failed to allocate concurrent big block.\n");
        free(tuples);
        free(indep_big_block);
        free(global_indep_buffers);
        free(global_indep_indexes);
        return -1;
    }
    tuple_t **global_conc_buffers = malloc(worst_conc_partitions * sizeof(tuple_t *));
    if (!global_conc_buffers) {
        fprintf(stderr, "Failed to allocate global concurrent buffers.\n");
        free(tuples);
        free(indep_big_block);
        free(global_indep_buffers);
        free(global_indep_indexes);
        free(conc_big_block);
        return -1;
    }
    int *global_conc_indexes = calloc(worst_conc_partitions, sizeof(int));
    if (!global_conc_indexes) {
        fprintf(stderr, "Failed to allocate global concurrent indexes.\n");
        free(tuples);
        free(indep_big_block);
        free(global_indep_buffers);
        free(global_indep_indexes);
        free(conc_big_block);
        free(global_conc_buffers);
        return -1;
    }
    
    // Run independent and concurrent experiments.
    // Description: Each thread gets a block of size: tuple_count * PARTITION_MULTIPLIER.
    //              For thread t and its partition p, compute:
    //                   pointer = indep_big_block + t*(tuple_count*PARTITION_MULTIPLIER) + p*effective_capacity

    // Run Independent Experiments
    for (int run = 0; run < num_runs; run++) {
        
        for (int t_idx = 0; t_idx < num_thread_options; t_idx++) {
            int thread_count = thread_options[t_idx];
            for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
                int partitions_per_thread = 1 << hb;
                int effective_capacity = (tuple_count / partitions_per_thread) * PARTITION_MULTIPLIER;
                for (int thr = 0; thr < thread_count; thr++) {
                    for (int part = 0; part < partitions_per_thread; part++) {
                        int index = thr * partitions_per_thread + part;
                        global_indep_buffers[index] = indep_big_block 
                            + thr * (tuple_count * PARTITION_MULTIPLIER)
                            + part * effective_capacity;
                        global_indep_indexes[index] = 0;
                    }
                }
                
                double throughput = 0;
                if (run_independent_timed(tuples, tuple_count, thread_count, hb,
                                          global_indep_buffers, global_indep_indexes,
                                          effective_capacity, &throughput) != 0) {
                    fprintf(stderr, "Error in independent run with %d threads and hb=%d\n", thread_count, hb);
                    continue;
                }
                indep_results[t_idx][hb - min_hash_bits] += throughput;
            }
        }
    }
    
    // Run Concurrent Experiments
    for (int run = 0; run < num_runs; run++) {
        for (int t_idx = 0; t_idx < num_thread_options; t_idx++) {
            int thread_count = thread_options[t_idx];
            for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
                int total_partitions = 1 << hb;
                int effective_capacity = (tuple_count / total_partitions) * PARTITION_MULTIPLIER;
                for (int i = 0; i < total_partitions; i++) {
                    global_conc_buffers[i] = conc_big_block + i * effective_capacity;
                    global_conc_indexes[i] = 0;
                }
                
                double throughput = 0;
                if (run_concurrent_timed(tuples, tuple_count, thread_count, total_partitions,
                                         global_conc_buffers, global_conc_indexes,
                                         effective_capacity, &throughput) != 0) {
                    fprintf(stderr, "Error in concurrent run with %d threads and partitions=%d\n", thread_count, total_partitions);
                    continue;
                }
                conc_results[t_idx][hb - min_hash_bits] += throughput;
            }
        }
    }
    
    // Average the results over runs.
    for (int i = 0; i < num_thread_options; i++) {
        for (int j = 0; j < max_hash_bits - min_hash_bits + 1; j++) {
            indep_results[i][j] /= num_runs;
            conc_results[i][j]  /= num_runs;
        }
    }
    
    // Write results to CSV files.
    FILE *indep_file = fopen("indep_numa_affinity.csv", "w");
    if (!indep_file) {
        perror("Error opening independent_results.csv");
        return -1;
    }
    fprintf(indep_file, "Method,Threads,HashBits,Throughput(MT/s)\n");
    FILE *conc_file = fopen("conc_numa_affinity.csv", "w");
    if (!conc_file) {
        perror("Error opening concurrent_results.csv");
        fclose(indep_file);
        return -1;
    }
    fprintf(conc_file, "Method,Threads,HashBits,Throughput(MT/s)\n");
    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            fprintf(indep_file, "independent,%d,%d,%.2f\n", thread_count, hb, indep_results[i][hb - min_hash_bits]);
            fprintf(conc_file, "concurrent,%d,%d,%.2f\n", thread_count, hb, conc_results[i][hb - min_hash_bits]);
        }
    }
    fclose(indep_file);
    fclose(conc_file);
    
    // Free allocated memory.
    free(tuples);
    for (int i = 0; i < num_thread_options; i++) {
        free(indep_results[i]);
        free(conc_results[i]);
    }
    free(indep_results);
    free(conc_results);
    free(global_indep_indexes);
    free(global_indep_buffers);
    free(indep_big_block);
    free(global_conc_indexes);
    free(global_conc_buffers);
    free(conc_big_block);
    
    return 0;
}

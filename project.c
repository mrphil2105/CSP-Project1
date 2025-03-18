#include <stdio.h>
#include <stdlib.h>
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

#ifndef __linux__
ssize_t getrandom(unsigned char *buffer, size_t length, unsigned int flags) {
    if (length <= 0 || length > MAX_TUPLES * 16)
        return -1;
    for (size_t i = 0; i < length; i++)
        buffer[i] = rand();
    return length;
}
#endif

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
    const int tuple_count = 1 << 24;
    const int num_runs = 5; // Multiple runs for averaging
    tuple_t *tuples = generate_tuples(tuple_count);
    if (!tuples) {
        fprintf(stderr, "Error generating tuples.\n");
        return -1;
    }

    int thread_options[] = {1, 2, 4, 8, 16};
    int num_thread_options = sizeof(thread_options) / sizeof(thread_options[0]);
    int min_hash_bits = 1;
    int max_hash_bits = 18;

    // Allocate result arrays.
    double **indep_results = malloc(num_thread_options * sizeof(double *));
    double **conc_results = malloc(num_thread_options * sizeof(double *));
    if (!indep_results || !conc_results) {
        fprintf(stderr, "Memory allocation failed for result arrays.\n");
        free(tuples);
        return -1;
    }
    for (int i = 0; i < num_thread_options; i++) {
        indep_results[i] = calloc(max_hash_bits - min_hash_bits + 1, sizeof(double));
        conc_results[i] = calloc(max_hash_bits - min_hash_bits + 1, sizeof(double));

    }

    for (int run = 0; run < num_runs; run++) {
        // Independent experiments.
        for (int i = 0; i < num_thread_options; i++) {
            int thread_count = thread_options[i];
            for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
                double throughput = 0;
                if (run_independent_timed(tuples, tuple_count, thread_count, hb, &throughput) != 0) {
                    fprintf(stderr, "Error in independent run with %d threads and hb=%d\n", thread_count, hb);
                    continue;
                }
                indep_results[i][hb - min_hash_bits] += throughput;
            }
        }

        // Concurrent experiments.
        for (int i = 0; i < num_thread_options; i++) {
            int thread_count = thread_options[i];
            for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
                int partition_count = 1 << hb;
                double throughput = 0;
                if (run_concurrent_timed(tuples, tuple_count, thread_count, partition_count, &throughput) != 0) {
                    fprintf(stderr, "Error in concurrent run with %d threads and partition_count=%d\n", thread_count,
                            partition_count);
                    continue;
                }
                conc_results[i][hb - min_hash_bits] += throughput;
            }
        }
    }

    // Average results.
    for (int i = 0; i < num_thread_options; i++) {
        for (int j = 0; j < max_hash_bits - min_hash_bits + 1; j++) {
            indep_results[i][j] /= num_runs;
            conc_results[i][j] /= num_runs;
        }
    }

    // Write results to CSV files.
    FILE *indep_file = fopen("independent_results.csv", "w");
    if (!indep_file) {
        perror("Error opening independent_results.csv");
        free(tuples);
        return -1;
    }
    fprintf(indep_file, "Method,Threads,HashBits,Throughput(MT/s)\n");
    FILE *conc_file = fopen("concurrent_results.csv", "w");
    if (!conc_file) {
        perror("Error opening concurrent_results.csv");
        fclose(indep_file);
        free(tuples);
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
    free(tuples);
    for (int i = 0; i < num_thread_options; i++) {
        free(indep_results[i]);
        free(conc_results[i]);
    }
    free(indep_results);
    free(conc_results);

    return 0;
}

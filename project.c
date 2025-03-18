#include <stdlib.h>
#include <stdio.h>

#ifdef __linux__
#include <sys/random.h>
#endif

#include "project.h"
#include "concurrent.h"
#include "independent.h"
#include "utils.h"

#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

const int MAX_TUPLES = 32000000;

#ifndef __linux__
ssize_t getrandom(unsigned char *buffer, size_t length, unsigned int flags) {
    if (length <= 0 || length > MAX_TUPLES * 16) {
        return -1;
    }
    for (size_t i = 0; i < length; i++) {
        buffer[i] = rand();
    }
    return length;
}
#endif

tuple_t *generate_tuples(int count) {
    if (count <= 0 || count > MAX_TUPLES) {
        return NULL;
    }
    size_t total_bytes = count * 16;
    unsigned char *buffer = malloc(total_bytes);
    if (buffer == NULL) {
        return NULL;
    }

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
            if (errno == EINTR) continue; // Interrupted, try again
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

int main(int argc, char* argv[]) {
    const int tuple_count = 1 << 24;
    const int num_runs = 1;  // Number of times to run the experiments

    tuple_t *tuples = generate_tuples(tuple_count);
    if (!tuples) {
        fprintf(stderr, "Error generating tuples.\n");
        return -1;
    }

    int thread_options[] = {1, 2, 4, 8, 16, 32};
    int num_thread_options = sizeof(thread_options) / sizeof(thread_options[0]);
    int min_hash_bits = 1;
    int max_hash_bits = 18;

    // Dynamically allocate memory for the result arrays
    double **indep_results = malloc(num_thread_options * sizeof(double *));
    double **conc_results = malloc(num_thread_options * sizeof(double *));
    if (indep_results == NULL || conc_results == NULL) {
        fprintf(stderr, "Memory allocation failed for result arrays.\n");
        free(tuples);
        return -1;
    }

    for (int i = 0; i < num_thread_options; i++) {
        indep_results[i] = malloc((max_hash_bits - min_hash_bits + 1) * sizeof(double));
        conc_results[i] = malloc((max_hash_bits - min_hash_bits + 1) * sizeof(double));
        if (indep_results[i] == NULL || conc_results[i] == NULL) {
            fprintf(stderr, "Memory allocation failed for result arrays at index %d.\n", i);
            free(tuples);
            return -1;
        }
        // Initialize the arrays to 0
        for (int j = 0; j < max_hash_bits - min_hash_bits + 1; j++) {
            indep_results[i][j] = 0;
            conc_results[i][j] = 0;
        }
    }

    for (int run = 0; run < num_runs; run++) {
        // Run independent experiments.
         for (int i = 0; i < num_thread_options; i++) {
            int thread_count = thread_options[i];
            for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
                double start_time = get_time_in_seconds();
                int rc = run_independent(tuples, tuple_count, thread_count, hb);
                double end_time = get_time_in_seconds();
                if (rc != 0) {
                    fprintf(stderr, "Error in run_independent with %d threads and hb=%d\n", thread_count, hb);
                    continue;
                }
                double elapsed = end_time - start_time;
                double throughput = (double)tuple_count / elapsed / 1e6;  // Throughput in million tuples per second
                indep_results[i][hb - min_hash_bits] += throughput;  // Accumulate throughput
            }
         }

        // Run concurrent experiments.
        for (int i = 0; i < num_thread_options; i++) {
            int thread_count = thread_options[i];
            for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
                int partition_count = 1 << hb;
                double total_throughput = 0.0; // Reset for each experiment

                for (int r = 0; r < num_runs; r++) {
                    double start_time = get_time_in_seconds();
                    int rc = run_concurrent(tuples, tuple_count, thread_count, partition_count);
                    double end_time = get_time_in_seconds();
                    if (rc != 0) {
                        fprintf(stderr, "Error in run_concurrent with %d threads and partition_count=%d\n", thread_count, partition_count);
                        continue;
                    }
                    double elapsed = end_time - start_time;
                    double throughput = (double)tuple_count / elapsed / 1e6;  // Throughput in million tuples per second
                    total_throughput += throughput;
                }

                // Average throughput across runs
                conc_results[i][hb - min_hash_bits] = total_throughput / num_runs;
            }
        }
    }

    // Open CSV files to write averaged results.
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

    // Write results for independent method
    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            double avg_throughput = indep_results[i][hb - min_hash_bits];  // Get independent throughput
            fprintf(indep_file, "independent,%d,%d,%.2f\n", thread_count, hb, avg_throughput);
        }
    }

    // Write results for concurrent method
    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            double avg_throughput = conc_results[i][hb - min_hash_bits];  // Get concurrent throughput
            fprintf(conc_file, "concurrent,%d,%d,%.2f\n", thread_count, hb, avg_throughput);
        }
    }

    fclose(indep_file);
    fclose(conc_file);
    free(tuples);

    // Free dynamically allocated memory for result arrays
    for (int i = 0; i < num_thread_options; i++) {
        free(indep_results[i]);
        free(conc_results[i]);
    }
    free(indep_results);
    free(conc_results);

    return 0;
}

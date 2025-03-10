#include <stdlib.h>
#include <stdio.h>

#ifdef __linux__
#include <sys/random.h>
#endif

#include "project.h"
#include "concurrent.h"
#include "independent.h"
#include "utils.h"

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
    unsigned char *buffer = malloc(count * 16);
    if (buffer == NULL) {
        return NULL;
    }
    getrandom(buffer, count * 16, 0);
    return (tuple_t *)buffer;
}

int main(int argc, char* argv[]) {
    const int tuple_count = 1 << 24;
    const int num_runs = 5;  // Number of times to run the experiments

    tuple_t *tuples = generate_tuples(tuple_count);
    if (!tuples) {
        fprintf(stderr, "Error generating tuples.\n");
        return -1;
    }

    int thread_options[] = {1, 2, 4, 8, 16};
    int num_thread_options = sizeof(thread_options) / sizeof(thread_options[0]);
    int min_hash_bits = 3;
    int max_hash_bits = 8;

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
                double start_time = get_time_in_seconds();
                int rc = run_concurrent(tuples, tuple_count, thread_count, partition_count);
                double end_time = get_time_in_seconds();
                if (rc != 0) {
                    fprintf(stderr, "Error in run_concurrent with %d threads and partition_count=%d\n", thread_count, partition_count);
                    continue;
                }
                double elapsed = end_time - start_time;
                double throughput = (double)tuple_count / elapsed / 1e6;  // Throughput in million tuples per second
                conc_results[i][hb - min_hash_bits] += throughput;  // Accumulate throughput
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

    // Write averaged results to CSV files.
    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            double avg_throughput = indep_results[i][hb - min_hash_bits] / num_runs;  // Average throughput
            fprintf(indep_file, "independent,%d,%d,%.2f\n", thread_count, hb, avg_throughput);
        }
    }

    for (int i = 0; i < num_thread_options; i++) {
        int thread_count = thread_options[i];
        for (int hb = min_hash_bits; hb <= max_hash_bits; hb++) {
            double avg_throughput = conc_results[i][hb - min_hash_bits] / num_runs;  // Average throughput
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

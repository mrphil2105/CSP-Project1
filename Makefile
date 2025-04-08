SHELL = /bin/bash
CC = gcc
CFLAGS = -O2 -Wall -pthread
LDFLAGS =

# Common sources.
SRCS = utils.c tuples.c thpool.c
HEADERS = project.h utils.h tuples.h thpool.h

# Ensure build directory exists
BUILD_DIR = build
RESULTS_DIR = results
PERF_DIR = perf

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(RESULTS_DIR):
	mkdir -p $(RESULTS_DIR)

$(PERF_DIR):
	mkdir -p $(PERF_DIR)

# -----------------------
# Independent executables (using independent_driver.c)
# -----------------------

# Default (no affinity)
INDEP_SRCS = independent.c independent_driver.c $(SRCS)
independent_no_affinity: $(BUILD_DIR) $(INDEP_SRCS) $(HEADERS) independent.h
	$(CC) $(CFLAGS) -o $(BUILD_DIR)/independent_no_affinity $(INDEP_SRCS) $(LDFLAGS)

# CPU Affinity
independent_cpu_aff: $(BUILD_DIR) $(INDEP_SRCS) $(HEADERS) independent.h affinity.h
	$(CC) $(CFLAGS) -DCPU_AFFINITY -o $(BUILD_DIR)/independent_cpu_aff $(INDEP_SRCS) $(LDFLAGS)

# NUMA Binding
independent_numa: $(BUILD_DIR) $(INDEP_SRCS) $(HEADERS) independent.h affinity.h
	$(CC) $(CFLAGS) -DNUMA_BINDING -o $(BUILD_DIR)/independent_numa $(INDEP_SRCS) -lnuma $(LDFLAGS)

# -----------------------
# Concurrent executables (using concurrent_driver.c)
# -----------------------

CONC_SRCS = concurrent.c concurrent_driver.c $(SRCS)
concurrent_no_affinity: $(BUILD_DIR) $(CONC_SRCS) $(HEADERS) concurrent.h
	$(CC) $(CFLAGS) -o $(BUILD_DIR)/concurrent_no_affinity $(CONC_SRCS) $(LDFLAGS)

concurrent_cpu_aff: $(BUILD_DIR) $(CONC_SRCS) $(HEADERS) concurrent.h affinity.h
	$(CC) $(CFLAGS) -DCPU_AFFINITY -o $(BUILD_DIR)/concurrent_cpu_aff $(CONC_SRCS) $(LDFLAGS)

concurrent_numa: $(BUILD_DIR) $(CONC_SRCS) $(HEADERS) concurrent.h affinity.h
	$(CC) $(CFLAGS) -DNUMA_BINDING -o $(BUILD_DIR)/concurrent_numa $(CONC_SRCS) -lnuma $(LDFLAGS)

# -----------------------
# Run targets using perf stat.
# -----------------------

EVENTS = cpu-cycles,cache-misses,page-faults,cpu-migrations,dTLB-load-misses,context-switches
REPEAT = 5

.PHONY: run_indep_default run_indep_cpu run_indep_numa run_conc_default run_conc_cpu run_conc_numa clean all

all: $(BUILD_DIR) independent_no_affinity independent_cpu_aff independent_numa concurrent_no_affinity concurrent_cpu_aff concurrent_numa

run_indep_default: $(RESULTS_DIR) $(PERF_DIR)
	@echo "Running independent_no_affinity"
	env PREFIX="indep_default_$(shell date +"%d_%m_%H%M%S")" \
		perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/independent_no_affinity \
		1> >(tee $(RESULTS_DIR)/independent_no_affinity_$(shell date +"%d_%m_%H%M%S").txt) \
		2> >(tee $(PERF_DIR)/independent_no_affinity_$(shell date +"%d_%m_%H%M%S").txt)

run_indep_cpu: $(RESULTS_DIR) $(PERF_DIR)
	@echo "Running independent_cpu_aff"
	env PREFIX="indep_cpu_$(shell date +"%d_%m_%H%M%S")" \
		perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/independent_cpu_aff \
		1> >(tee $(RESULTS_DIR)/independent_cpu_aff_$(shell date +"%d_%m_%H%M%S").txt) \
		2> >(tee $(PERF_DIR)/independent_cpu_aff_$(shell date +"%d_%m_%H%M%S").txt)

run_indep_numa: $(RESULTS_DIR) $(PERF_DIR)
	@echo "Running independent_numa"
	env PREFIX="indep_numa_$(shell date +"%d_%m_%H%M%S")" \
		perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/independent_numa \
		1> >(tee $(RESULTS_DIR)/independent_numa_$(shell date +"%d_%m_%H%M%S").txt) \
		2> >(tee $(PERF_DIR)/independent_numa_$(shell date +"%d_%m_%H%M%S").txt)

run_conc_default: $(RESULTS_DIR) $(PERF_DIR)
	@echo "Running concurrent_no_affinity"
	env PREFIX="conc_default_$(shell date +"%d_%m_%H%M%S")" \
		perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/concurrent_no_affinity \
		1> >(tee $(RESULTS_DIR)/concurrent_no_affinity_$(shell date +"%d_%m_%H%M%S").txt) \
		2> >(tee $(PERF_DIR)/concurrent_no_affinity_$(shell date +"%d_%m_%H%M%S").txt)

run_conc_cpu: $(RESULTS_DIR) $(PERF_DIR)
	@echo "Running concurrent_cpu_aff"
	env PREFIX="conc_cpu_$(shell date +"%d_%m_%H%M%S")" \
		perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/concurrent_cpu_aff \
		1> >(tee $(RESULTS_DIR)/concurrent_cpu_aff_$(shell date +"%d_%m_%H%M%S").txt) \
		2> >(tee $(PERF_DIR)/concurrent_cpu_aff_$(shell date +"%d_%m_%H%M%S").txt)

run_conc_numa: $(RESULTS_DIR) $(PERF_DIR)
	@echo "Running concurrent_numa"
	env PREFIX="conc_numa_$(shell date +"%d_%m_%H%M%S")" \
		perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/concurrent_numa \
		1> >(tee $(RESULTS_DIR)/concurrent_numa_$(shell date +"%d_%m_%H%M%S").txt) \
		2> >(tee $(PERF_DIR)/concurrent_numa_$(shell date +"%d_%m_%H%M%S").txt)

clean:
	rm -rf $(BUILD_DIR) $(RESULTS_DIR) $(PERF_DIR)

run_all: run_indep_default run_indep_cpu run_indep_numa run_conc_default run_conc_cpu run_conc_numa
	@echo "All experiments completed!"

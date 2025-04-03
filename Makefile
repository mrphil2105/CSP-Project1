CC = gcc
CFLAGS = -O2 -Wall -pthread
LDFLAGS =

# Common sources.
SRCS = utils.c tuples.c
HEADERS = project.h utils.h tuples.h

# Ensure build directory exists
BUILD_DIR = build
RESULTS_DIR = results

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(RESULTS_DIR):
	mkdir -p $(RESULTS_DIR)

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

run_indep_default: $(RESULTS_DIR)
	@echo "Running independent_no_affinity"
	PREFIX = "indep_default_$(date +"%d_%m_%H%M%S")"
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/independent_no_affinity | tee $(RESULTS_DIR)/independent_no_affinity_$(shell date +"%d_%m_%H%M%S").txt

run_indep_cpu: $(RESULTS_DIR)
	@echo "Running independent_cpu_aff"
	PREFIX = "indep_cpu_$(date +"%d_%m_%H%M%S")"
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/independent_cpu_aff | tee $(RESULTS_DIR)/independent_cpu_aff_$(shell date +"%d_%m_%H%M%S").txt

run_indep_numa: $(RESULTS_DIR)
	@echo "Running independent_numa"
	PREFIX = "indep_numa_$(date +"%d_%m_%H%M%S")"
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/independent_numa | tee $(RESULTS_DIR)/independent_numa_$(shell date +"%d_%m_%H%M%S").txt

run_conc_default: $(RESULTS_DIR)
	@echo "Running concurrent_no_affinity"
	PREFIX = "conc_default_$(date +"%d_%m_%H%M%S")"
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/concurrent_no_affinity | tee $(RESULTS_DIR)/concurrent_no_affinity_$(shell date +"%d_%m_%H%M%S").txt

run_conc_cpu: $(RESULTS_DIR)
	@echo "Running concurrent_cpu_aff"
	PREFIX = "conc_cpu_$(date +"%d_%m_%H%M%S")"
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/concurrent_cpu_aff | tee $(RESULTS_DIR)/concurrent_cpu_aff_$(shell date +"%d_%m_%H%M%S").txt

run_conc_numa: $(RESULTS_DIR)
	@echo "Running concurrent_numa"
	PREFIX = "conc_numa_$(date +"%d_%m_%H%M%S")"
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/concurrent_numa | tee $(RESULTS_DIR)/concurrent_numa_$(shell date +"%d_%m_%H%M%S").txt

clean:
	rm -rf $(BUILD_DIR) $(RESULTS_DIR)

run_all: run_indep_default run_indep_cpu run_indep_numa run_conc_default run_conc_cpu run_conc_numa
	@echo "All experiments completed!"

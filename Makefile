SHELL = /bin/bash
CC = gcc
CFLAGS = -O2 -Wall -pthread
LDFLAGS =

# Common sources.
SRCS = utils.c tuples.c thpool.c
HEADERS = project.h utils.h tuples.h thpool.h

# Directories.
BUILD_DIR = build
RESULTS_DIR = results
PERF_DIR = perf

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(RESULTS_DIR):
	mkdir -p $(RESULTS_DIR)

$(PERF_DIR):
	mkdir -p $(PERF_DIR)

# Experiment parameters.
THREADS = 1 2 4 8 16 32
HASHBITS = 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18

# Perf parameters.
REPEAT = 5
EVENTS = cpu-cycles,cache-misses,page-faults,cpu-migrations,dTLB-load-misses,context-switches
PERF ?= /usr/bin/perf

# -----------------------
# Build Targets for Independent Variants.
# -----------------------
INDEP_SRCS = independent.c independent_driver.c $(SRCS)

independent_no_affinity: $(BUILD_DIR) $(INDEP_SRCS) $(HEADERS) independent.h
	$(CC) $(CFLAGS) -o $(BUILD_DIR)/independent_no_affinity $(INDEP_SRCS) $(LDFLAGS)

independent_cpu_aff: $(BUILD_DIR) $(INDEP_SRCS) $(HEADERS) independent.h affinity.h
	$(CC) $(CFLAGS) -DCPU_AFFINITY -o $(BUILD_DIR)/independent_cpu_aff $(INDEP_SRCS) $(LDFLAGS)

independent_numa: $(BUILD_DIR) $(INDEP_SRCS) $(HEADERS) independent.h affinity.h
	$(CC) $(CFLAGS) -DNUMA_BINDING -o $(BUILD_DIR)/independent_numa $(INDEP_SRCS) -lnuma $(LDFLAGS)

# -----------------------
# Build Targets for Concurrent Variants.
# -----------------------
CONC_SRCS = concurrent.c concurrent_driver.c $(SRCS)

concurrent_no_affinity: $(BUILD_DIR) $(CONC_SRCS) $(HEADERS) concurrent.h
	$(CC) $(CFLAGS) -o $(BUILD_DIR)/concurrent_no_affinity $(CONC_SRCS) $(LDFLAGS)

concurrent_cpu_aff: $(BUILD_DIR) $(CONC_SRCS) $(HEADERS) concurrent.h affinity.h
	$(CC) $(CFLAGS) -DCPU_AFFINITY -o $(BUILD_DIR)/concurrent_cpu_aff $(CONC_SRCS) $(LDFLAGS)

concurrent_numa: $(BUILD_DIR) $(CONC_SRCS) $(HEADERS) concurrent.h affinity.h
	$(CC) $(CFLAGS) -DNUMA_BINDING -o $(BUILD_DIR)/concurrent_numa $(CONC_SRCS) -lnuma $(LDFLAGS)

# Build All.
all: $(BUILD_DIR) independent_no_affinity independent_cpu_aff independent_numa concurrent_no_affinity concurrent_cpu_aff concurrent_numa

# -----------------------
# Macro for Aggregated Run Targets (one parameter).
# This macro now runs the target binary through perf so that perf's output is captured.
# -----------------------
define RUN_TARGET
	@mkdir -p $(RESULTS_DIR)
	@mkdir -p $(PERF_DIR)
	@echo "Running $(1) experiments..."
	@result_file="$(RESULTS_DIR)/$(1)_results.txt"; \
	perf_file="$(PERF_DIR)/$(1)_perf.txt"; \
	echo "Threads,HashBits,Throughput" > $$result_file; \
	echo "===== $(1) experiments (run at $$(date)) =====" > $$perf_file; \
	for t in $(THREADS); do \
	  for hb in $(HASHBITS); do \
	    echo ">>> Running $(1) with $$t threads and $$hb hashbits at $$(date)" | tee -a $$perf_file; \
	    $(PERF) stat -e $(EVENTS) --repeat=$(REPEAT) ./$(BUILD_DIR)/$(1) $$t $$hb 1> tmp_out.txt 2> tmp_err.txt; \
	    cat tmp_out.txt >> $$result_file; \
	    echo "----" >> $$perf_file; \
	    cat tmp_err.txt >> $$perf_file; \
	    echo "" >> $$perf_file; \
	  done; \
	done; \
	rm -f tmp_out.txt tmp_err.txt
endef

# -----------------------
# Aggregated Run Targets for Independent Variants.
# -----------------------
.PHONY: run_indep_default
run_indep_default:
	$(call RUN_TARGET,independent_no_affinity)

.PHONY: run_indep_cpu
run_indep_cpu:
	$(call RUN_TARGET,independent_cpu_aff)

.PHONY: run_indep_numa
run_indep_numa:
	$(call RUN_TARGET,independent_numa)

# -----------------------
# Aggregated Run Targets for Concurrent Variants.
# -----------------------
.PHONY: run_conc_default
run_conc_default:
	$(call RUN_TARGET,concurrent_no_affinity)

.PHONY: run_conc_cpu
run_conc_cpu:
	$(call RUN_TARGET,concurrent_cpu_aff)

.PHONY: run_conc_numa
run_conc_numa:
	$(call RUN_TARGET,concurrent_numa)

# -----------------------
# Master Run Target.
# -----------------------
.PHONY: run_all
run_all: run_indep_default run_indep_cpu run_indep_numa run_conc_default run_conc_cpu run_conc_numa
	@echo "All experiments completed!"

# -----------------------
# Cleanup.
# -----------------------
.PHONY: clean
clean:
	rm -rf $(BUILD_DIR) $(RESULTS_DIR) $(PERF_DIR)

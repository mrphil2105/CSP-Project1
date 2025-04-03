CC = gcc
CFLAGS = -O2 -Wall -pthread
LDFLAGS =

# Common sources.
SRCS = utils.c tuples.c
HEADERS = project.h utils.h tuples.h

# -----------------------
# Independent executables (using independent_driver.c)
# -----------------------

# Default (no affinity)
INDEP_SRCS = independent.c independent_driver.c $(SRCS)
independent_no_affinity: $(INDEP_SRCS) $(HEADERS) independent.h
	$(CC) $(CFLAGS) -o build/independent_no_affinity $(INDEP_SRCS) $(LDFLAGS)

# CPU Affinity
independent_cpu_aff: $(INDEP_SRCS) $(HEADERS) independent.h affinity.h
	$(CC) $(CFLAGS) -DCPU_AFFINITY -o build/independent_cpu_aff $(INDEP_SRCS) $(LDFLAGS)

# NUMA Binding
independent_numa: $(INDEP_SRCS) $(HEADERS) independent.h affinity.h
	$(CC) $(CFLAGS) -DNUMA_BINDING -o build/independent_numa $(INDEP_SRCS) -lnuma $(LDFLAGS)

# -----------------------
# Concurrent executables (using concurrent_driver.c)
# -----------------------

CONC_SRCS = concurrent.c concurrent_driver.c $(SRCS)
concurrent_no_affinity: $(CONC_SRCS) $(HEADERS) concurrent.h
	$(CC) $(CFLAGS) -o build/concurrent_no_affinity $(CONC_SRCS) $(LDFLAGS)

concurrent_cpu_aff: $(CONC_SRCS) $(HEADERS) concurrent.h affinity.h
	$(CC) $(CFLAGS) -DCPU_AFFINITY -o build/concurrent_cpu_aff $(CONC_SRCS) $(LDFLAGS)

concurrent_numa: $(CONC_SRCS) $(HEADERS) concurrent.h affinity.h
	$(CC) $(CFLAGS) -DNUMA_BINDING -o build/concurrent_numa $(CONC_SRCS) -lnuma $(LDFLAGS)

# -----------------------
# Run targets using perf stat.
# -----------------------

EVENTS = cpu-cycles,cache-misses,page-faults,cpu-migrations,dTLB-load-misses,context-switches
REPEAT = 5

.PHONY: run_indep_default run_indep_cpu run_indep_numa run_conc_default run_conc_cpu run_conc_numa clean all

all: independent_no_affinity independent_cpu_aff independent_numa concurrent_no_affinity concurrent_cpu_aff concurrent_numa

# Make sure the build and results folders exist.
prepare:
	@mkdir -p build results

run_indep_default: prepare independent_no_affinity
	@echo "=== Running Independent Experiment: No Affinity ==="
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./build/independent_no_affinity | tee results/independent_no_affinity_$(shell date +"%d_%m_%H%M%S").txt

run_indep_cpu: prepare independent_cpu_aff
	@echo "=== Running Independent Experiment: CPU Affinity ==="
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./build/independent_cpu_aff | tee results/independent_cpu_aff_$(shell date +"%d_%m_%H%M%S").txt

run_indep_numa: prepare independent_numa
	@echo "=== Running Independent Experiment: NUMA Binding ==="
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./build/independent_numa | tee results/independent_numa_$(shell date +"%d_%m_%H%M%S").txt

run_conc_default: prepare concurrent_no_affinity
	@echo "=== Running Concurrent Experiment: No Affinity ==="
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./build/concurrent_no_affinity | tee results/concurrent_no_affinity_$(shell date +"%d_%m_%H%M%S").txt

run_conc_cpu: prepare concurrent_cpu_aff
	@echo "=== Running Concurrent Experiment: CPU Affinity ==="
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./build/concurrent_cpu_aff | tee results/concurrent_cpu_aff_$(shell date +"%d_%m_%H%M%S").txt

run_conc_numa: prepare concurrent_numa
	@echo "=== Running Concurrent Experiment: NUMA Binding ==="
	perf stat -e $(EVENTS) --repeat=$(REPEAT) ./build/concurrent_numa | tee results/concurrent_numa_$(shell date +"%d_%m_%H%M%S").txt

run_all: run_indep_default run_indep_cpu run_indep_numa run_conc_default run_conc_cpu run_conc_numa
	@echo "=== All experiments completed ==="

clean:
	rm -f build/* results/*

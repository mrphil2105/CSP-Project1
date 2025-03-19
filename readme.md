### Running perf

To get performance metrics with perf, then run the following command:
```bash 
perf stat -e cycles,instructions,cache-references,cache-misses ./project

# Recoding and analyzing the data

perf record -g ./project

perf report
```
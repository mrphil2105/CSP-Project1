#include <pthread.h>
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int count;
    int crossing;
} pthread_barrier_t;

int hash_to_partition(const unsigned char *key, int partition_count);
double get_time_in_seconds();
int pthread_barrier_destroy(pthread_barrier_t *barrier);
int pthread_barrier_wait(pthread_barrier_t *barrier);
int pthread_barrier_init(pthread_barrier_t *barrier, void *attr, int count);
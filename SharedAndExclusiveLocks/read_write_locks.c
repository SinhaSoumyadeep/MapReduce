#include <unistd.h>
#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>

#define SLEEP_MS 1000000 /* 1 millisecond == 1 million nsec. */
#define NUM_TASKS 10
#define NUM_READERS 2
#define NUM_WRITERS 2

extern void *reader(void *);
extern void begin_read(void);
extern void end_read(void);
extern void *writer(void *);
extern void begin_write(void);
extern void end_write(void);

extern void acquire_shared_lock(void);
extern void release_shared_lock(void);
extern void acquire_exclusive_lock(void);
extern void release_exclusive_lock(void);

static int num_readers = 0;
static int num_writers = 0;
static int cum_num_reads = 0;
static int cum_num_writes = 0;
struct timespec sleep_time = {0, 10 * SLEEP_MS}; // sleep for 10 milliseconds

int main(int argc, char *argv[]) {
  alarm(300); // Kill this program after 300 seconds; prevent runaway processes
  struct timespec start, finish;
  clock_gettime(CLOCK_REALTIME, &start);

  int i;
  pthread_t thread[100];

  for (i = 0; i < NUM_READERS; i++) {
    pthread_create(&thread[i], NULL, reader, NULL);
  }
  for (i = NUM_READERS; i < NUM_READERS + NUM_WRITERS; i++) {
    pthread_create(&thread[i], NULL, writer, NULL);
  }

  // Wait for threads to finish.
  for (i = 0; i < NUM_READERS + NUM_WRITERS; i++) {
    pthread_join(thread[i], NULL);
  }
  clock_gettime(CLOCK_REALTIME, &finish);
  long milliseconds = (finish.tv_nsec - start.tv_nsec) / 1000000;
  milliseconds += (finish.tv_sec - start.tv_sec) * 1000;
  printf("*** %d reads and %d writes in %ld milliseconds\n",
         cum_num_reads, cum_num_writes, milliseconds);
  return 0;
}

void *reader(void *dummy) {
  int i;
  for (i = 0; i < NUM_TASKS; i++) {
    acquire_shared_lock();
    begin_read();
    nanosleep(&sleep_time, NULL);
    end_read();
    release_shared_lock();
  }
  return NULL;
}

void *writer(void *dummy) {
  int i;
  for (i = 0; i < NUM_TASKS; i++) {
    acquire_exclusive_lock();
    begin_write();
    nanosleep(&sleep_time, NULL);
    end_write();
    release_exclusive_lock();
  }
  return NULL;
}

/* void do_work() {
 *   int i;
 *   for (i = 0; i < NUM_TASKS; i++) {
 *     enum operation op = get_op();
 *     if (op == READ) {
 *       acquire_shared_lock();
 *       begin_read();
 *       nanosleep(&sleep_time);
 *       end_read();
 *       release_shared_lock();
 *     } else if (op == WRITE) {
 *       acquire_exclusive_lock();
 *       begin_write();
 *       nanosleep(&sleep_time);
 *       end_write();
 *       release_exclusive_lock();
 *     }
 *   }
 * }
 */

pthread_mutex_t task_stats_lock = PTHREAD_MUTEX_INITIALIZER;  

void begin_read() {
  pthread_mutex_lock(&task_stats_lock);
  num_readers++;
  cum_num_reads++;
  assert(num_writers == 0);
  pthread_mutex_unlock(&task_stats_lock);
}
void end_read() {
  pthread_mutex_lock(&task_stats_lock);
  assert(num_writers == 0);
  num_readers--;
  pthread_mutex_unlock(&task_stats_lock);
}

void begin_write() {
  pthread_mutex_lock(&task_stats_lock);
  num_writers++;
  cum_num_writes++;
  assert(num_writers == 1 && num_readers == 0);
  pthread_mutex_unlock(&task_stats_lock);
}
void end_write() {
  pthread_mutex_lock(&task_stats_lock);
  assert(num_writers == 1 && num_readers == 0);
  num_writers--;
  pthread_mutex_unlock(&task_stats_lock);
}

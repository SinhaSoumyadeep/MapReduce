#include <pthread.h>

pthread_mutex_t acquire_release_lock = PTHREAD_MUTEX_INITIALIZER;


void acquire_shared_lock() {

  pthread_mutex_lock(&acquire_release_lock);

}

void release_shared_lock() {

  pthread_mutex_unlock(&acquire_release_lock);

}

void acquire_exclusive_lock() {
  
  pthread_mutex_lock(&acquire_release_lock);
     
}

void release_exclusive_lock() {

  pthread_mutex_unlock(&acquire_release_lock);

}

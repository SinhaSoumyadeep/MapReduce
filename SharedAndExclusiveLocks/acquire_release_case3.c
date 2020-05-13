
#include <time.h>

#include <pthread.h>

 int num_with_shared_lock = 0;
 int num_with_excl_lock = 0;

 pthread_mutex_t acquire_release_lock = PTHREAD_MUTEX_INITIALIZER; 
 pthread_mutex_t read_lock = PTHREAD_MUTEX_INITIALIZER; 


void acquire_shared_lock() {

 while (1) {
 	
       pthread_mutex_lock(&acquire_release_lock);
       if (num_with_excl_lock > 0) {
         pthread_mutex_unlock(&acquire_release_lock);
         struct timespec ten_milliseconds = {0, 10000000};
         nanosleep(&ten_milliseconds, NULL);
         continue;
       }
       num_with_shared_lock++;
       if (num_with_excl_lock == 0 && num_with_shared_lock == 1) {
         pthread_mutex_lock(&read_lock);
       }
       pthread_mutex_unlock(&acquire_release_lock);
       break;  // We now have the shared lock.
     }

}

void release_shared_lock() {
	pthread_mutex_lock(&acquire_release_lock);
     num_with_shared_lock--;
     if (num_with_shared_lock == 0) {
       pthread_mutex_unlock(&read_lock);
     }
    pthread_mutex_unlock(&acquire_release_lock);
}

void acquire_exclusive_lock() {
while (1) {
	 pthread_mutex_lock(&acquire_release_lock);

        if (num_with_excl_lock == 0 && num_with_shared_lock == 0) {
          num_with_excl_lock++;
          pthread_mutex_unlock(&acquire_release_lock);
          break; // We now have the exclusive lock
        } else {
          struct timespec ten_milliseconds = {0, 10000000};
          nanosleep(&ten_milliseconds, NULL);
        }
       pthread_mutex_unlock(&acquire_release_lock);
      
    }

}

void release_exclusive_lock() {

	pthread_mutex_lock(&acquire_release_lock);
     num_with_excl_lock--;
    pthread_mutex_unlock(&acquire_release_lock);

}

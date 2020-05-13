Copyright (c) 2020, Gene Cooperman (gene@ccs.neu.edu)
This work may be freely copied and modified as long as this
copyright statement remains.

	*** Homework 5:  shared and exclusive locks ***

[ See DELIVERABLES at the end of the four questions. ]

This homework is implemented based on locks in POSIX threads.  In a larger,
big data example, we would be using distributed threads across multiple
computers.  However, for ease of implementation and discovery by the students,
we will do everything with POSIX threads here.

The file read_write_locks.c has already been written for you.  It contains
the main routine, and the commands to generate threads and to invoke
mutex locks.  It is your job to implement shared and exclusive locks
on top of them, by exploring the several design strategies below.

Shared and exclusive locks are a critical component in the implementation
of MapReduce/Hadoop and also in the implementation of HBase (Google Bigtable).
This homework allows you to explore shared and exclusive locks in the
friendly domain of a single computer.  There are standard recipes for
converting your single-computer implementations of shared/exclusive locks
into implementations for distributed computers (clusters).  You would learn
those extensions to computer clusters in a course on Database Internals.
For example, see:
  CS 4200 - Database Internals:
    https://wl11gp.neu.edu/udcprod8/bwckctlg.p_disp_course_detail?cat_term_in=201830&subj_code_in=CS&crse_numb_in=4200

The file read_write_locks.c is an application that will generate
two reader threads and two writer threads.  each thread will run in a loop,
and either:
  READER:
   * acquire a shared lock
   * do the read
   * release the shared lock
or:
  WRITER:
   * acquire an exclusive lock
   * do the write
   * release the exclusive lock

Your job is to write the four routines:
  void acquire_shared_lock();
  void release_shared_lock();
  void acquire_exclusive_lock();
  void release_exclusive_lock();

You will write those function definitions inside a file
acquire_release_caseX.c, for caseX being case1, case2, etc.
You can then run it using:
  make case1
  make case2
  (etc.)

1. First, define these four functions as the trivial functions (do nothing).
   (This has already been done for you in the file acquire_release_case1.c)
   Then do:  make case1

  a. Observe that you hit an assert statement as readers and writers conflict.
     Explain why.

  b. Now let's look at the running time, if we ignore the assert statement.  Do:
       make clean
       make NOASSERT=1 case1
       make clean
     Observe that now the code completes with a time of about 100 milliseconds.
     Explain why.
     (HINT:  If you look closely at read_write_locks.c, you'll see that there
      are 2 readers and 2 writers.  Each reader and each writer does 10 tasks,
      and for each task, it sleeps 10 milliseconds.)

2. Next, let's fix the bug that the assert statement showed to us.
  Define these four functions so that each acquire_*_lock() calls:
     pthread_mutex_lock(&acquire_release_lock);
  and so that each release_*_lock() function calls:
     pthread_mutex_unlock(&acquire_release_lock);
  (You will also need to define 'acquire_release_lock', similarly to the way
  that 'task_stats_lock' is defined in read_write_locks.c, and also
  include the 'pthread.h' file.)
  Write this in acquire_release_case2.c.
  Then do:  make case2

  This caused each reader and writer to wait to acquire the
    "acquire_release_lock".
  Observe the number of reads and writes, as well as the total time taken.
  There is a clear relationship between the number of reads/writes and
  the total time taken.  Describe the relationship, and explain why.

3. Next, let's see if we can produce a more efficient version of this solution.
   In acquire_release_case3.c, define global variables:
     int num_with_shared_lock = 0;
     int num_with_excl_lock = 0;
   For acquire_shared_lock(), do:
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
   For release_shared_lock(), do:
     pthread_mutex_lock(&acquire_release_lock);
     num_with_shared_lock--;
     if (num_with_shared_lock == 0) {
       pthread_mutex_unlock(&read_lock);
     }
     pthread_mutex_unlock(&acquire_release_lock);
  Then define *_exclusive_lock() appropriately to test that
  "num_with_shared_lock == 0" before acquiring the exclusive lock.
  It should sleep and try again later if it finds that num_reader > 0.
  Define *_exclusive_lock() compatibly with the above design.
  In particular, acquire_exclusive_lock() should follow the pattern, below.
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


  a. With the above design, do:  make case3
     You will likely see the code continue in an infinite loop.
     Explain what you see, and why.

  b. Now, modify acquire_exclusive_lock() by moving the final
     'pthread_mutex_lock()' to before the 'nanosleep()' instead
     of after it.
     Then do:  make case3
     You will likely see the bug go away.
     Explain what happens, and why.

4. We saw that there was a bug in Case 3, when NUM_READERS is set to 2.
   In acquire_release_case4.c, define global variables (and needed 'include's):
     #include <stdio.h>
     #include <time.h>
     #include <pthread.h>

     pthread_mutex_t acquire_release_lock = PTHREAD_MUTEX_INITIALIZER;
     pthread_mutex_t write_lock = PTHREAD_MUTEX_INITIALIZER;

     int num_with_shared_lock = 0;
     int num_with_excl_lock = 0;





   For acquire_exclusive_lock(), define the function as follows:



   
     int has_lock = 0;
     while (! has_lock) {
       pthread_mutex_lock(&acquire_release_lock);
       if (num_with_shared_lock == 0 && num_with_excl_lock == 0) {
         num_with_excl_lock++;
         pthread_mutex_lock(&write_lock);
         has_lock = 1;
       }
       pthread_mutex_unlock(&acquire_release_lock);
       if (! has_lock) {  // delay before trying again for the lock
         struct timespec ten_milliseconds = {0, 10000000};
         nanosleep(&ten_milliseconds, NULL);
       }
     }







   Write the appropriate definitions for:
    * void release_exclusive_lock();
    * void acquire_shared_lock();
    * void release_shared_lock();
  There must be no calls to sleep or nanosleep in the remaining definitions.
  HINT:  For the reader function, think about testing if the write_lock is held.

  In the following, run your program on a computer with at least 5 CPU cores
  and preferably 10 or more.  The machine login.ccs.neu.edu has 48 CPU cores,
  but use the command 'top' to check if too many users are on it at one time.

  a. Then do:  make case4
     Explain what happens, and why.

  b. Next, modify read_write_locks.c to set NUM_READERS to 5.
     Do:  make case4
     Explain how the printed statistics change and why.

  c. Next, also modify read_write_locks.c to set NUM_WRITERS to 5.
     Do:  make case4
     Explain how the printed statistics change and why.

  d. Develop a formula in terms of NUM_READERS and NUM_WRITERS
     that will predict the time to complete the program read_write_locks,
     when this program is executed on a computer with 10 CPU cores available
     to the program.  (This formula will only be approximate, but see how
     well you can predict the times for a large number of readers, writers,
     or both.)

========
DELIVERABLES:
  For Deliverables, please submit:
    A.  A report with the answers to the 4 questions, clearly labelled.
        (For example:   QUESTION 1.a. ...
                        QUESTION 1.b. ...
        )
        The report may be a .txt or .pdf file, but make sure it has a filetype.
    B.  The four files that you wrote in C:
            acquire_release_caseX.c , for X = 1,2,3,4


========
FOR CLASS DISCUSSION (not required for the homework)

DELVING FURTHER INTO LOCKS USING POSIX THREADS:
In Case 3.b above, we hit a bug due to limitations of mutexes.
Semaphores were invented as a clean abstraction to allow two or more
POSIX threads to interact with each other.  For the curious who want to
dig deeper, read 'man sem_wait', 'man sem_post', and:
  https://en.wikipedia.org/wiki/Producer%E2%80%93consumer_problem

In case 4 above, in acquire_exclusive_lock(), a writer might have to
sleep in between attempts to acquire the lock.  Condition variables
and monitors were invented as a clean abstraction to avoid calls to sleep.
For the curious who want to dig deeper, read 'man pthread_cond_wait',
'man pthread_cond_signal', and:
  https://en.wikipedia.org/wiki/Readers%E2%80%93writers_problem
and:
  https://en.wikipedia.org/wiki/Monitor_(synchronization)
Condition variables (or monitors) are used to enforce an invariant
condition, such as:
  num_with_excl_lock == 0 ||
      (num_with_excl_lock == 1 && num_with_shared_lock == 0)

DELVING FURTHER INTO HANDLING MULTIPLE DATA ITEMS:
There is no time in this semester to explore this further through
implementation.  But for those who are interested, imagine that we have
10 items of data.  Suppose we have 10 fine-grained locks (one lock for
each item of data), and one coarse lock to lock everything.  Assume that
each type of lock includes a shared and an exclusive lock.  How would you
generalize the scheme above, if you had coarse- and fine-grained locks?

DELVING FURTHER IN DISTRIBUTED IMPLEMENTATIONS:
In Google's Bigtables, they use B-trees.  How would you generalize this
if the database was using B-trees?

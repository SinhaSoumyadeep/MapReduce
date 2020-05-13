# FUTURE WORK:
#   CONSIDER ARGUMENT FOR PRINTING:  STATS AND TIME.
#
#   SLEEP_MS has been made a command-line argument.
#    But it is not yet used in the code.
#  
#   Consider making these other parameters into arguments:
#    #define NUM_TASKS 10
#    #define NUM_READERS 2
#    #define NUM_WRITERS 2

FILE = acquire_release

# We use '-g3' instead of '-g', to see macro values.
CFLAGS=-g3 -O0
ifdef NOASSERT
  CFLAGS += -DNDEBUG=1
endif

default: read_write_locks

case%: acquire_release_case%.c
	cp $< acquire_release.c
	touch acquire_release.c
	${MAKE} check

${FILE}.o: ${FILE}.c
	gcc -c ${CFLAGS} $<

lib${FILE}.a: ${FILE}.o
	ar rc $@ $^

read_write_locks: read_write_locks.c lib${FILE}.a
	gcc ${CFLAGS} -o $@ $^ -lpthread

check: read_write_locks
	./read_write_locks ${SLEEP_MS}

gdb: read_write_locks
	gdb --args read_write_locks

vi: ${FILE}.c
	vi ${FILE}.c

emacs: ${FILE}.c
	emacs ${FILE}.c

clean:
	rm -f a.out *~ *.o lib${FILE}.a read_write_locks

dist: clean
	dir=`basename $$PWD`; cd ..; tar cvf $$dir.tar ./$$dir; gzip $$dir.tar
	dir=`basename $$PWD`; ls -l ../$$dir.tar.gz

homework: clean
	echo "COPYING acquire_release'*'.c files to /tmp for safety!"
	chmod go-rwx acquire_release*.c
	if test -f acquire_release.c; then \
	  cp acquire_release.c /tmp/; \
	fi
	rm -f acquire_release.c
	${MAKE} dist

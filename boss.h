#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <sys/times.h>
#include <sys/time.h>
#include <ctype.h>


long int boss(long int numKeys, int procs);

long int employee(long int numKeys, int procs);

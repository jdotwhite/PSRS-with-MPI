#include <stdio.h>
#include "materialsl.c"
#include "mpi.h"

int main( int argc, char *argv[]){
	if (argc!=3){
		printf("Usage Error: ./worker [# of keys] [# of machines]\n");
		exit(EXIT_FAILURE);
	}


	MPI_Init(&argc, &argv);

	int numKeys = atoi(argv[1]);
	int procs;

	int rank, size;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &procs);

	MPI_Status status;




}

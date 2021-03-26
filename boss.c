#include "materials.c"
#include "boss.h"

int main( int argc, char *argv[]){
	
	if (argc!=3){
		printf("Usage Error: ./psrs [# of keys] [# of machines]\n");
		exit(EXIT_FAILURE);
	}
	MPI_Init( &argc, &argv);

	int numKeys = atoi(argv[1]);
	int procs = atoi(argv[2]);
	
	// check for correctness of inputs
	if (numKeys < procs*procs){
		printf("Usage: # of processors squared must be smaller than number of keys");
		exit(EXIT_FAILURE);
	}

	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	if (rank == 0){
	// then this is the "boss" process
		printf("boss proc\n");
		boss(numKeys, procs);
	       	

	}
	else{
		printf("Just an employee\n");
		employee(numKeys, procs);
	}
	printf("here\n");
	MPI_Finalize();
	return 0;




}

long int boss(long int numKeys, int procs){
	
	//take a message from each employee to make sure they're all running
	MPI_Status status;
	int dummy;
	//for(int i=1; i<procs; i++){
	//	MPI_Recv(&dummy, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
	//}

	int p = procs / 2;
	int w = numKeys / (procs*procs);
	
	long int localKeys = numKeys / procs;
	
	//Begin phase 1
	srandom(rank * 23);
	long int *array = (long int*)malloc(localKeys * sizeof(long int));
	array = genKeys(localKeys);
	qsort(array, localKeys, sizeof(long int), comparison);
	
	long int samples[procs*procs];
	//find samples
	for(int sample = 0; sample < procs; sample++){
		samples[sample] = array[sample*w];
		printf("%ld\n", samples[sample]);
	}
	//Phase 2, begin by receiving other samples
	//we are in rank 0, so begin at 1 as we already have local samples
	long int* sampleBuff = malloc(procs*sizeof(long int));
	for(int i=1; i < procs; i++){
		MPI_Recv(sampleBuff, procs, MPI_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		for(int x = 0; x < procs; x++){
			int index = i*procs + x;
			samples[index] = sampleBuff[x];
		}

	}
	free(sampleBuff);

	//now we have the samples from all processors
	//sort them, then select pivots using p and procs values
	
	qsort(samples, procs*procs, sizeof(long int), comparison);
	long int pivots[procs-1];
	for(int mult = 1; mult<p; mult++){
		pivots[mult-1] = samples[(p + mult*procs)-1];
	}
	//have pivots, now send them out to employees
	for(int i=1; i<procs; i++){
		MPI_Send(pivots, procs-1, MPI_LONG, i, 0, MPI_COMM_WORLD);
	}
	//Now create partitions in both boss proc and employees
	long int* partitions[procs];
	long int subsizes[procs];
	
	long int index = 0;
	for(int piv = 0; piv<procs-1; piv++){
		long int count = 0;
		long int initial = index; 
		//partitions[piv] = (long int*)malloc(localKeys*sizeof(long int));
	
		while(array[index] < pivots[piv]){
			
			//partitions[piv][subIndex] = array[index];
			//printf("%ld\n", partitions[piv][subIndex]);
			index++;
			count++;
	
	}
		partitions[piv] = (long int*)malloc(count * sizeof(long int));
		memcpy(partitions[piv], &array[initial], count*sizeof(long int));
		subsizes[piv] = count;

	}
	//partitions[procs-1] = (long int*)malloc(localKeys*sizeof(long int));
	long int count = 0;
	long int initial = index; 
	while(index < localKeys){
		//partitions[procs-1][subIndex] = array[index];
		//printf("index %ld: %ld\n", index, partitions[procs-1][subIndex]);
		index++;
		count++;
	}
	partitions[procs-1] = (long int*)malloc(count * sizeof(long int));
	memcpy(partitions[procs-1], &array[initial], count*sizeof(long int));
	subsizes[procs-1] = count;
	//for(int index = 0; index<localKeys; index++){
	//	printf("index %ld: %ld\n", index, partitions[procs-1][index]);
	//}
	free(array);

	return 0;
}


long int employee(long int numKeys, int procs){
	
	MPI_Status status;
	int dummy = 0;
	//MPI_Send(&dummy, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

	int p = procs / 2;
	int w = numKeys / (procs*procs);
	
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	
	long int firstKey = (rank * numKeys) / procs;
	long int lastKey = ((rank+1)*numKeys) / procs;
	long int localKeys = lastKey - firstKey;

	//Begin phase 1
	//create local array, sort and find samples
	srandom(rank * 23);
	long int *array = (long int*)malloc(localKeys * sizeof(long int));
	array = genKeys(localKeys);
	qsort(array, localKeys, sizeof(long int), comparison);

	long int samples[procs*procs];
	//find samples
	for(int sample = 0; sample < procs; sample++){
		samples[sample] = array[sample*w];
		printf("%ld\n", samples[sample]);
	}
	printf("employee here1\n");
	//samples collected, send those bad boys on their way
	//Begin Phase 2

	long int sendBuff[procs];
	MPI_Send(sendBuff, procs, MPI_LONG, 0, 0, MPI_COMM_WORLD);

	//now receive the pivots
	long int* pivBuff = malloc((procs-1)*sizeof(long int));
	long int pivots[procs-1];
	MPI_Recv(pivBuff, procs-1, MPI_LONG, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
	for(int i=0; i<procs-1; i++){
		pivots[i] = pivBuff[i];
	}
	free(pivBuff);
	printf("employee2");
	free(array);
	return 0;
	}


	








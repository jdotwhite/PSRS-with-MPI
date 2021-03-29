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
	return EXIT_SUCCESS;




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
	long int *array = genKeys(localKeys);
	qsort(array, localKeys, sizeof(long int), comparison);
	
	long int samples[procs*procs];
	//find samplesn
	printf("Samples:\n");
	for(int sample = 0; sample < procs; sample++){
		samples[sample] = array[sample*w];
		}
	//Phase 2, begin by receiving other samples
	for(int i=1; i < procs; i++){
		long int* sampleBuff = malloc(procs*sizeof(long int));
		MPI_Recv(sampleBuff, procs, MPI_LONG, i , MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		for(int x = 0; x < procs; x++){
			int index = i * procs + x;
			samples[index] = sampleBuff[x];
			}
		free(sampleBuff);

	}
	for(int i = 0; i<procs*procs; i++){
		printf("%ld\n", samples[i]);
	}

	//now we have the samples from all processors
	//sort them, then select pivots using p and procs values
	
	qsort(samples, procs*procs, sizeof(long int), comparison);
	long int* pivots = malloc((procs-1)*sizeof(long int));

	for(int mult = 1; mult<procs; mult++){
		pivots[mult-1] = samples[(p + mult*procs)-1];
	}
	printf("Piv: %ld\n", pivots[0]);
	long int *SendBuff = malloc((procs-1) * sizeof(long int));
	memcpy(SendBuff, pivots, (procs-1)*sizeof(long int));
	//have pivots, now send them out to employees
	for(int i=1; i<procs; i++){
		MPI_Send(SendBuff, procs-1, MPI_LONG, i, 0, MPI_COMM_WORLD);
	}
	free(SendBuff);
	//Now create partitions in both boss proc and employees
	long int* partitions[procs];
	long int subsizes[procs];
	
	long int index = 0;
	long int initial = 0;
	printf("boss here 1\n");
	fflush(stdout);
	for(int piv = 0; piv<procs-1; piv++){
		long int count = 0; 
		
		while((array[index] <= pivots[piv]) && (index < localKeys)){
			index++;
			count++;
	
		}
		subsizes[piv] = count;
		if(count > 0){
			//now we have the partition, send it
			partitions[piv] = (long int*)malloc(count * sizeof(long int));
			memcpy(partitions[piv], &array[initial], count*sizeof(long int));
			printf("\n\n");
			}
		initial = index;

	}
	printf("boss here 2");
	//partitions[procs-1] = (long int*)malloc(localKeys*sizeof(long int));
	long int count = 0; 
	while(index < localKeys){
		index++;
		count++;
	}
	subsizes[procs-1] = count;
	if(count > 0){
		partitions[procs-1] = (long int*)malloc(count * sizeof(long int));
		memcpy(partitions[procs-1], &array[initial], count*sizeof(long int));
		subsizes[procs-1] = count;
	}

	
	for(int sendRank = 1; sendRank<procs; sendRank++){
		long int* partBuff = malloc(subsizes[sendRank]*sizeof(long int));
		memcpy(partBuff, partitions[sendRank], subsizes[sendRank]*sizeof(long int));
		MPI_Send(&subsizes[sendRank], 1, MPI_LONG, sendRank, 0, MPI_COMM_WORLD);
		MPI_Send(partBuff, subsizes[sendRank], MPI_LONG, sendRank, 0, MPI_COMM_WORLD);
		free(partitions[sendRank]);
		}
	
	printf("boss here3\n");

	for (int recvRank = 1; recvRank<procs; recvRank++){
		long int *size = malloc(sizeof(long int));
		MPI_Recv(size, 1, MPI_LONG, recvRank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		subsizes[recvRank] = *size;
		long int *partBuff = malloc( *size * sizeof(long int));
		partitions[recvRank] = malloc( *size * sizeof(long int));
		long int recSize = *size;
		free(size);
		MPI_Recv( partBuff, recSize, MPI_LONG, recvRank, MPI_ANY_TAG, MPI_COMM_WORLD,&status);
		memcpy(partitions[recvRank], partBuff, recSize*sizeof(long int));
		free(partBuff);

		}
	long int running_size = subsizes[0];
	long int* final = partitions[0];
	for(int i=1; i<procs; i++){
		if(subsizes[i] > 0){
			final = MergeSubs(final, partitions[i], running_size, subsizes[i]);
			running_size += subsizes[i];
		}
	}
		
	
	free(array);	
	printf("boss here3\n");
	
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
	long int *array = genKeys(localKeys);
	qsort(array, localKeys, sizeof(long int), comparison);

	long int samples[procs];
	//find samples
	for(int sample = 0; sample < procs; sample++){
		samples[sample] = array[sample*w];
		//printf("%ld\n", samples[sample]);
	}
	printf("employee here1\n");
	//samples collected, send those bad boys on their way
	//Begin Phase 2

	long int* sendBuff = malloc(procs * sizeof(long int));
	memcpy(sendBuff, samples, procs*sizeof(long int));

	MPI_Send(sendBuff, procs, MPI_LONG, 0, 0, MPI_COMM_WORLD);
	
	//now receive the pivots
	long int* pivBuff = malloc((procs-1)*sizeof(long int));
	long int* pivots = malloc((procs-1)*sizeof(long int));
	MPI_Recv(pivBuff, procs-1, MPI_LONG, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
	
	memcpy(pivots, pivBuff, (procs-1)*sizeof(long int));

	free(pivBuff);
	//create our partitions
	

	long int* partitions[procs];
	long int subsizes[procs];
	
	long int index = 0;
	long int initial = 0;
	for(int piv = 0; piv<procs-1; piv++){
		printf("Pivot: %ld\n", pivots[piv]);
		long int count = 0; 

		while((array[index] <= pivots[piv]) && (index < localKeys)){
			index++;
			count++;
	
		}
		if(count > 0){
			partitions[piv] = (long int*)malloc(count * sizeof(long int));
			memcpy(partitions[piv], &array[initial], count*sizeof(long int));
			for(int i=0; i<count;i++){
				}
			
		}
		subsizes[piv] = count;
		initial = index;

	}
	//partitions[procs-1] = (long int*)malloc(localKeys*sizeof(long int));
	printf("employee here 2\n");

	long int count = 0;
	while(index < localKeys){
		index++;
		count++;
	}
	if(count>0){
		partitions[procs-1] = (long int*)malloc(count * sizeof(long int));
		memcpy(partitions[procs-1], &array[initial], count*sizeof(long int));
		subsizes[procs-1] = count;
		}
	//now that we have our partitions and their accompanying sizes, 
	//send the partitions to their appropriate ranks
	
	for(int sendRank = 0; sendRank<procs; sendRank++){
		if (sendRank!=rank){
			long int* partBuff = malloc(subsizes[sendRank]*sizeof(long int));
			memcpy(partBuff, partitions[sendRank], subsizes[sendRank]*sizeof(long int));
			MPI_Send(&subsizes[sendRank], 1, MPI_LONG, sendRank, 0, MPI_COMM_WORLD);
			MPI_Send(partitions[sendRank], subsizes[sendRank], MPI_LONG, sendRank, 0, MPI_COMM_WORLD);
			free(partitions[sendRank]);
		}
		}

	for (int recvRank = 0; recvRank<procs; recvRank++){
		if(recvRank != rank){
			long int *size = malloc(sizeof(long int));
			MPI_Recv(size, 1, MPI_LONG, recvRank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			subsizes[recvRank] = *size;
			long int *partBuff = malloc( *size * sizeof(long int));
			partitions[recvRank] = malloc( *size * sizeof(long int));
			long int recSize = *size;
			free(size);
			MPI_Recv( partBuff, recSize, MPI_LONG, recvRank, MPI_ANY_TAG, MPI_COMM_WORLD,&status);
			memcpy(partitions[recvRank], partBuff, recSize*sizeof(long int));
			free(partBuff);
		}
	}
	//now we have the correct partitions, merge them up
	
	long int running_size = subsizes[0];
	long int* final = partitions[0];
	for(int i=1; i<procs; i++){
		if(subsizes[i] > 0){
			final = MergeSubs(final, partitions[i], running_size, subsizes[i]);
			running_size += subsizes[i];
		}
	}
	for(int index=0; index<running_size; index++){
		printf("%ld, ", final[index]);

	}
	

	free(array);
	printf("employee here 3\n");
	return 0;
	}


	








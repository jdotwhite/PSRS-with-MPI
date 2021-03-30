#include "materials.c"
#include "boss.h"

int main( int argc, char *argv[]){
	
	if (argc!=3){
		printf("Usage Error: ./psrs [# of keys] [# of machines]\n");
		exit(EXIT_FAILURE);
	}
	MPI_Init( &argc, &argv);
	
	double start = MPI_Wtime();

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
		boss(numKeys, procs);
		double end = MPI_Wtime();
		printf("Time elapsed: %.8lf\n", end-start);
		
	       	

	}
	else{
		employee(numKeys, procs);
	}
	MPI_Finalize();
	return EXIT_SUCCESS;




}

long int boss(long int numKeys, int procs){
	
	double t0, t1, t2, t3, t4;
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
	t0 = MPI_Wtime();
	
	qsort(array, localKeys, sizeof(long int), comparison);
	
	long int samples[procs*procs];
	//find samplesn
	for(int sample = 0; sample < procs; sample++){
		samples[sample] = array[sample*w];
		}
	t1 = MPI_Wtime();
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
	
	//now we have the samples from all processors
	//sort them, then select pivots using p and procs values
	
	qsort(samples, procs*procs, sizeof(long int), comparison);
	long int* pivots = malloc((procs-1)*sizeof(long int));

	for(int mult = 1; mult<procs; mult++){
		pivots[mult-1] = samples[(p + mult*procs)-1];
	}
	//printf("Piv: %ld\n", pivots[0]);
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
			}
		initial = index;

	}
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
	t2 = MPI_Wtime();
	//Phase 3, partition exchange
	
	for(int sendRank = 0; sendRank< procs; sendRank++){
		
		long int *partBuff = malloc(subsizes[sendRank]*sizeof(long int));
		long int *sizeRec = malloc(sizeof(long int));
		long int *sizeSend = malloc(sizeof(long int));
		*sizeSend = subsizes[sendRank];
		memcpy(partBuff, partitions[sendRank], subsizes[sendRank]*sizeof(long int));
		free(partitions[sendRank]);
		//send and recv sizes
		MPI_Sendrecv(sizeSend, 1, MPI_LONG, sendRank, 0, sizeRec, 1, MPI_LONG, sendRank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		subsizes[sendRank] = *sizeRec;
		partitions[sendRank] = malloc(subsizes[sendRank] * sizeof(long int));
		MPI_Sendrecv(partBuff, *sizeSend, MPI_LONG, sendRank, 0, partitions[sendRank], subsizes[sendRank], MPI_LONG, sendRank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		free(partBuff);
		free(sizeRec);
		free(sizeSend);

			
		
	}


	t3 = MPI_Wtime();
	//Phase 4, Merge, receive, merge

	long int running_size = subsizes[0];
	long int* final = malloc(running_size*sizeof(long int));
	memcpy(final, partitions[0], running_size*sizeof(long int));
	free(partitions[0]);

	for(int i=1; i<procs; i++){
		if(subsizes[i] > 0){
			final = MergeSubs(final, partitions[i], running_size, subsizes[i]);
			running_size += subsizes[i];
			free(partitions[i]);
		}
	}
	//receive partitions and merge
	long int* sorted = malloc(numKeys * sizeof(long int));
	memcpy(sorted, final, running_size * sizeof(long int));
	free(final);
	long int largest = subsizes[0];
	for(int recvRank = 1; recvRank<procs; recvRank++){
		
		long int *size = malloc(sizeof(long int));
		MPI_Recv(size, 1, MPI_LONG, recvRank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		long int* partBuff = malloc( *size * sizeof(long int));
		long int recSize = *size;
		if(recSize > largest){
			largest = recSize;
		}
		free(size);
		MPI_Recv( partBuff, recSize, MPI_LONG, recvRank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		memcpy(&sorted[running_size], partBuff, recSize * sizeof(long int));
		running_size += recSize;
		free(partBuff);
		}
	t4 = MPI_Wtime();
	printf("Phase times:\nP1: %.2lf\nP2: %.2lf\nP3: %.2lf\nP4: %.2lf\n", 100*(t1-t0)/(t4-t0), 100*(t2-t1)/(t4-t0), 100*(t3-t2)/(t4-t0), 100*(t4-t3)/(t4-t0));
	double RDFA = (largest * procs) / numKeys;
	printf("RDFA: %.3lf\n", RDFA);
	//for (int i=0; i<numKeys-1; i++){
	//	if(sorted[i] > sorted[i+1]){
	//		printf("WRONG");
	//	}
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
	srandom(rank*100);
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
		}

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
		long int count = 0; 

		while((array[index] <= pivots[piv]) && (index < localKeys)){
			index++;
			count++;
	
		}
		if(count > 0){
			partitions[piv] = (long int*)malloc(count * sizeof(long int));
			memcpy(partitions[piv], &array[initial], count*sizeof(long int));
			
		}
		subsizes[piv] = count;
		initial = index;

	}
	
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
	//
	for(int sendRank = 0; sendRank< procs; sendRank++){
		if(sendRank != rank){
			long int *partBuff = malloc(subsizes[sendRank]*sizeof(long int));
			long int *sizeRec = malloc(sizeof(long int));
			long int *sizeSend = malloc(sizeof(long int));
			*sizeSend = subsizes[sendRank];
			memcpy(partBuff, partitions[sendRank], subsizes[sendRank]*sizeof(long int));
			free(partitions[sendRank]);
			//send and recv sizes
			MPI_Sendrecv(sizeSend, 1, MPI_LONG, sendRank, 0, sizeRec, 1, MPI_LONG, sendRank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			subsizes[sendRank] = *sizeRec;
			partitions[sendRank] = malloc(subsizes[sendRank] * sizeof(long int));
			MPI_Sendrecv(partBuff, *sizeSend, MPI_LONG, sendRank, 0, partitions[sendRank], subsizes[sendRank], MPI_LONG, sendRank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
			free(partBuff);
			free(sizeRec);
			free(sizeSend);


			
		}
	}

	
	//now we have the correct partitions, merge them up
	
	long int running_size = subsizes[0];
	long int* final = malloc(running_size*sizeof(long int));
	memcpy(final, partitions[0], running_size*sizeof(long int));
	free(partitions[0]);
	for(int i=1; i<procs; i++){
		if(subsizes[i] > 0){
			final = MergeSubs(final, partitions[i], running_size, subsizes[i]);
			running_size += subsizes[i];
			free(partitions[i]);
		}
	}

	MPI_Send(&running_size, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD);
	MPI_Send(final, running_size, MPI_LONG, 0, 0, MPI_COMM_WORLD);
	
	free(final);
	free(array);
	return 0;
	}


	








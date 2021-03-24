#include <stdio.h>
#include <stdlib.h>
#include <limits.h>


long int *genKeys(int numKeys){
	//function will return an array of length numKeys of integer values between 0 and MAX_INT
	long int *keys = malloc(numKeys * sizeof(long int));
	for( int i = 0; i<numKeys; i++){
		keys[i] = random();
		
	}
	return keys;

}


int comparison(const void *a, const void *b){

	return ( *(long int*)a - *(long int*)b);
}


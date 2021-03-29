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



long int* MergeSubs(long int listA[], long int listB[], int len1, int len2){
	long int* listC = malloc((len1 + len2)*sizeof(long int));
	
	long int i=0;
	long int j=0;
	long int y=0;

	while( (i < len1) && (j<len2)){
		if (listA[i] < listB[j]){
			listC[y] = listA[i];
			y++;
			i++;
		}
		else{
			listC[y] = listB[j];
			y++;
			j++;
			}
	}
	//get leftovers, also handles for one of the lengths being 0
	while(i<len1){
		listC[y] = listA[i];
		y++;
		i++;
	}
	while(j < len2){
		listC[y] = listB[j];
		y++;
		j++;
	}
	return listC;
}



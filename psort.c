
/*
# Filename: psort.c
# Student name and No.: Sankalok Sen 
# Development platform: WSL & Workbench2
# Remark: Implemented concurrently without using mutex locks, programming is working correctly
# but is slightly slower than the benchmark when number of workers increase >9 for 100,000,000 cases
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/time.h>
#include <pthread.h>

int checking(unsigned int *, long);
int compare(const void *, const void *);
void *sort(void *);

// global variables
long size;  // size of the array
long num_of_workers; // number of threads
unsigned int * intarr; // array of random integers
unsigned int * threads_idx_1; // starting idx of threads in Phase 1
unsigned int ** intarr2d; // Phase 1 2d array
unsigned int * threads_idx_1_1; // starting idx of each threads in part 2 of Phase 1
unsigned int * sampling_1; // sampling for Phase 1 and stores sorted for Phase 2
unsigned int * pivot_2; //stores pivot indexes for Phase 2
unsigned int * pivot_values_2; //stores pivot values for Phase 2 and 3
unsigned int ** size_4_pos; //stores disjoint indexes for phase 4
unsigned int * size_4; //stores size of disjoing indexes for phase 4
unsigned int ** intarr2d_4; // Phase 4 2d array

pthread_mutex_t mutex;

struct args {
    unsigned int * arr;
    long start;
    long stop;
};

int main (int argc, char **argv)
{
	long i, j, k, l, m;
	struct timeval start, end;

    //arg cases
	if ((argc < 2))
	{
		printf("Usage: ./psort <number> <num_of_workers> [DEFAULT = 4]\n");
		exit(0);
	}

    if((argc == 2))
    {
        size = atol(argv[1]);
        num_of_workers = 4;
    }

    if((argc == 3))
    {
        size = atol(argv[1]);
        num_of_workers = atol(argv[2]);
    }

	intarr = (unsigned int *)malloc(size*sizeof(unsigned int));
	if (intarr == NULL) {perror("malloc"); exit(0); }
	
	// set the random seed for generating a fixed random
	// sequence across different runs
  	char * env = getenv("RANNUM");  //get the env variable
	if (!env)                       //if not exists
			srandom(3230);
		else
			srandom(atol(env));
		
		for (i=0; i<size; i++) {
			intarr[i] = random();
		}
	
    threads_idx_1 = (unsigned int *)malloc((num_of_workers+1)*sizeof(unsigned int));

    intarr2d = (unsigned int **)malloc(num_of_workers*sizeof(unsigned int*));
    for (i = 0; i < num_of_workers; i++) {
        intarr2d[i] = (unsigned int *)malloc((long)(size/num_of_workers)*sizeof(unsigned int));
    }

	// measure the start time
	gettimeofday(&start, NULL);
	
	// just call the qsort library
	// replace qsort by your parallel sorting algorithm using pthread

	//qsort(intarr, size, sizeof(unsigned int), compare);

    //initialize worker thread sizes
    for(i = 0; i <= num_of_workers; i++) {
        if(i != num_of_workers)
            threads_idx_1[i] = (long)((i*size)/num_of_workers);
        else
            threads_idx_1[i] = size;
    }

    //splitting intarr into 2d array with each array concurrently sorted by a thread
    for(i = 0; i < num_of_workers; i++) {
        k = 0;
        long start = threads_idx_1[i];
        long stop = threads_idx_1[i+1];
        for(j = start; j < stop; j++) {
            intarr2d[i][k] = intarr[j];
            k = k + 1;
        }
    }

    //worker threads for phase 1 sorting
    pthread_t workers_thread[num_of_workers];

    for(i = 0; i < num_of_workers; i++) {
        struct args *a = (struct args *)malloc(sizeof(struct args));
        a->arr = intarr2d[i];
        a->start = threads_idx_1[i];
        a->stop = threads_idx_1[i+1];
        pthread_create(workers_thread+i, NULL, &sort, (void *)a);
    }

    for(i = 0; i < num_of_workers; i++) {
        pthread_join(workers_thread[i], NULL);
    }

    threads_idx_1_1 = (unsigned int *)malloc(num_of_workers*sizeof(unsigned int));

    // phase 1 indexing
    for(i = 0; i < num_of_workers; i++) {
        threads_idx_1_1[i] = (long)((i*size)/(num_of_workers*num_of_workers));
    }

    sampling_1 = (unsigned int *)malloc((num_of_workers*num_of_workers)*sizeof(unsigned int));

    // phase 1 sampling
    long s_1_size = 0;
    for(i = 0; i < num_of_workers; i++) {
        for(j = 0; j < num_of_workers; j++) {
            sampling_1[s_1_size] = intarr2d[i][threads_idx_1_1[j]];
            s_1_size = s_1_size + 1;
        }
    }

    //phase 1-2 sorting of samples
    qsort(sampling_1, s_1_size, sizeof(unsigned int), compare);

    pivot_2 = (unsigned int *)malloc((num_of_workers-1)*sizeof(unsigned int));

    //phase 2 pivots
    for(i = 1; i <= num_of_workers-1; i++) {
        pivot_2[i-1] = (long)((i*num_of_workers) + (num_of_workers/2) - 1); 
    }

    pivot_values_2 = (unsigned int *)malloc((num_of_workers-1)*sizeof(unsigned int));

    //phase 2-3 pivot values
    for(i = 0; i < num_of_workers-1; i++) {
        pivot_values_2[i] = sampling_1[pivot_2[i]];
    }

    //phase 4 indexes and positions
    size_4_pos = (unsigned int **)malloc(num_of_workers*sizeof(unsigned int*));
    for (i = 0; i < num_of_workers; i++) {
        size_4_pos[i] = (unsigned int *)malloc((num_of_workers+1)*sizeof(unsigned int));
    }

    for(i = 0; i < num_of_workers; i++) {
        long start = threads_idx_1[i];
        long stop = threads_idx_1[i+1];
        size_4_pos[i][0] = 0;
        m = 1;
        for(l = 0; l < num_of_workers-1; l++) {
            k = 0;
            for(j = start; j < stop; j++) {
                if(intarr2d[i][k] > pivot_values_2[l]) {
                    size_4_pos[i][m++] = k-1;
                    break;
                }
                else {
                    k = k + 1;
                }
            }
        }
        size_4_pos[i][num_of_workers] = stop-start-1;
    }

    size_4 = (unsigned int *)malloc(num_of_workers*sizeof(unsigned int));

    //phase 4 sizes of each thread computation
    for(i = 0; i < num_of_workers; i++) {
        size_4[i] = 0;
    }

    for(i = 0; i < num_of_workers; i++) {
        for(j = 0; j < num_of_workers; j++) {
            if(j == 0) {
                size_4[j] += (size_4_pos[i][j+1] - size_4_pos[i][j] + 1);
            }
            else {
                size_4[j] += (size_4_pos[i][j+1] - size_4_pos[i][j]);
            }
        }
    }

    //phase 4 extracting threads from the 2d array
    intarr2d_4 = (unsigned int **)malloc(num_of_workers*sizeof(unsigned int*));
    for (i = 0; i < num_of_workers; i++) {
        intarr2d_4[i] = (unsigned int *)malloc((long)(2*size/num_of_workers)*sizeof(unsigned int));
    }

    l = 0;
    for(i = 0; i < num_of_workers; i++) {
        m = 0;
        for(j = 0; j < num_of_workers; j++) {
            long start, stop;
            if(i == 0) {
                start = size_4_pos[j][i];
            }
            else {
                start = size_4_pos[j][i] + 1;
            }
            stop = size_4_pos[j][i+1];
            for(k = start; k <= stop; k++) {
                intarr2d_4[l][m] = intarr2d[j][k];
                m = m + 1;
            }
        }
        l = l + 1;
    }

    //phase 4 sorting 
    for(i = 0; i < num_of_workers; i++) {
        struct args *a = (struct args *)malloc(sizeof(struct args));
        a->arr = intarr2d_4[i];
        a->start = 0;
        a->stop = (2*size/num_of_workers);
        pthread_create(workers_thread+i, NULL, &sort, (void *)a);
    }

    for(i = 0; i < num_of_workers; i++) {
        pthread_join(workers_thread[i], NULL);
    }

    //phase 5 concatenation
    m = 0;
    for(i = 0; i < num_of_workers; i++) {
        for(j = 0; j < (2*size/num_of_workers); j++) {
            if(intarr2d_4[i][j] != 0) {
                intarr[m] = intarr2d_4[i][j];
                m = m + 1;
            }
        }
    }

	// measure the end time
	gettimeofday(&end, NULL);
	
	if (!checking(intarr, size)) {
		printf("The array is not in sorted order!!\n");
	}
	
	printf("Total elapsed time: %.4f s\n", (end.tv_sec - start.tv_sec)*1.0 + (end.tv_usec - start.tv_usec)/1000000.0);
	  
	free(intarr);
    free(threads_idx_1);
    for(i = 0; i < num_of_workers; i++) {
        free(intarr2d[i]);
    }
    free(intarr2d);
    free(threads_idx_1_1);
    free(sampling_1);
    free(pivot_2);
    free(pivot_values_2);
    for(i = 0; i < num_of_workers; i++) {
        free(size_4_pos[i]);
    }
    free(size_4_pos);
    free(size_4);
    for(i = 0; i < num_of_workers; i++) {
        free(intarr2d_4[i]);
    }
    free(intarr2d_4);

	return 0;
}

void *sort(void * a) {
    qsort(((struct args*)a)->arr, (((struct args*)a)->stop - ((struct args*)a)->start), sizeof(unsigned int), compare);
}

int compare(const void * a, const void * b) {
	return (*(unsigned int *)a>*(unsigned int *)b) ? 1 : ((*(unsigned int *)a==*(unsigned int *)b) ? 0 : -1);
}

int checking(unsigned int * list, long size) {
	long i;
	printf("First : %d\n", list[0]);
	printf("At 25%%: %d\n", list[size/4]);
	printf("At 50%%: %d\n", list[size/2]);
	printf("At 75%%: %d\n", list[3*size/4]);
	printf("Last  : %d\n", list[size-1]);
	for (i=0; i<size-1; i++) {
		if (list[i] > list[i+1]) {
		  return 0;
		}
	}
	return 1;
}
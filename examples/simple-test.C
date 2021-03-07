#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <mpi.h>

int main(int argc, char **argv)
{
    int rank, size;
   
    MPI_Init(&argc,&argv);
    
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    char data[2];
    size_t len = 1;
    if (rank == 0) {
        FILE *fp;
        fp = fopen("./rank0.txt", "w+");
        fread(data, 1, 1, fp);
        MPI_Send(&data, 1, MPI_CHAR, 1, 0, MPI_COMM_WORLD);
        printf("rank %d sent data = %c\n", rank, data);
        fwrite(&data, 1, 1, fp);
    } else {
        FILE *fp;
        fp = fopen("./rank1.txt", "w+");
        fread(data, 1, 1, fp);
        MPI_Recv(&data, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("rank %d sent data = %c\n", rank, data);
        fwrite(&data, 1, 1, fp);
    }
    MPI_Finalize();
}
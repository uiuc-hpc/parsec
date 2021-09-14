/*
 * Copyright (c) 2017-2020 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#include "parsec/data_dist/matrix/matrix.h"
#include "parsec/data_dist/matrix/two_dim_rectangle_cyclic.h"
#include "parsec/data_dist/matrix/two_dim_rectangle_cyclic_band.h"
#include "parsec/data_dist/matrix/sym_two_dim_rectangle_cyclic_band.h"
#include "two_dim_band_test.h"
#include <string.h>

#if defined(PARSEC_HAVE_MPI)
#include <mpi.h>
#elif defined(PARSEC_HAVE_LCI)
#include <lc.h>
#endif

int main(int argc, char *argv[])
{
    parsec_context_t* parsec;
    int rank, nodes, ch;
    int pargc = 0, i, dashdash = -1;
    char **pargv;
    enum matrix_uplo uplo = matrix_Upper; //matrix_Lower
    enum matrix_uplo full = matrix_UpperLower;
    /* Super */
    int N = 16, NB = 4, P = 1, KP = 1, KQ = 1;
    /* Band */
    int P_BAND = 1, KP_BAND = 1, KQ_BAND = 1, BAND_SIZE = 1;

#if defined(PARSEC_HAVE_MPI)
    {
        int provided;
        MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &nodes);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
#elif defined(PARSEC_HAVE_LCI)
    lc_ep ep;
    lc_init(1, &ep);
    lci_global_ep = &ep;
    lc_get_num_proc(&nodes);
    lc_get_proc_num(&rank);
#else
    nodes = 1;
    rank = 0;
#endif

    for(i = 1; i < argc; i++) {
        if( strcmp(argv[i], "--") == 0 ) {
            dashdash = i;
            pargc = 0;
        } else if( dashdash != -1 ) {
            pargc++;
        }
    }
    pargv = malloc( (pargc+1) * sizeof(char*));
    if( dashdash != -1 ) {
        for(i = dashdash+1; i < argc; i++) {
            pargv[i-dashdash-1] = strdup(argv[i]);
        }
        pargv[i-dashdash-1] = NULL;
    } else {
        pargv[0] = NULL;
    }

    /* Initialize PaRSEC */
    parsec = parsec_init(-1, &pargc, &pargv);

    while ((ch = getopt(argc, argv, "N:T:s:S:P:p:f:F:b:h")) != -1) {
        switch (ch) {
            case 'N': N = atoi(optarg); break;
            case 'T': NB = atoi(optarg); break;
            case 's': KP = atoi(optarg); break;
            case 'S': KQ = atoi(optarg); break;
            case 'P': P = atoi(optarg); break;
            case 'p': P_BAND = atoi(optarg); break;
            case 'f': KP_BAND = atoi(optarg); break;
            case 'F': KQ_BAND = atoi(optarg); break;
            case 'b': BAND_SIZE = atoi(optarg); break;
            case '?': case 'h': default:
                fprintf(stderr,
                        "SUPER:\n"
                        "-N : dimension (N) of the matrices (default: 16)\n"
                        "-T : dimension (NB) of the tiles (default: 4)\n"
                        "-s : rows of tiles in a k-cyclic distribution (default: 1)\n"
                        "-S : columns of tiles in a k-cyclic distribution (default: 1)\n"
                        "-P : rows (P) in the PxQ process grid (default: 1)\n"
                        "BAND:\n"
                        "-p : rows (p) in the pxq process grid (default: 1)\n"
                        "-f : rows of tiles in a k-cyclic distribution (default: 1)\n"
                        "-F : columns of tiles in a k-cyclic distribution (default: 1)\n"
                        "-b : band size (default: 1)\n"
                        "\n");
            exit(1);
        }
    }

    /* dcY initializing matrix structure */

    /* Init Off_band */
    two_dim_block_cyclic_band_t dcY;
    two_dim_block_cyclic_init(&dcY.off_band, matrix_RealDouble, matrix_Tile,
                                nodes, rank, NB, NB, N, N, 0, 0,
                                N, N, KP, KQ, P);
    parsec_data_collection_set_key((parsec_data_collection_t*)&dcY, "dcY off_band");

    /* Init band */
    two_dim_block_cyclic_init(&dcY.band, matrix_RealDouble, matrix_Tile,
                                nodes, rank, NB, NB, NB*(2*BAND_SIZE-1), N, 0, 0,
                                NB*(2*BAND_SIZE-1), N, KP_BAND, KQ_BAND, P_BAND);
    parsec_data_collection_set_key(&dcY.band.super.super, "dcY band");

    /* Init two_dim_block_cyclic_band_t structure */
    two_dim_block_cyclic_band_init( &dcY, nodes, rank, BAND_SIZE );

    /* YP */
    sym_two_dim_block_cyclic_band_t dcYP;

    /* Init Off_band */
    sym_two_dim_block_cyclic_init(&dcYP.off_band, matrix_RealDouble,
                                nodes, rank, NB, NB, N, N, 0, 0,
                                N, N, P, uplo);
    parsec_data_collection_set_key((parsec_data_collection_t*)&dcYP, "dcYP off_band");

    /* Init band */
    two_dim_block_cyclic_init(&dcYP.band, matrix_RealDouble, matrix_Tile,
                                nodes, rank, NB, NB, NB*BAND_SIZE, N, 0, 0,
                                NB*BAND_SIZE, N, KP_BAND, KQ_BAND, P_BAND);
    parsec_data_collection_set_key(&dcYP.band.super.super, "dcYP band");

    /* Init two_dim_block_cyclic_band_t structure */
    sym_two_dim_block_cyclic_band_init( &dcYP, nodes, rank, BAND_SIZE );

    /* Allocate memory and set value */
    parsec_two_dim_band_test(parsec, (parsec_tiled_matrix_dc_t *)&dcY, full);

    if( 0 == rank )
        printf("Y  Init \tSUPER: PxQ= %3d %-3d, KPxKQ=%3d %-3d, N= %7d, NB= %4d; BAND: PxQ= %3d %-3d KPxKQ=%3d %-3d, BAND_SIZE=%3d, M= %7d, N= %4d\n",
               P, nodes/P, KP, KQ, N, NB,
               P_BAND, nodes/P_BAND, KP_BAND, KQ_BAND, BAND_SIZE, NB*(2*BAND_SIZE-1), N);


    /* Allocate memory and set value */
    parsec_two_dim_band_test(parsec, (parsec_tiled_matrix_dc_t *)&dcYP, uplo);

    if( 0 == rank )
        printf("YP Init \tSUPER: PxQ= %3d %-3d, KPxKQ=%3d %-3d, N= %7d, NB= %4d; BAND: PxQ= %3d %-3d KPxKQ=%3d %-3d, BAND_SIZE=%3d, M= %7d, N= %4d\n",
               P, nodes/P, KP, KQ, N, NB,
               P_BAND, nodes/P_BAND, KP_BAND, KQ_BAND, BAND_SIZE, NB*BAND_SIZE, N);

    /* Free memory */
    parsec_two_dim_band_free(parsec, (parsec_tiled_matrix_dc_t *)&dcY, full);
    parsec_two_dim_band_free(parsec, (parsec_tiled_matrix_dc_t *)&dcYP, uplo);
    parsec_tiled_matrix_dc_destroy((parsec_tiled_matrix_dc_t*)&dcY);
    parsec_tiled_matrix_dc_destroy((parsec_tiled_matrix_dc_t*)&dcYP);

    /* Clean up parsec*/
    parsec_fini(&parsec);

#ifdef PARSEC_HAVE_MPI
    MPI_Finalize();
#elif defined(PARSEC_HAVE_LCI)
    lc_finalize();
#endif

    return 0;
}

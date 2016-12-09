/*
 * Copyright (c) 2009-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 *
 * @precisions normal z -> s d c
 *
 */

#include "common.h"
#include "flops.h"
#include "data_dist/matrix/sym_two_dim_rectangle_cyclic.h"
#include "data_dist/matrix/two_dim_rectangle_cyclic.h"

#if defined(CHECK_B)
static int check_solution( parsec_context_t *parsec, int loud, PLASMA_enum uplo,
                           tiled_matrix_desc_t *ddescA,
                           tiled_matrix_desc_t *ddescB,
                           tiled_matrix_desc_t *ddescX );
#endif

static int check_inverse( parsec_context_t *parsec, int loud, PLASMA_enum uplo,
                          tiled_matrix_desc_t *ddescA,
                          tiled_matrix_desc_t *ddescInvA,
                          tiled_matrix_desc_t *ddescI );

int main(int argc, char ** argv)
{
    parsec_context_t* parsec;
    int iparam[IPARAM_SIZEOF];
    int ret = 0;
    int uplo = PlasmaLower;
    PLASMA_Complex64_t *U_but_vec;
    DagDouble_t time_butterfly, time_facto, time_total;

    /* Set defaults for non argv iparams */
    iparam_default_facto(iparam);
    iparam_default_ibnbmb(iparam, 0, 180, 180);

    /* Initialize PaRSEC */
    parsec = setup_parsec(argc, argv, iparam);
    PASTE_CODE_IPARAM_LOCALS(iparam);
    PASTE_CODE_FLOPS(FLOPS_ZHETRF, ((DagDouble_t)N));

    /* initializing matrix structure */
    LDA = max( LDA, N );
    LDB = max( LDB, N );
    SMB = 1;
    SNB = 1;

    PASTE_CODE_ALLOCATE_MATRIX(ddescA, 1,
        sym_two_dim_block_cyclic, (&ddescA, matrix_ComplexDouble,
                                   nodes, rank, MB, NB, LDA, N, 0, 0,
                                   N, N, P, uplo));

    PASTE_CODE_ALLOCATE_MATRIX(ddescA0, check,
        sym_two_dim_block_cyclic, (&ddescA0, matrix_ComplexDouble,
                                   nodes, rank, MB, NB, LDA, N, 0, 0,
                                   N, N, P, uplo));

#if defined(CHECK_B)
    PASTE_CODE_ALLOCATE_MATRIX(ddescB, check,
        two_dim_block_cyclic, (&ddescB, matrix_ComplexDouble, matrix_Tile,
                                   nodes, rank, MB, NB, LDB, NRHS, 0, 0,
                                   N, NRHS, SMB, SNB, P));

    PASTE_CODE_ALLOCATE_MATRIX(ddescX, check,
        two_dim_block_cyclic, (&ddescX, matrix_ComplexDouble, matrix_Tile,
                                   nodes, rank, MB, NB, LDB, NRHS, 0, 0,
                                   N, NRHS, SMB, SNB, P));
#endif

    PASTE_CODE_ALLOCATE_MATRIX(ddescInvA, check_inv,
        two_dim_block_cyclic, (&ddescInvA, matrix_ComplexDouble, matrix_Tile,
                               nodes, rank, MB, NB, LDA, N, 0, 0,
                               N, N, SMB, SNB, P));

    PASTE_CODE_ALLOCATE_MATRIX(ddescI, check_inv,
        two_dim_block_cyclic, (&ddescI, matrix_ComplexDouble, matrix_Tile,
                               nodes, rank, MB, NB, LDA, N, 0, 0,
                               N, N, SMB, SNB, P));

    /* matrix generation */
    if(loud > 2) printf("+++ Generate matrices ... ");
    dplasma_zplghe( parsec, (double)(0), uplo,
                    (tiled_matrix_desc_t *)&ddescA, 1358);

    if( check ){
        dplasma_zlacpy( parsec, uplo,
                        (tiled_matrix_desc_t *)&ddescA,
                        (tiled_matrix_desc_t *)&ddescA0);

#if defined(CHECK_B)
        dplasma_zplrnt( parsec, 0,
                        (tiled_matrix_desc_t *)&ddescB, 3872);

        dplasma_zlacpy( parsec, PlasmaUpperLower,
                        (tiled_matrix_desc_t *)&ddescB,
                        (tiled_matrix_desc_t *)&ddescX);
#endif
        if (check_inv) {
            dplasma_zlaset( parsec, PlasmaUpperLower, 0., 1., (tiled_matrix_desc_t *)&ddescInvA);
            dplasma_zlaset( parsec, PlasmaUpperLower, 0., 1., (tiled_matrix_desc_t *)&ddescI);
        }
    }
    if(loud > 2) printf("Done\n");


    if(loud > 2) printf("+++ Computing Butterfly ... \n");
    SYNC_TIME_START();
    if (loud) TIME_START();

    ret = dplasma_zhebut(parsec, (tiled_matrix_desc_t *)&ddescA, &U_but_vec, butterfly_level);
    if( ret < 0 )
        return ret;

    SYNC_TIME_PRINT(rank, ("zhebut computation N= %d NB= %d\n", N, NB));

    /* Backup butterfly time */
    time_butterfly = sync_time_elapsed;
    if(loud > 2) printf("... Done\n");


    if(loud > 2) printf("+++ Computing Factorization ... \n");
    SYNC_TIME_START();
    dplasma_zhetrf(parsec, (tiled_matrix_desc_t *)&ddescA);

    SYNC_TIME_PRINT(rank, ("zhetrf computation N= %d NB= %d : %f gflops\n", N, NB,
                           (gflops = (flops/1e9)/(sync_time_elapsed))));
    if(loud > 2) printf(" ... Done.\n");

    time_facto = sync_time_elapsed;
    time_total = time_butterfly + time_facto;

    if(0 == rank) {
        printf( "zhebut+zhetrf computation : %f gflops (%02.2f%% / %02.2f%%)\n",
                (gflops = (flops/1e9)/(time_total)),
                time_butterfly / time_total * 100.,
                time_facto     / time_total * 100.);
    }
    (void)gflops;

    if(check) {

#if defined(CHECK_B)
        dplasma_zhetrs(parsec, uplo,
                       (tiled_matrix_desc_t *)&ddescA,
                       (tiled_matrix_desc_t *)&ddescX,
                       U_but_vec, butterfly_level);

        /* Check the solution */
        ret |= check_solution( parsec, (rank == 0) ? loud : 0, uplo,
                               (tiled_matrix_desc_t *)&ddescA0,
                               (tiled_matrix_desc_t *)&ddescB,
                               (tiled_matrix_desc_t *)&ddescX);

        parsec_data_free(ddescB.mat);
        tiled_matrix_desc_destroy( (tiled_matrix_desc_t*)&ddescB);

        parsec_data_free(ddescX.mat);
        tiled_matrix_desc_destroy( (tiled_matrix_desc_t*)&ddescX);
#endif

        if (check_inv) {

            fprintf(stderr, "Start hetrs\n");
            dplasma_zhetrs(parsec, uplo,
                           (tiled_matrix_desc_t *)&ddescA,
                           (tiled_matrix_desc_t *)&ddescInvA,
                           U_but_vec, butterfly_level);

            fprintf(stderr, "Start check_inv\n");
            /* Check the solution against the inverse */
            ret |= check_inverse(parsec, (rank == 0) ? loud : 0, uplo,
                                 (tiled_matrix_desc_t *)&ddescA0,
                                 (tiled_matrix_desc_t *)&ddescInvA,
                                 (tiled_matrix_desc_t *)&ddescI);

            parsec_data_free(ddescInvA.mat);
            tiled_matrix_desc_destroy( (tiled_matrix_desc_t*)&ddescInvA);

            parsec_data_free(ddescI.mat);
            tiled_matrix_desc_destroy( (tiled_matrix_desc_t*)&ddescI);
        }
    }

    parsec_data_free(ddescA.mat);
    tiled_matrix_desc_destroy( (tiled_matrix_desc_t*)&ddescA);

    cleanup_parsec(parsec, iparam);

    return ret;
}

#if defined(CHECK_B)
/*
 * This function destroys B
 */
static int check_solution( parsec_context_t *parsec, int loud, PLASMA_enum uplo,
                           tiled_matrix_desc_t *ddescA,
                           tiled_matrix_desc_t *ddescB,
                           tiled_matrix_desc_t *ddescX )
{
    int info_solution;
    double Rnorm = 0.0;
    double Anorm = 0.0;
    double Bnorm = 0.0;
    double Xnorm, result;
    int N = ddescB->m;
    double eps = LAPACKE_dlamch_work('e');

    Anorm = dplasma_zlanhe(parsec, PlasmaInfNorm, uplo, ddescA);
    Bnorm = dplasma_zlange(parsec, PlasmaInfNorm, ddescB);
    Xnorm = dplasma_zlange(parsec, PlasmaInfNorm, ddescX);

    /* Compute A*x */
    dplasma_zhemm( parsec, PlasmaLeft, uplo, -1.0, ddescA, ddescX, 1.0, ddescB);

    Rnorm = dplasma_zlange(parsec, PlasmaInfNorm, ddescB);

    result = Rnorm / ( ( Anorm * Xnorm + Bnorm ) * N * eps ) ;

    if ( loud > 2 ) {
        printf("============\n");
        printf("Checking the Residual of the solution \n");
        if ( loud > 3 )
            printf( "-- ||A||_oo = %e, ||X||_oo = %e, ||B||_oo= %e, ||A X - B||_oo = %e\n",
                    Anorm, Xnorm, Bnorm, Rnorm );

        printf("-- ||Ax-B||_oo/((||A||_oo||x||_oo+||B||_oo).N.eps) = %e \n", result);
    }

    if (  isnan(Xnorm) || isinf(Xnorm) || isnan(result) || isinf(result) || (result > 60.0) ) {
        if( loud ) printf("-- Solution is suspicious ! \n");
        info_solution = 1;
    }
    else{
        if( loud ) printf("-- Solution is CORRECT ! \n");
        info_solution = 0;
    }

    return info_solution;
}
#endif

static int check_inverse( parsec_context_t *parsec, int loud, PLASMA_enum uplo,
                          tiled_matrix_desc_t *ddescA,
                          tiled_matrix_desc_t *ddescInvA,
                          tiled_matrix_desc_t *ddescI )
{
    int info_solution;
    double Anorm    = 0.0;
    double InvAnorm = 0.0;
    double Rnorm, result;
    int m = ddescA->m;
    double eps = LAPACKE_dlamch_work('e');

    Anorm    = dplasma_zlanhe(parsec, PlasmaInfNorm, uplo, ddescA);
    InvAnorm = dplasma_zlange(parsec, PlasmaInfNorm, ddescInvA);

    /* Compute I - A*A^{-1} */
    dplasma_zhemm( parsec, PlasmaLeft, uplo, -1.0, ddescA, ddescInvA, 1.0, ddescI);

    Rnorm = dplasma_zlange(parsec, PlasmaInfNorm, ddescI);

    result = Rnorm / ( ( Anorm * InvAnorm ) * m * eps ) ;

    if ( loud > 2 ) {
        printf("============\n");
        printf("Checking the Residual of the solution \n");
        if ( loud > 3 )
            printf( "-- ||A||_oo = %e, ||A^{-1}||_oo = %e, ||A A^{-1} - I||_oo = %e\n",
                    Anorm, InvAnorm, Rnorm );

        printf("-- ||AA^{-1}-I||_oo/((||A||_oo||A^{-1}||_oo).N.eps) = %e \n", result);
    }

    if (  isnan(Rnorm) || isinf(Rnorm) || isnan(result) || isinf(result) || (result > 60.0) ) {
        if( loud ) printf("-- Solution is suspicious ! \n");
        info_solution = 1;
    }
    else{
        if( loud ) printf("-- Solution is CORRECT ! \n");
        info_solution = 0;
    }

    return info_solution;
}

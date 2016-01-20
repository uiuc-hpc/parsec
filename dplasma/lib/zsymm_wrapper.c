/*
 * Copyright (c) 2010-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2013      Inria. All rights reserved.
 * $COPYRIGHT
 *
 * @precisions normal z -> c d s
 *
 */

#include "dplasma.h"
#include "dplasma/lib/dplasmatypes.h"

#include "zsymm.h"

/**
 ******************************************************************************
 *
 * @ingroup dplasma_complex64
 *
 *  dplasma_zsymm_New - Generates the dague handle to compute the following
 *  operation.  WARNING: The computations are not done by this call.
 *
 *     \f[ C = \alpha \times A \times B + \beta \times C \f]
 *
 *  or
 *
 *     \f[ C = \alpha \times B \times A + \beta \times C \f]
 *
 *  where alpha and beta are scalars, A is an symmetric matrix and  B and
 *  C are m by n matrices.
 *
 *******************************************************************************
 *
 * @param[in] side
 *          Specifies whether the symmetric matrix A appears on the
 *          left or right in the operation as follows:
 *          = PlasmaLeft:      \f[ C = \alpha \times A \times B + \beta \times C \f]
 *          = PlasmaRight:     \f[ C = \alpha \times B \times A + \beta \times C \f]
 *
 * @param[in] uplo
 *          Specifies whether the upper or lower triangular part of
 *          the symmetric matrix A is to be referenced as follows:
 *          = PlasmaLower:     Only the lower triangular part of the
 *                             symmetric matrix A is to be referenced.
 *          = PlasmaUpper:     Only the upper triangular part of the
 *                             symmetric matrix A is to be referenced.
 *
 * @param[in] alpha
 *          Specifies the scalar alpha.
 *
 * @param[in] A
 *          Descriptor of the symmetric matrix A.  A is a ka-by-ka
 *          matrix, where ka is C->M when side = PlasmaLeft, and is
 *          C->N otherwise. Only the uplo triangular part is
 *          referenced.
 *
 * @param[in] B
 *          Descriptor of the M-by-N matrix B
 *
 * @param[in] beta
 *          Specifies the scalar beta.
 *
 * @param[in,out] C
 *          Descriptor of the M-by-N matrix C which is overwritten by
 *          the result of the operation.
 *
 *******************************************************************************
 *
 * @return
 *          \retval NULL if incorrect parameters are given.
 *          \retval The dague handle describing the operation that can be
 *          enqueued in the runtime with dague_enqueue(). It, then, needs to be
 *          destroy with dplasma_zsymm_Destruct();
 *
 *******************************************************************************
 *
 * @sa dplasma_zsymm
 * @sa dplasma_zsymm_Destruct
 * @sa dplasma_csymm_New
 * @sa dplasma_dsymm_New
 * @sa dplasma_ssymm_New
 *
 ******************************************************************************/
dague_handle_t*
dplasma_zsymm_New( PLASMA_enum side,
                   PLASMA_enum uplo,
                   dague_complex64_t alpha,
                   const tiled_matrix_desc_t* A,
                   const tiled_matrix_desc_t* B,
                   dague_complex64_t beta,
                   tiled_matrix_desc_t* C)
{
    dague_zsymm_handle_t* handle;

    handle = dague_zsymm_new(side, uplo, alpha, beta,
                             (dague_ddesc_t*)A,
                             (dague_ddesc_t*)B,
                             (dague_ddesc_t*)C);

    dplasma_add2arena_tile(handle->arenas[DAGUE_zsymm_DEFAULT_ARENA],
                           C->mb*C->nb*sizeof(dague_complex64_t),
                           DAGUE_ARENA_ALIGNMENT_SSE,
                           dague_datatype_double_complex_t, C->mb);

    return (dague_handle_t*)handle;
}

/**
 *******************************************************************************
 *
 * @ingroup dplasma_complex64
 *
 *  dplasma_zsymm_Destruct - Free the data structure associated to an handle
 *  created with dplasma_zsymm_New().
 *
 *******************************************************************************
 *
 * @param[in] o
 *          On entry, the handle to destroy
 *          On exit, the handle cannot be used anymore.
 *
 *******************************************************************************
 *
 * @sa dplasma_zsymm_New
 * @sa dplasma_zsymm
 *
 ******************************************************************************/
void
dplasma_zsymm_Destruct( dague_handle_t *handle )
{
    dague_zsymm_handle_t *zsymm_handle = (dague_zsymm_handle_t*)handle;
    dague_matrix_del2arena( zsymm_handle->arenas[DAGUE_zsymm_DEFAULT_ARENA] );
    handle->destructor(handle);
}

/**
 ******************************************************************************
 *
 * @ingroup dplasma_complex64
 *
 *  dplasma_zsymm - Computes the following operation.
 *
 *     \f[ C = \alpha \times A \times B + \beta \times C \f]
 *
 *  or
 *
 *     \f[ C = \alpha \times B \times A + \beta \times C \f]
 *
 *  where alpha and beta are scalars, A is an symmetric matrix and  B and
 *  C are m by n matrices.
 *
 *******************************************************************************
 *
 * @param[in,out] dague
 *          The dague context of the application that will run the operation.
 *
 * @param[in] side
 *          Specifies whether the symmetric matrix A appears on the
 *          left or right in the operation as follows:
 *          = PlasmaLeft:      \f[ C = \alpha \times A \times B + \beta \times C \f]
 *          = PlasmaRight:     \f[ C = \alpha \times B \times A + \beta \times C \f]
 *
 * @param[in] uplo
 *          Specifies whether the upper or lower triangular part of
 *          the symmetric matrix A is to be referenced as follows:
 *          = PlasmaLower:     Only the lower triangular part of the
 *                             symmetric matrix A is to be referenced.
 *          = PlasmaUpper:     Only the upper triangular part of the
 *                             symmetric matrix A is to be referenced.
 *
 * @param[in] alpha
 *          Specifies the scalar alpha.
 *
 * @param[in] A
 *          Descriptor of the symmetric matrix A.  A is a ka-by-ka
 *          matrix, where ka is C->M when side = PlasmaLeft, and is
 *          C->N otherwise. Only the uplo triangular part is
 *          referenced.
 *
 * @param[in] B
 *          Descriptor of the M-by-N matrix B
 *
 * @param[in] beta
 *          Specifies the scalar beta.
 *
 * @param[in,out] C
 *          Descriptor of the M-by-N matrix C which is overwritten by
 *          the result of the operation.
 *
 *******************************************************************************
 *
 * @return
 *          \retval -i if the ith parameters is incorrect.
 *          \retval 0 on success.
 *
 *******************************************************************************
 *
 * @sa dplasma_zsymm_New
 * @sa dplasma_zsymm_Destruct
 * @sa dplasma_csymm
 * @sa dplasma_dsymm
 * @sa dplasma_ssymm
 *
 ******************************************************************************/
int
dplasma_zsymm( dague_context_t *dague,
               PLASMA_enum side,
               PLASMA_enum uplo,
               dague_complex64_t alpha,
               const tiled_matrix_desc_t *A,
               const tiled_matrix_desc_t *B,
               dague_complex64_t beta,
               tiled_matrix_desc_t *C)
{
    dague_handle_t *dague_zsymm = NULL;

    /* Check input arguments */
    if ((side != PlasmaLeft) && (side != PlasmaRight)) {
        dplasma_error("dplasma_zsymm", "illegal value of side");
        return -1;
    }
    if ((uplo != PlasmaLower) && (uplo != PlasmaUpper)) {
        dplasma_error("dplasma_zsymm", "illegal value of uplo");
        return -2;
    }
    if ( (A->m != A->n) ) {
        dplasma_error("dplasma_zsymm", "illegal size of matrix A which should be square");
        return -4;
    }
    if ( (B->m != C->m) || (B->n != C->n) ) {
        dplasma_error("dplasma_zsymm", "illegal sizes of matrices B and C");
        return -5;
    }
    if ( ((side == PlasmaLeft) && (A->n != C->m)) ||
         ((side == PlasmaRight) && (A->n != C->n)) ) {
        dplasma_error("dplasma_zsymm", "illegal size of matrix A");
        return -6;
    }

    dague_zsymm = dplasma_zsymm_New(side, uplo,
                                    alpha, A, B,
                                    beta, C);

    if ( dague_zsymm != NULL )
    {
        dague_enqueue( dague, (dague_handle_t*)dague_zsymm);
        dplasma_progress(dague);
        dplasma_zsymm_Destruct( dague_zsymm );
    }
    return 0;
}

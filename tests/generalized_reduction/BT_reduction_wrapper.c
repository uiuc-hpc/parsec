/*
 * Copyright (c) 2009-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#include "parsec/runtime.h"
#include "parsec/data_distribution.h"
#include "parsec/datatype.h"
#include "parsec/arena.h"

#if defined(PARSEC_HAVE_MPI)
#include <mpi.h>
#elif defined(PARSEC_HAVE_LCI)
#include <lc.h>
#endif
#include <stdio.h>

#include "BT_reduction.h"
#include "BT_reduction_wrapper.h"

static parsec_datatype_t block;

/**
 * @param [IN] A    the data, already distributed and allocated
 * @param [IN] nb   tile size
 * @param [IN] nt   number of tiles
 *
 * @return the parsec object to schedule.
 */
parsec_taskpool_t *BT_reduction_new(parsec_tiled_matrix_dc_t *A, int nb, int nt)
{
    parsec_BT_reduction_taskpool_t *tp = NULL;

    tp = parsec_BT_reduction_new(A, nb, nt);

    ptrdiff_t lb, extent;
    parsec_type_create_contiguous(nb, parsec_datatype_int_t, &block);
    parsec_type_extent(block, &lb, &extent);

    parsec_arena_construct(tp->arenas[PARSEC_BT_reduction_DEFAULT_ARENA],
                           extent, PARSEC_ARENA_ALIGNMENT_SSE,
                           block);

    return (parsec_taskpool_t*)tp;
}

/**
 * @param [INOUT] o the parsec object to destroy
 */
void BT_reduction_destroy(parsec_taskpool_t *o)
{
    parsec_type_free(&block);
    PARSEC_INTERNAL_TASKPOOL_DESTRUCT(o);
}

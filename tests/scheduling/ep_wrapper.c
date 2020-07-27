/*
 * Copyright (c) 2014-2019 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#include "parsec/runtime.h"
#include <stdio.h>

#include "parsec/data_distribution.h"
#include "parsec/arena.h"

#include "ep.h"
#include "ep_wrapper.h"

/**
 * @param [IN] A     the data, already distributed and allocated
 * @param [IN] nt    number of tasks at a given level
 * @param [IN] level number of levels
 *
 * @return the parsec object to schedule.
 */
parsec_taskpool_t *ep_new(parsec_data_collection_t *A, int nt, int level)
{
    parsec_ep_taskpool_t *tp = NULL;

    if( nt <= 0 || level <= 0 ) {
        fprintf(stderr, "To work, EP must have at least one task to run per level\n");
        return (parsec_taskpool_t*)tp;
    }

    tp = parsec_ep_new(nt, level, A);

    ptrdiff_t lb, extent;
    parsec_type_extent(parsec_datatype_int8_t, &lb, &extent);
    /* The datatype is irrelevant as the example does not do communications between nodes */
    parsec_arena_datatype_construct( &tp->arenas_datatypes[PARSEC_ep_DEFAULT_ARENA],
                                     extent, PARSEC_ARENA_ALIGNMENT_SSE,
                                     parsec_datatype_int8_t );

    return (parsec_taskpool_t*)tp;
}

/**
 * @param [INOUT] o the parsec object to destroy
 */
void ep_destroy(parsec_taskpool_t *o)
{

    parsec_taskpool_free(o);
}

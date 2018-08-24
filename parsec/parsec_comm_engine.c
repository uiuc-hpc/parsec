/*
 * Copyright (c) 2009-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#include "parsec/parsec_mpi_funnelled.h"

/* This function will be called by the runtime */
parsec_comm_engine_t *
parsec_comm_engine_init(parsec_context_t *parsec_context)
{
    /* call the selected module init */
    return mpi_funnelled_init(parsec_context);
}

int
parsec_comm_engine_fini(parsec_comm_engine_t *comm_engine)
{
    /* call the selected module fini */
    return mpi_funnelled_fini(comm_engine);
}

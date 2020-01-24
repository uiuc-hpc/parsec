/*
 * Copyright (c) 2009-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 */

#include <assert.h>
#include "parsec/parsec_comm_engine.h"
#if defined(PARSEC_HAVE_MPI)
#include "parsec/parsec_mpi_funnelled.h"
#elif defined(PARSEC_HAVE_LCI)
#include "parsec/parsec_lci.h"
#endif

parsec_comm_engine_t parsec_ce;

/* This function will be called by the runtime */
parsec_comm_engine_t *
parsec_comm_engine_init(parsec_context_t *parsec_context)
{
    parsec_comm_engine_t *ce = NULL;
    /* call the selected module init */
#if defined(PARSEC_HAVE_MPI)
    ce = mpi_funnelled_init(parsec_context);
#elif defined(PARSEC_HAVE_LCI)
    ce = lci_init(parsec_context);
#endif

    assert(ce->capabilites.sided > 0 && ce->capabilites.sided < 3);
    return ce;
}

int
parsec_comm_engine_fini(parsec_comm_engine_t *comm_engine)
{
    int ret = 1;
    /* call the selected module fini */
#if defined(PARSEC_HAVE_MPI)
    ret = mpi_funnelled_fini(comm_engine);
#elif defined(PARSEC_HAVE_LCI)
    ret = lci_fini(comm_engine);
#endif
    return ret;
}

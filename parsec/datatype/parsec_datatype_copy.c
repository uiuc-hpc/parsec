/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011      NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2013      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "parsec_config.h"

#include <stddef.h>
#include <stdlib.h>

#include "parsec/prefetch.h"
#include "parsec/util/output.h"
#include "parsec/datatype/parsec_datatype.h"
#include "parsec/datatype/parsec_convertor.h"
#include "parsec/datatype/parsec_datatype_internal.h"
#include "parsec/datatype/parsec_datatype_checksum.h"


#if PARSEC_ENABLE_DEBUG
#define DO_DEBUG(INST)  if( parsec_ddt_copy_debug ) { INST }
#else
#define DO_DEBUG(INST)
#endif  /* PARSEC_ENABLE_DEBUG */

static size_t parsec_datatype_memop_block_size = 128 * 1024;

/**
 * Non overlapping memory regions
 */
#undef MEM_OP_NAME
#define MEM_OP_NAME  non_overlap
#undef MEM_OP
#define MEM_OP       MEMCPY
#include "parsec_datatype_copy.h"

#define MEMMOVE(d, s, l)                                  \
    do {                                                  \
        if( (((d) < (s)) && (((d) + (l)) > (s))) ||       \
            (((s) < (d)) && (((s) + (l)) > (d))) ) {      \
            memmove( (d), (s), (l) );                     \
        } else {                                          \
            MEMCPY( (d), (s), (l) );                      \
        }                                                 \
    } while (0)

/**
 * Overlapping memory regions
 */
#undef MEM_OP_NAME
#define MEM_OP_NAME  overlap
#undef MEM_OP
#define MEM_OP       MEMMOVE
#include "parsec_datatype_copy.h"

int32_t parsec_datatype_copy_content_same_ddt( const parsec_datatype_s* datatype, int32_t count,
                                             char* destination_base, char* source_base )
{
    ptrdiff_t extent;
    int32_t (*fct)( const parsec_datatype_s*, int32_t, char*, char*);

    DO_DEBUG( parsec_output( 0, "parsec_datatype_copy_content_same_ddt( %p, %d, dst %p, src %p )\n",
                           (void*)datatype, count, (void*)destination_base, (void*)source_base ); );

    /* empty data ? then do nothing. This should normally be trapped
     * at a higher level.
     */
    if( 0 == count ) return 1;

    /**
     * see discussion in coll_basic_reduce.c for the computation of extent when
     * count != 1. Short version of the story:
     * (true_extent + ((count - 1) * extent))
     */
    extent = (datatype->true_ub - datatype->true_lb) + (count - 1) * (datatype->ub - datatype->lb);

    fct = non_overlap_copy_content_same_ddt;
    if( destination_base < source_base ) {
        if( (destination_base + extent) > source_base ) {
            /* memmove */
            fct = overlap_copy_content_same_ddt;
        }
    } else {
        if( (source_base + extent) > destination_base ) {
            /* memmove */
            fct = overlap_copy_content_same_ddt;
        }
    }
    return fct( datatype, count, destination_base, source_base );
}


/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "parsec/parsec_config.h"
#include "parsec/datatype/parsec_datatype_config.h"
#include "parsec/constants.h"
#include "parsec/datatype/parsec_datatype.h"
#include "parsec/datatype/parsec_datatype_internal.h"

int32_t parsec_datatype_create_contiguous( int count, const parsec_datatype_s* oldType,
                                         parsec_datatype_s** newType )
{
    parsec_datatype_s* pdt;

    if( 0 == count ) {
        pdt = parsec_datatype_create( 0 );
        parsec_datatype_add( pdt, &parsec_datatype_empty, 0, 0, 0 );
    } else {
        pdt = parsec_datatype_create( oldType->desc.used + 2 );
        parsec_datatype_add( pdt, oldType, count, 0, (oldType->ub - oldType->lb) );
    }
    *newType = pdt;
    return PARSEC_SUCCESS;
}
